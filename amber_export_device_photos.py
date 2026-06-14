# amber_export_device_photos.py
# Отдельная выгрузка фотографий и CSV только по выбранным Янтарям / DeviceId.
#
# По умолчанию выгружает проблемные DeviceId 8 и 10.
#
# Запуск:
#   pip install pyodbc tqdm
#   python amber_export_device_photos.py --server "SERVER" --database "DB" --from-date "2024-01-01" --to-date "2024-02-01"
#
# Для DeviceId 8 и 10 за весь нужный период:
#   python amber_export_device_photos.py --device-ids "8,10" --out-dir "C:\\Users\\dtsygankov\\output_amber_8_10"
#
# Для проверки без физического копирования:
#   python amber_export_device_photos.py --device-ids "8,10" --dry-run
#
# Что создаёт:
#   OUT_DIR/photos/device_<DeviceId>/cam_<CameraAmberId>/frame_<FrameNo>/...
#   OUT_DIR/_data/photos.csv
#   OUT_DIR/_data/recognition_input.csv
#   OUT_DIR/_data/alarms.csv
#   OUT_DIR/_data/manual_containers.csv
#   OUT_DIR/_data/alarm_details.csv
#   OUT_DIR/_data/gamma_maxima.csv
#   OUT_DIR/_reports/copied.csv
#   OUT_DIR/_reports/already_exists.csv
#   OUT_DIR/_reports/missing.csv
#   OUT_DIR/_reports/errors.csv

from __future__ import annotations

import argparse
import csv
import glob
import re
import shutil
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pyodbc

try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **kwargs):
        return x


# =============================================================================
# НАСТРОЙКИ
# =============================================================================

DB_SERVER = r""
DB_NAME = r""
DB_DRIVER = "ODBC Driver 17 for SQL Server"

USE_INTEGRATED_AUTH = True
DB_USER = r""
DB_PASSWORD = r""

# Главное: тут задаются Янтари / DeviceId.
DEVICE_IDS = "8,10"

DATE_FROM = "2024-01-01"
DATE_TO = "2024-01-02"

PHOTO_ROOT = r"\\vld-fs02\FileStorage\Amber"
OUT_DIR = r".\output_amber_device_8_10"

PHOTO_TIME_WINDOW_SECONDS = 2.5
LIMIT_PHOTOS = 0
DRY_RUN = False

# Если точное имя не найдено, искать <AlarmAmberId>_*_<FrameNo>.*.
# Для диагностики лучше True. Если будет медленно, поставь False.
MATCH_BY_ALARM_AND_FRAME_IF_UNIQUE = True

# Рекурсивный поиск по дневной папке обычно очень медленный.
SEARCH_RECURSIVE_IN_DAY_DIR = False

# copyfile быстрее copy2 на сетевых папках.
USE_COPYFILE = True

T_ALARMS = "Fac_Sea_Amber_Alarms"
T_PHOTOS = "Fac_Sea_Amber_AlarmPhotos"
T_CAMERAS = "Fac_Sea_Amber_AlarmDevices"
# Если есть отдельная таблица камер, можно поменять:
# T_CAMERAS = "Fac_Sea_Amber_AlarmCameras"

T_CONTAINERS = "Fac_Sea_Amber_AlarmContainers"
T_CARGO_CONTAINERS = "Cargo_Containers"
T_DETAILS = "Fac_Sea_Amber_AlarmDetails"

ALARM_PK = "Id"
ALARM_AMBER_ID = "AmberId"
ALARM_TIME = "EventDateTime"
ALARM_DEVICE_ID = "DeviceId"

PHOTO_PK = "Id"
PHOTO_DOC_ID = "DocId"
PHOTO_CAMERA_ID = "CameraId"
PHOTO_TIME = "PhotoDateTime"
PHOTO_FRAME_NO = "FrameNo"
PHOTO_DOC_JOIN_ALARM = ALARM_PK

CAMERA_PK = "Id"
CAMERA_AMBER_ID = "AmberId"

CONTAINER_DOC_ID = "DocId"
CONTAINER_CONTAINER_ID = "ContainerId"
CARGO_PK = "Id"

DETAIL_DOC_ID = "DocId"
DETAIL_TIME = "EventDateTime"
DETAIL_GAMMA_COUNT = "GammaCount"
DETAIL_NEUTRON_COUNT = "NeutronCount"

ALLOWED_EXT = (".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff")


# =============================================================================
# HELPERS
# =============================================================================

def q(name: str) -> str:
    return ".".join(f"[{part.replace(']', ']]')}]" for part in str(name).split("."))


def parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value).strip().replace("T", " ")
    return datetime.fromisoformat(text)


def csv_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return "" if value is None else value


def safe_part(value: Any) -> str:
    text = str(value).strip()
    text = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", text)
    text = re.sub(r"\s+", "_", text)
    return text or "empty"


def write_csv(path: Path, header: Sequence[str], rows: Iterable[Sequence[Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(list(header))
        for row in rows:
            writer.writerow([csv_value(x) for x in row])
            count += 1
    return count


def query_to_csv(conn: pyodbc.Connection, sql: str, params: Sequence[Any], path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    cur = conn.cursor()
    cur.execute(sql, *params)
    header = [x[0] for x in cur.description]
    count = 0
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(header)
        for row in cur:
            writer.writerow([csv_value(x) for x in row])
            count += 1
    return count


def connect_db(args: argparse.Namespace) -> pyodbc.Connection:
    if args.integrated_auth:
        conn_str = (
            f"DRIVER={{{args.driver}}};"
            f"SERVER={args.server};"
            f"DATABASE={args.database};"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={{{args.driver}}};"
            f"SERVER={args.server};"
            f"DATABASE={args.database};"
            f"UID={args.user};"
            f"PWD={args.password};"
            "TrustServerCertificate=yes;"
        )
    return pyodbc.connect(conn_str, timeout=30)


def table_exists(conn: pyodbc.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT CASE WHEN OBJECT_ID(?) IS NULL THEN 0 ELSE 1 END", table_name)
    return bool(cur.fetchone()[0])


def get_columns(conn: pyodbc.Connection, table_name: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.name
        FROM sys.columns c
        WHERE c.object_id = OBJECT_ID(?)
        ORDER BY c.column_id
        """,
        table_name,
    )
    return [str(row[0]) for row in cur.fetchall()]


def select_all_columns_expr(conn: pyodbc.Connection, table_name: str, alias: str, prefix: str) -> str:
    cols = get_columns(conn, table_name)
    return ",\n        ".join(f"{alias}.{q(col)} AS {q(prefix + col)}" for col in cols)


def parse_device_ids(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        items = value
    else:
        items = str(value).replace(";", ",").split(",")
    out: List[str] = []
    seen = set()
    for item in items:
        text = str(item).strip()
        if text and text not in seen:
            out.append(text)
            seen.add(text)
    return out


def device_filter_sql(alias: str, device_ids: Sequence[str]) -> Tuple[str, List[Any]]:
    ids = [str(x).strip() for x in device_ids if str(x).strip()]
    if not ids:
        return "", []
    placeholders = ", ".join("?" for _ in ids)
    return f" AND CAST({alias}.{q(ALARM_DEVICE_ID)} AS nvarchar(100)) IN ({placeholders})", ids


def id_variants(value: Any) -> List[str]:
    raw = str(value).strip()
    variants = [raw, raw.strip("{}"), raw.lower().strip("{}"), raw.upper().strip("{}")]
    out: List[str] = []
    seen = set()
    for v in variants:
        if v and v.lower() != "none" and v not in seen:
            out.append(v)
            seen.add(v)
    return out


def frame_variants(value: Any) -> List[str]:
    raw = str(value).strip()
    variants = [raw]
    if raw.isdigit():
        n = int(raw)
        variants += [str(n), f"{n:04d}", f"{n:05d}", f"{n:06d}"]
    out: List[str] = []
    seen = set()
    for v in variants:
        if v and v not in seen:
            out.append(v)
            seen.add(v)
    return out


# =============================================================================
# PHOTO ROWS
# =============================================================================

@dataclass
class PhotoRow:
    alarm_pk: str
    alarm_amber_id: str
    device_id: str
    alarm_time: datetime
    photo_pk: str
    photo_time: datetime
    frame_no: str
    photo_camera_id: str
    camera_pk: str
    camera_amber_id: str
    delta_seconds: float


def fetch_photo_rows(
    conn: pyodbc.Connection,
    dt_from: datetime,
    dt_to: datetime,
    limit_photos: int,
    device_ids: Sequence[str],
) -> List[PhotoRow]:
    top_clause = f"TOP ({int(limit_photos)})" if limit_photos else ""
    window_ms = int(round(PHOTO_TIME_WINDOW_SECONDS * 1000))
    device_where, device_params = device_filter_sql("a", device_ids)

    if table_exists(conn, T_CAMERAS):
        camera_join = f"""
        LEFT JOIN {q(T_CAMERAS)} c
            ON c.{q(CAMERA_PK)} = p.{q(PHOTO_CAMERA_ID)}
        """
        camera_pk_expr = f"COALESCE(CAST(c.{q(CAMERA_PK)} AS nvarchar(100)), CAST(p.{q(PHOTO_CAMERA_ID)} AS nvarchar(100)))"
        camera_amber_expr = f"COALESCE(CAST(c.{q(CAMERA_AMBER_ID)} AS nvarchar(100)), CAST(p.{q(PHOTO_CAMERA_ID)} AS nvarchar(100)))"
    else:
        camera_join = ""
        camera_pk_expr = f"CAST(p.{q(PHOTO_CAMERA_ID)} AS nvarchar(100))"
        camera_amber_expr = f"CAST(p.{q(PHOTO_CAMERA_ID)} AS nvarchar(100))"

    sql = f"""
    SELECT {top_clause}
        CAST(a.{q(ALARM_PK)} AS nvarchar(100)) AS alarm_pk,
        CAST(a.{q(ALARM_AMBER_ID)} AS nvarchar(100)) AS alarm_amber_id,
        CAST(a.{q(ALARM_DEVICE_ID)} AS nvarchar(100)) AS device_id,
        a.{q(ALARM_TIME)} AS alarm_time,

        CAST(p.{q(PHOTO_PK)} AS nvarchar(100)) AS photo_pk,
        p.{q(PHOTO_TIME)} AS photo_time,
        CAST(p.{q(PHOTO_FRAME_NO)} AS nvarchar(100)) AS frame_no,
        CAST(p.{q(PHOTO_CAMERA_ID)} AS nvarchar(100)) AS photo_camera_id,

        {camera_pk_expr} AS camera_pk,
        {camera_amber_expr} AS camera_amber_id,

        DATEDIFF(millisecond, a.{q(ALARM_TIME)}, p.{q(PHOTO_TIME)}) / 1000.0 AS delta_seconds
    FROM {q(T_ALARMS)} a
    JOIN {q(T_PHOTOS)} p
        ON p.{q(PHOTO_DOC_ID)} = a.{q(PHOTO_DOC_JOIN_ALARM)}
    {camera_join}
    WHERE
        a.{q(ALARM_TIME)} >= ?
        AND a.{q(ALARM_TIME)} < ?
        {device_where}
        AND p.{q(PHOTO_TIME)} >= DATEADD(millisecond, -{window_ms}, a.{q(ALARM_TIME)})
        AND p.{q(PHOTO_TIME)} <= DATEADD(millisecond,  {window_ms}, a.{q(ALARM_TIME)})
    ORDER BY
        a.{q(ALARM_DEVICE_ID)},
        a.{q(ALARM_TIME)},
        a.{q(ALARM_PK)},
        p.{q(PHOTO_TIME)},
        p.{q(PHOTO_FRAME_NO)}
    """

    cur = conn.cursor()
    cur.execute(sql, dt_from, dt_to, *device_params)

    out: List[PhotoRow] = []
    for r in cur.fetchall():
        out.append(
            PhotoRow(
                alarm_pk=str(r.alarm_pk),
                alarm_amber_id=str(r.alarm_amber_id),
                device_id=str(r.device_id),
                alarm_time=parse_dt(r.alarm_time),
                photo_pk=str(r.photo_pk),
                photo_time=parse_dt(r.photo_time),
                frame_no=str(r.frame_no),
                photo_camera_id=str(r.photo_camera_id),
                camera_pk=str(r.camera_pk),
                camera_amber_id=str(r.camera_amber_id),
                delta_seconds=float(r.delta_seconds),
            )
        )
    return out


# =============================================================================
# PHOTO FILE SEARCH / COPY
# =============================================================================

def source_day_dir(photo_root: Path, dt: datetime) -> Path:
    return photo_root / f"{dt.year:04d}" / f"{dt.month:02d}" / f"{dt.day:02d}"


def source_name_candidates(row: PhotoRow) -> List[str]:
    names: List[str] = []
    seen = set()

    camera_values = [row.camera_amber_id, row.camera_pk, row.photo_camera_id]

    for alarm in id_variants(row.alarm_amber_id):
        for camera_value in camera_values:
            for camera in id_variants(camera_value):
                for frame in frame_variants(row.frame_no):
                    for ext in ALLOWED_EXT:
                        name = f"{alarm}_{camera}_{frame}{ext}"
                        key = name.lower()
                        if key not in seen:
                            names.append(name)
                            seen.add(key)
    return names


def find_by_alarm_and_frame(base_dir: Path, row: PhotoRow) -> List[Path]:
    candidates: List[Path] = []
    for alarm in id_variants(row.alarm_amber_id):
        for frame in frame_variants(row.frame_no):
            pattern = str(base_dir / f"{alarm}_*_{frame}.*")
            candidates.extend(Path(p) for p in glob.glob(pattern))

    unique: Dict[str, Path] = {}
    for p in candidates:
        if p.is_file() and p.suffix.lower() in ALLOWED_EXT:
            unique[str(p).lower()] = p

    out = list(unique.values())
    out.sort(key=lambda p: p.name.lower())
    return out


def find_source_file(photo_root: Path, row: PhotoRow) -> Optional[Path]:
    dirs = [source_day_dir(photo_root, row.photo_time), source_day_dir(photo_root, row.alarm_time)]
    dirs = list(dict.fromkeys(dirs))
    names = source_name_candidates(row)

    for base_dir in dirs:
        for name in names:
            candidate = base_dir / name
            if candidate.exists():
                return candidate

        if MATCH_BY_ALARM_AND_FRAME_IF_UNIQUE:
            found = find_by_alarm_and_frame(base_dir, row)
            if len(found) == 1:
                return found[0]

        if SEARCH_RECURSIVE_IN_DAY_DIR and base_dir.exists():
            for name in names:
                found = [p for p in base_dir.rglob(name) if p.is_file()]
                if found:
                    found.sort(key=lambda p: str(p).lower())
                    return found[0]

    return None


def destination_file(out_dir: Path, row: PhotoRow, src: Path) -> Path:
    dst_dir = (
        out_dir
        / "photos"
        / f"device_{safe_part(row.device_id)}"
        / f"cam_{safe_part(row.camera_amber_id)}"
        / f"frame_{safe_part(row.frame_no)}"
    )
    dst_dir.mkdir(parents=True, exist_ok=True)
    return dst_dir / src.name


def row_common(row: PhotoRow) -> List[Any]:
    return [
        row.alarm_pk,
        row.alarm_amber_id,
        row.device_id,
        row.alarm_time,
        row.photo_pk,
        row.photo_time,
        round(row.delta_seconds, 3),
        row.camera_pk,
        row.camera_amber_id,
        row.photo_camera_id,
        row.frame_no,
    ]


def dir_preview(path: Path, limit: int = 10) -> str:
    try:
        if not path.exists():
            return "DIR_NOT_EXISTS"
        items = []
        for i, item in enumerate(path.iterdir()):
            if i >= limit:
                items.append("...")
                break
            items.append(item.name)
        return " | ".join(items) if items else "DIR_EMPTY"
    except Exception as exc:
        return f"DIR_ERROR: {exc}"


def copy_file(src: Path, dst: Path) -> None:
    if USE_COPYFILE:
        shutil.copyfile(src, dst)
    else:
        shutil.copy2(src, dst)


def copy_photos(rows: List[PhotoRow], photo_root: Path, out_dir: Path, dry_run: bool) -> None:
    data_dir = out_dir / "_data"
    reports_dir = out_dir / "_reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    header = [
        "alarm_pk", "alarm_amber_id", "device_id", "alarm_time",
        "photo_pk", "photo_time", "delta_seconds",
        "camera_pk", "camera_amber_id", "photo_camera_id", "frame_no",
        "source", "destination", "copy_status",
    ]

    all_rows: List[List[Any]] = []
    copied_rows: List[List[Any]] = []
    already_rows: List[List[Any]] = []
    would_rows: List[List[Any]] = []
    missing_rows: List[List[Any]] = []
    error_rows: List[List[Any]] = []

    for row in tqdm(rows, desc="Copy photos", unit="photo"):
        common = row_common(row)
        try:
            src = find_source_file(photo_root, row)
            if src is None:
                dirs = [source_day_dir(photo_root, row.photo_time), source_day_dir(photo_root, row.alarm_time)]
                dirs = list(dict.fromkeys(dirs))
                all_rows.append(common + ["", "", "missing"])
                missing_rows.append(
                    common + [
                        " | ".join(source_name_candidates(row)[:40]),
                        " | ".join(str(d) for d in dirs),
                        " | ".join(str(d.exists()) for d in dirs),
                        " || ".join(dir_preview(d) for d in dirs),
                    ]
                )
                continue

            dst = destination_file(out_dir, row, src)

            if dry_run:
                status = "would_copy_dry_run"
                would_rows.append(common + [str(src), str(dst)])
            elif dst.exists():
                status = "already_exists"
                already_rows.append(common + [str(src), str(dst)])
            else:
                copy_file(src, dst)
                status = "copied"
                copied_rows.append(common + [str(src), str(dst)])

            all_rows.append(common + [str(src), str(dst), status])

        except Exception as exc:
            all_rows.append(common + ["", "", "error"])
            error_rows.append(common + [repr(exc)])

    write_csv(data_dir / "photos.csv", header, all_rows)
    write_csv(data_dir / "recognition_input.csv", header, [r for r in all_rows if r[-1] not in ("missing", "error")])

    write_csv(reports_dir / "copied.csv", header[:-1], copied_rows)
    write_csv(reports_dir / "already_exists.csv", header[:-1], already_rows)
    write_csv(reports_dir / "would_copy_dry_run.csv", header[:-1], would_rows)
    write_csv(
        reports_dir / "missing.csv",
        header[:11] + ["expected_names", "expected_day_dirs", "day_dirs_exist", "day_dirs_preview"],
        missing_rows,
    )
    write_csv(reports_dir / "errors.csv", header[:11] + ["error"], error_rows)

    print()
    print("Photo result:")
    print(f"  total rows: {len(rows)}")
    print(f"  copied: {len(copied_rows)}")
    print(f"  already_exists: {len(already_rows)}")
    print(f"  would_copy_dry_run: {len(would_rows)}")
    print(f"  missing: {len(missing_rows)}")
    print(f"  errors: {len(error_rows)}")


# =============================================================================
# METADATA CSV
# =============================================================================

def export_alarms(conn: pyodbc.Connection, dt_from: datetime, dt_to: datetime, data_dir: Path, device_ids: Sequence[str]) -> None:
    device_where, device_params = device_filter_sql("a", device_ids)
    sql = f"""
    SELECT a.*
    FROM {q(T_ALARMS)} a
    WHERE a.{q(ALARM_TIME)} >= ? AND a.{q(ALARM_TIME)} < ? {device_where}
    ORDER BY a.{q(ALARM_DEVICE_ID)}, a.{q(ALARM_TIME)}, a.{q(ALARM_PK)}
    """
    count = query_to_csv(conn, sql, [dt_from, dt_to, *device_params], data_dir / "alarms.csv")
    print(f"alarms.csv: {count}")


def export_manual_containers(conn: pyodbc.Connection, dt_from: datetime, dt_to: datetime, data_dir: Path, device_ids: Sequence[str]) -> None:
    if not table_exists(conn, T_CONTAINERS):
        write_csv(data_dir / "manual_containers.csv", ["error"], [(f"Table not found: {T_CONTAINERS}",)])
        print("manual_containers.csv: table not found")
        return

    device_where, device_params = device_filter_sql("a", device_ids)
    a_cols = select_all_columns_expr(conn, T_ALARMS, "a", "alarm_")
    ac_cols = select_all_columns_expr(conn, T_CONTAINERS, "ac", "alarm_container_")

    cargo_join = ""
    cargo_cols = ""
    if table_exists(conn, T_CARGO_CONTAINERS):
        cargo_cols = select_all_columns_expr(conn, T_CARGO_CONTAINERS, "cc", "cargo_")
        cargo_join = f"""
        LEFT JOIN {q(T_CARGO_CONTAINERS)} cc
            ON cc.{q(CARGO_PK)} = ac.{q(CONTAINER_CONTAINER_ID)}
        """

    select_cols = ",\n        ".join(x for x in [a_cols, ac_cols, cargo_cols] if x)
    sql = f"""
    SELECT
        {select_cols}
    FROM {q(T_ALARMS)} a
    JOIN {q(T_CONTAINERS)} ac
        ON ac.{q(CONTAINER_DOC_ID)} = a.{q(ALARM_PK)}
    {cargo_join}
    WHERE a.{q(ALARM_TIME)} >= ? AND a.{q(ALARM_TIME)} < ? {device_where}
    ORDER BY a.{q(ALARM_DEVICE_ID)}, a.{q(ALARM_TIME)}, a.{q(ALARM_PK)}
    """
    count = query_to_csv(conn, sql, [dt_from, dt_to, *device_params], data_dir / "manual_containers.csv")
    print(f"manual_containers.csv: {count}")


def export_alarm_details(conn: pyodbc.Connection, dt_from: datetime, dt_to: datetime, data_dir: Path, device_ids: Sequence[str]) -> None:
    if not table_exists(conn, T_DETAILS):
        write_csv(data_dir / "alarm_details.csv", ["error"], [(f"Table not found: {T_DETAILS}",)])
        print("alarm_details.csv: table not found")
        return

    device_where, device_params = device_filter_sql("a", device_ids)
    a_cols = select_all_columns_expr(conn, T_ALARMS, "a", "alarm_")
    d_cols = select_all_columns_expr(conn, T_DETAILS, "d", "detail_")
    select_cols = ",\n        ".join(x for x in [a_cols, d_cols] if x)

    sql = f"""
    SELECT
        DATEDIFF(millisecond, a.{q(ALARM_TIME)}, d.{q(DETAIL_TIME)}) / 1000.0 AS detail_delta_seconds,
        {select_cols}
    FROM {q(T_ALARMS)} a
    JOIN {q(T_DETAILS)} d
        ON d.{q(DETAIL_DOC_ID)} = a.{q(ALARM_PK)}
    WHERE a.{q(ALARM_TIME)} >= ? AND a.{q(ALARM_TIME)} < ? {device_where}
    ORDER BY a.{q(ALARM_DEVICE_ID)}, a.{q(ALARM_TIME)}, a.{q(ALARM_PK)}, d.{q(DETAIL_TIME)}
    """
    count = query_to_csv(conn, sql, [dt_from, dt_to, *device_params], data_dir / "alarm_details.csv")
    print(f"alarm_details.csv: {count}")


def export_gamma_maxima(conn: pyodbc.Connection, dt_from: datetime, dt_to: datetime, data_dir: Path, device_ids: Sequence[str]) -> None:
    if not table_exists(conn, T_DETAILS):
        write_csv(data_dir / "gamma_maxima.csv", ["error"], [(f"Table not found: {T_DETAILS}",)])
        print("gamma_maxima.csv: table not found")
        return

    columns = set(get_columns(conn, T_DETAILS))
    if DETAIL_DOC_ID not in columns or DETAIL_TIME not in columns or DETAIL_GAMMA_COUNT not in columns:
        write_csv(data_dir / "gamma_maxima.csv", ["error"], [(f"Missing required detail columns in {T_DETAILS}",)])
        print("gamma_maxima.csv: missing columns")
        return

    device_where, device_params = device_filter_sql("a", device_ids)
    neutron_expr = (
        f"TRY_CONVERT(float, d.{q(DETAIL_NEUTRON_COUNT)}) AS neutron_count"
        if DETAIL_NEUTRON_COUNT in columns
        else "CAST(NULL AS float) AS neutron_count"
    )

    sql = f"""
    SELECT
        CAST(a.{q(ALARM_PK)} AS nvarchar(100)) AS alarm_pk,
        CAST(a.{q(ALARM_AMBER_ID)} AS nvarchar(100)) AS alarm_amber_id,
        CAST(a.{q(ALARM_DEVICE_ID)} AS nvarchar(100)) AS device_id,
        a.{q(ALARM_TIME)} AS alarm_time,
        d.{q(DETAIL_TIME)} AS detail_time,
        TRY_CONVERT(float, d.{q(DETAIL_GAMMA_COUNT)}) AS gamma_count,
        {neutron_expr}
    FROM {q(T_ALARMS)} a
    JOIN {q(T_DETAILS)} d
        ON d.{q(DETAIL_DOC_ID)} = a.{q(ALARM_PK)}
    WHERE a.{q(ALARM_TIME)} >= ? AND a.{q(ALARM_TIME)} < ? {device_where}
    ORDER BY a.{q(ALARM_DEVICE_ID)}, a.{q(ALARM_PK)}, d.{q(DETAIL_TIME)}
    """

    cur = conn.cursor()
    cur.execute(sql, dt_from, dt_to, *device_params)

    by_alarm: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in cur.fetchall():
        if r.gamma_count is None:
            continue
        alarm_time = parse_dt(r.alarm_time)
        detail_time = parse_dt(r.detail_time)
        by_alarm[str(r.alarm_pk)].append(
            {
                "alarm_pk": str(r.alarm_pk),
                "alarm_amber_id": str(r.alarm_amber_id),
                "device_id": str(r.device_id),
                "alarm_time": alarm_time,
                "detail_time": detail_time,
                "delta_seconds": round((detail_time - alarm_time).total_seconds(), 3),
                "gamma_count": float(r.gamma_count),
                "neutron_count": "" if r.neutron_count is None else float(r.neutron_count),
            }
        )

    out_rows: List[Tuple[Any, ...]] = []
    for alarm_pk, items in by_alarm.items():
        items.sort(key=lambda x: x["detail_time"])
        absolute = max(items, key=lambda x: x["gamma_count"])
        out_rows.append(
            (
                "absolute",
                absolute["alarm_pk"],
                absolute["alarm_amber_id"],
                absolute["device_id"],
                absolute["alarm_time"],
                absolute["detail_time"],
                absolute["delta_seconds"],
                absolute["gamma_count"],
                absolute["neutron_count"],
            )
        )

        for i, item in enumerate(items):
            prev_gamma = items[i - 1]["gamma_count"] if i > 0 else None
            next_gamma = items[i + 1]["gamma_count"] if i + 1 < len(items) else None
            left_ok = prev_gamma is None or item["gamma_count"] >= prev_gamma
            right_ok = next_gamma is None or item["gamma_count"] >= next_gamma
            strict = (
                (prev_gamma is not None and item["gamma_count"] > prev_gamma)
                or (next_gamma is not None and item["gamma_count"] > next_gamma)
            )
            if left_ok and right_ok and strict:
                out_rows.append(
                    (
                        "local",
                        item["alarm_pk"],
                        item["alarm_amber_id"],
                        item["device_id"],
                        item["alarm_time"],
                        item["detail_time"],
                        item["delta_seconds"],
                        item["gamma_count"],
                        item["neutron_count"],
                    )
                )

    count = write_csv(
        data_dir / "gamma_maxima.csv",
        ["max_type", "alarm_pk", "alarm_amber_id", "device_id", "alarm_time", "detail_time", "delta_seconds", "gamma_count", "neutron_count"],
        out_rows,
    )
    print(f"gamma_maxima.csv: {count}")


def export_metadata(conn: pyodbc.Connection, dt_from: datetime, dt_to: datetime, out_dir: Path, device_ids: Sequence[str]) -> None:
    data_dir = out_dir / "_data"
    data_dir.mkdir(parents=True, exist_ok=True)
    print("Export metadata CSV...")
    export_alarms(conn, dt_from, dt_to, data_dir, device_ids)
    export_manual_containers(conn, dt_from, dt_to, data_dir, device_ids)
    export_alarm_details(conn, dt_from, dt_to, data_dir, device_ids)
    export_gamma_maxima(conn, dt_from, dt_to, data_dir, device_ids)


# =============================================================================
# RUN
# =============================================================================

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export photos and CSV only for selected Amber DeviceId values.")

    parser.add_argument("--server", default=DB_SERVER)
    parser.add_argument("--database", default=DB_NAME)
    parser.add_argument("--driver", default=DB_DRIVER)

    parser.add_argument("--integrated-auth", dest="integrated_auth", action="store_true", default=USE_INTEGRATED_AUTH)
    parser.add_argument("--sql-auth", dest="integrated_auth", action="store_false")
    parser.add_argument("--user", default=DB_USER)
    parser.add_argument("--password", default=DB_PASSWORD)

    parser.add_argument("--device-ids", default=DEVICE_IDS, help='DeviceId через запятую, например "8,10". Пусто = все.')
    parser.add_argument("--from-date", default=DATE_FROM, dest="date_from")
    parser.add_argument("--to-date", default=DATE_TO, dest="date_to")

    parser.add_argument("--photo-root", default=PHOTO_ROOT)
    parser.add_argument("--out-dir", default=OUT_DIR)
    parser.add_argument("--photo-window-seconds", type=float, default=PHOTO_TIME_WINDOW_SECONDS)
    parser.add_argument("--limit-photos", type=int, default=LIMIT_PHOTOS)

    parser.add_argument("--dry-run", dest="dry_run", action="store_true", default=DRY_RUN)
    parser.add_argument("--no-dry-run", dest="dry_run", action="store_false")

    return parser


def main() -> None:
    args = build_arg_parser().parse_args()

    missing = []
    if not args.server:
        missing.append("DB_SERVER / --server")
    if not args.database:
        missing.append("DB_NAME / --database")
    if not args.integrated_auth and not args.user:
        missing.append("DB_USER / --user")
    if not args.date_from:
        missing.append("DATE_FROM / --from-date")
    if not args.date_to:
        missing.append("DATE_TO / --to-date")
    if missing:
        raise SystemExit("Не заполнены настройки:\n  - " + "\n  - ".join(missing))

    global PHOTO_TIME_WINDOW_SECONDS
    PHOTO_TIME_WINDOW_SECONDS = args.photo_window_seconds

    device_ids = parse_device_ids(args.device_ids)
    dt_from = parse_dt(args.date_from)
    dt_to = parse_dt(args.date_to)
    photo_root = Path(args.photo_root)
    out_dir = Path(args.out_dir)

    print("Settings:")
    print(f"  server: {args.server}")
    print(f"  database: {args.database}")
    print(f"  device_ids: {', '.join(device_ids) if device_ids else 'all'}")
    print(f"  period: {dt_from} -> {dt_to}")
    print(f"  photo_root: {photo_root}")
    print(f"  out_dir: {out_dir}")
    print(f"  photo_window_seconds: {PHOTO_TIME_WINDOW_SECONDS}")
    print(f"  limit_photos: {args.limit_photos or 'no limit'}")
    print(f"  dry_run: {args.dry_run}")
    print()

    print("Connecting to MSSQL...")
    with connect_db(args) as conn:
        print("Fetching photo rows...")
        rows = fetch_photo_rows(conn, dt_from, dt_to, args.limit_photos, device_ids)
        print(f"Rows fetched: {len(rows)}")

        copy_photos(rows, photo_root, out_dir, args.dry_run)
        export_metadata(conn, dt_from, dt_to, out_dir, device_ids)

    print()
    print("Done.")
    print(f"Photos: {out_dir / 'photos'}")
    print(f"Data CSV: {out_dir / '_data'}")
    print(f"Reports: {out_dir / '_reports'}")


if __name__ == "__main__":
    main()
