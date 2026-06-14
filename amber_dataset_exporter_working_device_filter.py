# amber_dataset_exporter_working_device_filter.py
# Нормальный скрипт для подготовки данных по задаче Янтарь.
#
# Делает сразу всё, что нужно перед пунктами 2-6:
#   1. Берёт из БД срабатывания Alarms за период.
#   2. Берёт фото из AlarmPhotos только в окне EventDateTime ± PHOTO_TIME_WINDOW_SECONDS.
#   3. Ищет файлы в \\vld-fs02\FileStorage\Amber\yyyy\MM\dd\
#      по имени:
#        {Alarms.AmberId}_{AlarmCameras/AlarmDevices.AmberId}_{AlarmPhotos.FrameNo}.jpg
#   4. Копирует фото в:
#        OUT_DIR\photos\device_<DeviceId>\cam_<CameraAmberId>\frame_<FrameNo>\
#   5. Сохраняет CSV для всех следующих пунктов задания:
#        OUT_DIR\_data\alarms.csv
#        OUT_DIR\_data\photos.csv
#        OUT_DIR\_data\recognition_input.csv
#        OUT_DIR\_data\manual_containers.csv
#        OUT_DIR\_data\alarm_details.csv
#        OUT_DIR\_data\gamma_maxima.csv
#        OUT_DIR\_data\analysis_join.csv
#        OUT_DIR\_reports\copied.csv
#        OUT_DIR\_reports\missing.csv
#        OUT_DIR\_reports\errors.csv
#
# Запуск:
#   pip install pyodbc tqdm
#   python amber_dataset_exporter_working_device_filter.py
#
# Для теста:
#   python amber_dataset_exporter_working_device_filter.py --limit-photos 1000
#
# Важно:
#   Если в твоей БД таблица камер называется Fac_Sea_Amber_AlarmCameras,
#   поменяй T_CAMERAS ниже. По твоим скриншотам похожа на Fac_Sea_Amber_AlarmDevices.

from __future__ import annotations

from collections import defaultdict
import argparse
import csv
import glob
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pyodbc
from tqdm import tqdm



# Эта версия сделана именно из рабочего amber_dataset_exporter.py,
# а не из упрощённого скрипта, который не находил фото.
# Отличие от рабочего скрипта: добавлен только фильтр DEVICE_IDS / --device-ids.

# =============================================================================
# НАСТРОЙКИ: ЗАПОЛНИТЬ ПОД СЕБЯ
# =============================================================================

DB_SERVER = r""          # пример: r"TOS01" или r"TOS01\INSTANCE"
DB_NAME = r""            # имя базы
DB_DRIVER = "ODBC Driver 17 for SQL Server"

# True  = Windows-аутентификация.
# False = SQL-логин/пароль ниже.
USE_INTEGRATED_AUTH = True
DB_USER = r""
DB_PASSWORD = r""

DATE_FROM = "2024-01-01"  # включительно
DATE_TO = "2024-01-02"    # НЕ включительно

# Фильтр по Янтарям / DeviceId.
# Для пункта 4 сейчас удобно вытащить отдельно проблемные DeviceId 8 и 10.
# Пустая строка = без фильтра, все DeviceId.
DEVICE_IDS = "8,10"

PHOTO_ROOT = r"\\vld-fs02\FileStorage\Amber"
OUT_DIR = r".\output_amber_device_8_10"

# Фото брать только в окне EventDateTime ± N секунд.
PHOTO_TIME_WINDOW_SECONDS = 2.5

# 0 = без ограничения.
LIMIT_PHOTOS = 0

# True = только проверить и создать CSV, но физически НЕ копировать файлы.
DRY_RUN = False

# На сетевой папке рекурсивный поиск может быть медленным. Обычно False.
SEARCH_RECURSIVE_IN_DAY_DIR = False

# Если точное имя не найдено, пробовать <AlarmAmberId>_*_<FrameNo>.*.
# Используется только если кандидат ровно один.
MATCH_BY_ALARM_AND_FRAME_IF_UNIQUE = True


# =============================================================================
# НАСТРОЙКИ СХЕМЫ БД
# =============================================================================

T_ALARMS = "Fac_Sea_Amber_Alarms"
T_PHOTOS = "Fac_Sea_Amber_AlarmPhotos"

# По заданию это AlarmCameras, но на твоём скриншоте структура похожа на AlarmDevices.
# Важно, чтобы таблица содержала Id и AmberId камеры.
T_CAMERAS = "Fac_Sea_Amber_AlarmDevices"
# Если в БД реально есть таблица AlarmCameras, поменять на:
# T_CAMERAS = "Fac_Sea_Amber_AlarmCameras"

T_CONTAINERS = "Fac_Sea_Amber_AlarmContainers"
T_DETAILS = "Fac_Sea_Amber_AlarmDetails"
T_CARGO_CONTAINERS = "Cargo_Containers"

# Alarms
ALARM_PK = "Id"
ALARM_AMBER_ID = "AmberId"
ALARM_TIME = "EventDateTime"
ALARM_DEVICE_ID = "DeviceId"

# AlarmPhotos
PHOTO_PK = "Id"
PHOTO_DOC_ID = "DocId"
PHOTO_CAMERA_ID = "CameraId"
PHOTO_TIME = "PhotoDateTime"
PHOTO_FRAME_NO = "FrameNo"

# Обычно AlarmPhotos.DocId = Alarms.Id.
# Если окажется, что DocId ссылается на Alarms.AmberId, поменять на ALARM_AMBER_ID.
PHOTO_DOC_JOIN_ALARM = ALARM_PK

# Cameras / Devices
CAMERA_PK = "Id"
CAMERA_AMBER_ID = "AmberId"

# AlarmContainers
CONTAINER_DOC_ID = "DocId"
CONTAINER_CONTAINER_ID = "ContainerId"

# Cargo_Containers
CARGO_PK = "Id"
CARGO_FULL_NUMBER = "FullContNumber"

# AlarmDetails
DETAIL_DOC_ID = "DocId"
DETAIL_TIME = "EventDateTime"
DETAIL_GAMMA_COUNT = "GammaCount"
DETAIL_NEUTRON_COUNT = "NeutronCount"

ALLOWED_EXT = (".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff")


# =============================================================================
# МОДЕЛИ
# =============================================================================

@dataclass(frozen=True)
class PhotoRow:
    alarm_pk: str
    alarm_amber_id: str
    device_id: str
    alarm_time: datetime

    photo_pk: str
    photo_time: datetime
    frame_no: str

    camera_pk: str
    camera_amber_id: str

    delta_seconds: float


# =============================================================================
# УТИЛИТЫ
# =============================================================================

def q(name: str) -> str:
    return ".".join(f"[{part.replace(']', ']]')}]" for part in str(name).split("."))


def parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).strip().replace("T", " "))


def safe_part(value: Any) -> str:
    text = str(value).strip()
    text = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", text)
    text = re.sub(r"\s+", "_", text)
    return text or "empty"


def csv_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return "" if value is None else value


def normalize_container_number(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"[^A-Z0-9]", "", str(value).upper().strip())


def id_variants(value: Any) -> List[str]:
    raw = str(value).strip()
    variants = [
        raw,
        raw.strip("{}"),
        raw.lower().strip("{}"),
        raw.upper().strip("{}"),
    ]

    result: List[str] = []
    seen = set()
    for item in variants:
        if item and item.lower() != "none" and item not in seen:
            seen.add(item)
            result.append(item)
    return result


def frame_variants(value: Any) -> List[str]:
    raw = str(value).strip()
    variants = [raw]

    if raw.isdigit():
        n = int(raw)
        variants += [str(n), f"{n:04d}", f"{n:05d}", f"{n:06d}"]

    result: List[str] = []
    seen = set()
    for item in variants:
        if item and item not in seen:
            seen.add(item)
            result.append(item)
    return result


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

    header = [item[0] for item in cur.description]
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


def select_all_columns_expr(conn: pyodbc.Connection, table: str, alias: str, prefix: str) -> str:
    cols = get_columns(conn, table)
    return ",\n        ".join(f"{alias}.{q(col)} AS {q(prefix + col)}" for col in cols)


def detect_column(columns: Sequence[str], variants: Sequence[str]) -> Optional[str]:
    by_lower = {col.lower(): col for col in columns}

    for variant in variants:
        if variant.lower() in by_lower:
            return by_lower[variant.lower()]

    for col in columns:
        col_lower = col.lower()
        for variant in variants:
            if variant.lower() in col_lower:
                return col

    return None


# =============================================================================
# SQL: ФОТО
# =============================================================================


def parse_device_ids(value: Any) -> List[str]:
    """
    Принимает строку вида "8,10" или список значений.
    Возвращает список DeviceId как строки.
    Пустое значение = без фильтра.
    """
    if value is None:
        return []

    if isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raw_items = str(value).replace(";", ",").split(",")

    result: List[str] = []
    seen = set()

    for item in raw_items:
        text = str(item).strip()
        if not text:
            continue
        if text not in seen:
            seen.add(text)
            result.append(text)

    return result


def device_filter_sql(alias: str, device_ids: Sequence[str]) -> Tuple[str, List[Any]]:
    """
    Возвращает кусок SQL и параметры для фильтра по DeviceId.
    CAST нужен, чтобы одинаково работало и для int, и для uniqueidentifier/nvarchar.
    """
    ids = [str(x).strip() for x in device_ids if str(x).strip()]
    if not ids:
        return "", []

    placeholders = ", ".join("?" for _ in ids)
    return f" AND CAST({alias}.{q(ALARM_DEVICE_ID)} AS nvarchar(100)) IN ({placeholders})", ids


def fetch_photo_rows(conn: pyodbc.Connection, dt_from: datetime, dt_to: datetime, limit_photos: int, device_ids: Sequence[str]) -> List[PhotoRow]:
    top_clause = f"TOP ({int(limit_photos)})" if limit_photos else ""
    window_ms = int(round(PHOTO_TIME_WINDOW_SECONDS * 1000))
    device_where, device_params = device_filter_sql("a", device_ids)

    sql = f"""
    SELECT {top_clause}
        CAST(a.{q(ALARM_PK)} AS nvarchar(100)) AS alarm_pk,
        CAST(a.{q(ALARM_AMBER_ID)} AS nvarchar(100)) AS alarm_amber_id,
        CAST(a.{q(ALARM_DEVICE_ID)} AS nvarchar(100)) AS device_id,
        a.{q(ALARM_TIME)} AS alarm_time,

        CAST(p.{q(PHOTO_PK)} AS nvarchar(100)) AS photo_pk,
        p.{q(PHOTO_TIME)} AS photo_time,
        CAST(p.{q(PHOTO_FRAME_NO)} AS nvarchar(100)) AS frame_no,

        CAST(c.{q(CAMERA_PK)} AS nvarchar(100)) AS camera_pk,
        CAST(c.{q(CAMERA_AMBER_ID)} AS nvarchar(100)) AS camera_amber_id,

        DATEDIFF(millisecond, a.{q(ALARM_TIME)}, p.{q(PHOTO_TIME)}) / 1000.0 AS delta_seconds
    FROM {q(T_ALARMS)} a
    JOIN {q(T_PHOTOS)} p
        ON p.{q(PHOTO_DOC_ID)} = a.{q(PHOTO_DOC_JOIN_ALARM)}
    JOIN {q(T_CAMERAS)} c
        ON c.{q(CAMERA_PK)} = p.{q(PHOTO_CAMERA_ID)}
    WHERE
        a.{q(ALARM_TIME)} >= ?
        AND a.{q(ALARM_TIME)} < ?
        {device_where}
        AND p.{q(PHOTO_TIME)} >= DATEADD(millisecond, -{window_ms}, a.{q(ALARM_TIME)})
        AND p.{q(PHOTO_TIME)} <= DATEADD(millisecond,  {window_ms}, a.{q(ALARM_TIME)})
    ORDER BY
        a.{q(ALARM_TIME)},
        a.{q(ALARM_PK)},
        c.{q(CAMERA_AMBER_ID)},
        p.{q(PHOTO_TIME)},
        p.{q(PHOTO_FRAME_NO)}
    """

    cur = conn.cursor()
    cur.execute(sql, dt_from, dt_to, *device_params)

    result: List[PhotoRow] = []
    for row in cur.fetchall():
        result.append(
            PhotoRow(
                alarm_pk=str(row.alarm_pk),
                alarm_amber_id=str(row.alarm_amber_id),
                device_id=str(row.device_id),
                alarm_time=parse_dt(row.alarm_time),
                photo_pk=str(row.photo_pk),
                photo_time=parse_dt(row.photo_time),
                frame_no=str(row.frame_no),
                camera_pk=str(row.camera_pk),
                camera_amber_id=str(row.camera_amber_id),
                delta_seconds=float(row.delta_seconds),
            )
        )

    return result


# =============================================================================
# ФАЙЛЫ ФОТО
# =============================================================================

def source_day_dir(photo_root: Path, dt: datetime) -> Path:
    return photo_root / f"{dt.year:04d}" / f"{dt.month:02d}" / f"{dt.day:02d}"


def source_name_candidates(row: PhotoRow) -> List[str]:
    names: List[str] = []
    seen = set()

    def add(name: str) -> None:
        key = name.lower()
        if key not in seen:
            seen.add(key)
            names.append(name)

    # Главное имя из задания.
    for ext in (".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff"):
        add(f"{row.alarm_amber_id}_{row.camera_amber_id}_{row.frame_no}{ext}")

    # Варианты регистра/скобок/GUID и нулей в FrameNo.
    for alarm in id_variants(row.alarm_amber_id):
        for camera in id_variants(row.camera_amber_id):
            for frame in frame_variants(row.frame_no):
                for ext in (".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff"):
                    add(f"{alarm}_{camera}_{frame}{ext}")

    return names


def find_by_alarm_and_frame(base_dir: Path, row: PhotoRow) -> List[Path]:
    candidates: List[Path] = []

    for alarm in id_variants(row.alarm_amber_id):
        for frame in frame_variants(row.frame_no):
            pattern = str(base_dir / f"{alarm}_*_{frame}.*")
            found = [Path(p) for p in glob.glob(pattern)]
            found = [p for p in found if p.is_file() and p.suffix.lower() in ALLOWED_EXT]
            candidates.extend(found)

    unique: Dict[str, Path] = {}
    for item in candidates:
        unique[str(item).lower()] = item

    result = list(unique.values())
    result.sort(key=lambda p: p.name.lower())
    return result


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
            candidates = find_by_alarm_and_frame(base_dir, row)
            if len(candidates) == 1:
                return candidates[0]

        if SEARCH_RECURSIVE_IN_DAY_DIR and base_dir.exists():
            for name in names:
                matches = [p for p in base_dir.rglob(name) if p.is_file()]
                if matches:
                    matches.sort(key=lambda p: str(p).lower())
                    return matches[0]

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


def copy_photos_and_make_datasets(rows: List[PhotoRow], out_dir: Path, photo_root: Path, dry_run: bool) -> None:
    data_dir = out_dir / "_data"
    reports_dir = out_dir / "_reports"

    photos_rows: List[Tuple[Any, ...]] = []
    recognition_rows: List[Tuple[Any, ...]] = []
    copied_rows: List[Tuple[Any, ...]] = []
    already_exists_rows: List[Tuple[Any, ...]] = []
    would_copy_rows: List[Tuple[Any, ...]] = []
    missing_rows: List[Tuple[Any, ...]] = []
    error_rows: List[Tuple[Any, ...]] = []

    for row in tqdm(rows, desc="Copy photos", unit="photo"):
        try:
            src = find_source_file(photo_root, row)

            common = (
                row.alarm_pk,
                row.alarm_amber_id,
                row.device_id,
                row.alarm_time,
                row.photo_pk,
                row.photo_time,
                round(row.delta_seconds, 3),
                row.camera_pk,
                row.camera_amber_id,
                row.frame_no,
            )

            if src is None:
                dirs = [source_day_dir(photo_root, row.photo_time), source_day_dir(photo_root, row.alarm_time)]
                dirs = list(dict.fromkeys(dirs))

                expected_names = " | ".join(source_name_candidates(row)[:20])
                expected_dirs = " | ".join(str(d) for d in dirs)
                dirs_exist = " | ".join(str(d.exists()) for d in dirs)
                preview = " || ".join(dir_preview(d) for d in dirs)

                missing_rows.append(common + (expected_names, expected_dirs, dirs_exist, preview))
                photos_rows.append(common + ("", "", "missing"))
                continue

            dst = destination_file(out_dir, row, src)

            if dry_run:
                copy_status = "would_copy_dry_run"
                would_copy_rows.append(common + (str(src), str(dst)))
            else:
                if dst.exists():
                    copy_status = "already_exists"
                    already_exists_rows.append(common + (str(src), str(dst)))
                else:
                    shutil.copy2(src, dst)
                    copy_status = "copied"
                    copied_rows.append(common + (str(src), str(dst)))

            dataset_row = common + (str(src), str(dst), copy_status)
            photos_rows.append(dataset_row)
            recognition_rows.append(dataset_row)

        except Exception as exc:
            error_rows.append(
                (
                    row.alarm_pk,
                    row.alarm_amber_id,
                    row.device_id,
                    row.alarm_time,
                    row.photo_pk,
                    row.photo_time,
                    round(row.delta_seconds, 3),
                    row.camera_pk,
                    row.camera_amber_id,
                    row.frame_no,
                    repr(exc),
                )
            )

    base_header = [
        "alarm_pk",
        "alarm_amber_id",
        "device_id",
        "alarm_time",
        "photo_pk",
        "photo_time",
        "delta_seconds",
        "camera_pk",
        "camera_amber_id",
        "frame_no",
    ]

    photo_header = base_header + ["source", "destination", "copy_status"]
    copy_report_header = base_header + ["source", "destination"]

    write_csv(data_dir / "photos.csv", photo_header, photos_rows)
    write_csv(data_dir / "recognition_input.csv", photo_header, recognition_rows)

    write_csv(
        data_dir / "recognition_results_template.csv",
        photo_header
        + [
            "recognized_container_number",
            "confidence",
            "service_status",
            "is_correct",
            "manual_container_numbers",
            "raw_json",
        ],
        [],
    )

    write_csv(reports_dir / "copied.csv", copy_report_header, copied_rows)
    write_csv(reports_dir / "already_exists.csv", copy_report_header, already_exists_rows)
    write_csv(reports_dir / "would_copy_dry_run.csv", copy_report_header, would_copy_rows)

    write_csv(
        reports_dir / "missing.csv",
        base_header + ["expected_file_names", "expected_day_dirs", "day_dirs_exist", "day_dirs_preview"],
        missing_rows,
    )

    write_csv(
        reports_dir / "errors.csv",
        base_header + ["error"],
        error_rows,
    )

    print()
    print(f"Всего строк фото из БД: {len(rows)}")

    if dry_run:
        print(f"DRY_RUN=True, физически не копировал. Нашёл файлов для копирования: {len(would_copy_rows)}")
    else:
        print(f"Скопировано новых файлов: {len(copied_rows)}")
        print(f"Уже было на месте: {len(already_exists_rows)}")

    print(f"Не найдено файлов: {len(missing_rows)}")
    print(f"Ошибок: {len(error_rows)}")
    print(f"Фото и CSV: {out_dir.resolve()}")


# =============================================================================
# ЭКСПОРТ CSV ДЛЯ ПУНКТОВ ЗАДАНИЯ
# =============================================================================

def export_alarms(conn: pyodbc.Connection, dt_from: datetime, dt_to: datetime, data_dir: Path, device_ids: Sequence[str]) -> None:
    device_where, device_params = device_filter_sql("a", device_ids)
    sql = f"""
    SELECT a.*
    FROM {q(T_ALARMS)} a
    WHERE a.{q(ALARM_TIME)} >= ? AND a.{q(ALARM_TIME)} < ? {device_where}
    ORDER BY a.{q(ALARM_TIME)}, a.{q(ALARM_PK)}
    """
    count = query_to_csv(conn, sql, [dt_from, dt_to, *device_params], data_dir / "alarms.csv")
    print(f"alarms.csv: {count}")


def export_photo_db_full(conn: pyodbc.Connection, dt_from: datetime, dt_to: datetime, data_dir: Path, limit_photos: int, device_ids: Sequence[str]) -> None:
    top_clause = f"TOP ({int(limit_photos)})" if limit_photos else ""
    window_ms = int(round(PHOTO_TIME_WINDOW_SECONDS * 1000))
    device_where, device_params = device_filter_sql("a", device_ids)

    a_cols = select_all_columns_expr(conn, T_ALARMS, "a", "alarm_")
    p_cols = select_all_columns_expr(conn, T_PHOTOS, "p", "photo_")
    c_cols = select_all_columns_expr(conn, T_CAMERAS, "c", "camera_")
    select_cols = ",\n        ".join([x for x in [a_cols, p_cols, c_cols] if x])

    sql = f"""
    SELECT {top_clause}
        DATEDIFF(millisecond, a.{q(ALARM_TIME)}, p.{q(PHOTO_TIME)}) / 1000.0 AS delta_seconds,
        {select_cols}
    FROM {q(T_ALARMS)} a
    JOIN {q(T_PHOTOS)} p
        ON p.{q(PHOTO_DOC_ID)} = a.{q(PHOTO_DOC_JOIN_ALARM)}
    JOIN {q(T_CAMERAS)} c
        ON c.{q(CAMERA_PK)} = p.{q(PHOTO_CAMERA_ID)}
    WHERE
        a.{q(ALARM_TIME)} >= ?
        AND a.{q(ALARM_TIME)} < ?
        {device_where}
        AND p.{q(PHOTO_TIME)} >= DATEADD(millisecond, -{window_ms}, a.{q(ALARM_TIME)})
        AND p.{q(PHOTO_TIME)} <= DATEADD(millisecond,  {window_ms}, a.{q(ALARM_TIME)})
    ORDER BY
        a.{q(ALARM_TIME)},
        a.{q(ALARM_PK)},
        c.{q(CAMERA_AMBER_ID)},
        p.{q(PHOTO_TIME)},
        p.{q(PHOTO_FRAME_NO)}
    """
    count = query_to_csv(conn, sql, [dt_from, dt_to, *device_params], data_dir / "photos_db_full.csv")
    print(f"photos_db_full.csv: {count}")


def export_manual_containers(conn: pyodbc.Connection, dt_from: datetime, dt_to: datetime, data_dir: Path, device_ids: Sequence[str]) -> None:
    device_where, device_params = device_filter_sql("a", device_ids)

    if not table_exists(conn, T_CONTAINERS):
        write_csv(data_dir / "manual_containers.csv", ["error"], [(f"Table not found: {T_CONTAINERS}",)])
        print(f"manual_containers.csv: table not found")
        return

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

    select_cols = ",\n        ".join([x for x in [a_cols, ac_cols, cargo_cols] if x])

    sql = f"""
    SELECT
        {select_cols}
    FROM {q(T_ALARMS)} a
    JOIN {q(T_CONTAINERS)} ac
        ON ac.{q(CONTAINER_DOC_ID)} = a.{q(ALARM_PK)}
    {cargo_join}
    WHERE a.{q(ALARM_TIME)} >= ? AND a.{q(ALARM_TIME)} < ?
    ORDER BY a.{q(ALARM_TIME)}, a.{q(ALARM_PK)}
    """
    count = query_to_csv(conn, sql, [dt_from, dt_to, *device_params], data_dir / "manual_containers.csv")
    print(f"manual_containers.csv: {count}")


def export_alarm_details(conn: pyodbc.Connection, dt_from: datetime, dt_to: datetime, data_dir: Path, device_ids: Sequence[str]) -> None:
    device_where, device_params = device_filter_sql("a", device_ids)

    if not table_exists(conn, T_DETAILS):
        write_csv(data_dir / "alarm_details.csv", ["error"], [(f"Table not found: {T_DETAILS}",)])
        print("alarm_details.csv: table not found")
        return

    a_cols = select_all_columns_expr(conn, T_ALARMS, "a", "alarm_")
    d_cols = select_all_columns_expr(conn, T_DETAILS, "d", "detail_")
    select_cols = ",\n        ".join([x for x in [a_cols, d_cols] if x])

    sql = f"""
    SELECT
        DATEDIFF(millisecond, a.{q(ALARM_TIME)}, d.{q(DETAIL_TIME)}) / 1000.0 AS detail_delta_seconds,
        {select_cols}
    FROM {q(T_ALARMS)} a
    JOIN {q(T_DETAILS)} d
        ON d.{q(DETAIL_DOC_ID)} = a.{q(ALARM_PK)}
    WHERE a.{q(ALARM_TIME)} >= ? AND a.{q(ALARM_TIME)} < ?
    ORDER BY a.{q(ALARM_TIME)}, a.{q(ALARM_PK)}, d.{q(DETAIL_TIME)}
    """
    count = query_to_csv(conn, sql, [dt_from, dt_to, *device_params], data_dir / "alarm_details.csv")
    print(f"alarm_details.csv: {count}")


def export_gamma_maxima(conn: pyodbc.Connection, dt_from: datetime, dt_to: datetime, data_dir: Path, device_ids: Sequence[str]) -> None:
    device_where, device_params = device_filter_sql("a", device_ids)

    if not table_exists(conn, T_DETAILS):
        write_csv(data_dir / "gamma_maxima.csv", ["error"], [(f"Table not found: {T_DETAILS}",)])
        return

    columns = set(get_columns(conn, T_DETAILS))
    required = {DETAIL_DOC_ID, DETAIL_TIME, DETAIL_GAMMA_COUNT}

    if not required.issubset(columns):
        write_csv(
            data_dir / "gamma_maxima.csv",
            ["error"],
            [(f"Missing columns in {T_DETAILS}: {sorted(required - columns)}",)],
        )
        return

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
    WHERE a.{q(ALARM_TIME)} >= ? AND a.{q(ALARM_TIME)} < ?
    ORDER BY a.{q(ALARM_PK)}, d.{q(DETAIL_TIME)}
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
        items.sort(key=lambda item: item["detail_time"])

        absolute = max(items, key=lambda item: item["gamma_count"])
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

        # Локальные максимумы.
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
        [
            "max_type",
            "alarm_pk",
            "alarm_amber_id",
            "device_id",
            "alarm_time",
            "detail_time",
            "delta_seconds",
            "gamma_count",
            "neutron_count",
        ],
        out_rows,
    )
    print(f"gamma_maxima.csv: {count}")


def export_analysis_join(data_dir: Path) -> None:
    """
    Делает удобную таблицу для дальнейшего анализа:
      photo + ручные контейнеры + ближайший абсолютный максимум GammaCount.
    Работает уже по созданным CSV, без БД.
    """
    photos_path = data_dir / "photos.csv"
    manual_path = data_dir / "manual_containers.csv"
    gamma_path = data_dir / "gamma_maxima.csv"

    if not photos_path.exists():
        return

    def read_dicts(path: Path) -> List[Dict[str, str]]:
        with path.open("r", newline="", encoding="utf-8-sig") as f:
            return [dict(row) for row in csv.DictReader(f, delimiter=";")]

    photos = read_dicts(photos_path)

    manual_by_alarm: Dict[str, List[str]] = defaultdict(list)
    if manual_path.exists():
        manual_rows = read_dicts(manual_path)
        if manual_rows:
            columns = list(manual_rows[0].keys())
            alarm_col = detect_column(columns, ["alarm_Id", "alarm_id", "alarm_pk", "alarm_container_DocId", "DocId"])
            num_col = detect_column(columns, ["cargo_FullContNumber", "FullContNumber", "ContainerNumber", "ContNumber", "Number"])

            if alarm_col and num_col:
                for row in manual_rows:
                    alarm = str(row.get(alarm_col, "")).strip()
                    num = normalize_container_number(row.get(num_col, ""))
                    if alarm and num and num not in manual_by_alarm[alarm]:
                        manual_by_alarm[alarm].append(num)

    gamma_abs_by_alarm: Dict[str, Dict[str, str]] = {}
    if gamma_path.exists():
        gamma_rows = read_dicts(gamma_path)
        for row in gamma_rows:
            if row.get("max_type") == "absolute":
                gamma_abs_by_alarm[row.get("alarm_pk", "")] = row

    out_rows: List[List[Any]] = []

    for photo in photos:
        alarm_pk = photo.get("alarm_pk", "")
        gamma = gamma_abs_by_alarm.get(alarm_pk, {})

        out_rows.append(
            [
                alarm_pk,
                photo.get("alarm_amber_id", ""),
                photo.get("device_id", ""),
                photo.get("alarm_time", ""),
                "|".join(manual_by_alarm.get(alarm_pk, [])),
                photo.get("photo_pk", ""),
                photo.get("photo_time", ""),
                photo.get("delta_seconds", ""),
                photo.get("camera_amber_id", ""),
                photo.get("frame_no", ""),
                photo.get("destination", ""),
                photo.get("copy_status", ""),
                gamma.get("detail_time", ""),
                gamma.get("delta_seconds", ""),
                gamma.get("gamma_count", ""),
                gamma.get("neutron_count", ""),
            ]
        )

    count = write_csv(
        data_dir / "analysis_join.csv",
        [
            "alarm_pk",
            "alarm_amber_id",
            "device_id",
            "alarm_time",
            "manual_container_numbers",
            "photo_pk",
            "photo_time",
            "photo_delta_seconds",
            "camera_amber_id",
            "frame_no",
            "photo_path",
            "copy_status",
            "gamma_max_time",
            "gamma_max_delta_seconds",
            "gamma_max_count",
            "neutron_count_at_gamma_max",
        ],
        out_rows,
    )
    print(f"analysis_join.csv: {count}")


def export_all_metadata(conn: pyodbc.Connection, dt_from: datetime, dt_to: datetime, out_dir: Path, limit_photos: int, device_ids: Sequence[str]) -> None:
    data_dir = out_dir / "_data"
    data_dir.mkdir(parents=True, exist_ok=True)

    print()
    print("Export CSV for next tasks...")

    export_alarms(conn, dt_from, dt_to, data_dir, device_ids)
    export_photo_db_full(conn, dt_from, dt_to, data_dir, limit_photos, device_ids)
    export_manual_containers(conn, dt_from, dt_to, data_dir, device_ids)
    export_alarm_details(conn, dt_from, dt_to, data_dir, device_ids)
    export_gamma_maxima(conn, dt_from, dt_to, data_dir, device_ids)
    export_analysis_join(data_dir)


# =============================================================================
# CLI
# =============================================================================

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export Amber photos and all CSV metadata for tasks.")

    parser.add_argument("--server", default=DB_SERVER)
    parser.add_argument("--database", default=DB_NAME)
    parser.add_argument("--driver", default=DB_DRIVER)

    parser.add_argument("--integrated-auth", dest="integrated_auth", action="store_true", default=USE_INTEGRATED_AUTH)
    parser.add_argument("--sql-auth", dest="integrated_auth", action="store_false")
    parser.add_argument("--user", default=DB_USER)
    parser.add_argument("--password", default=DB_PASSWORD)

    parser.add_argument("--from-date", default=DATE_FROM, dest="date_from")
    parser.add_argument("--to-date", default=DATE_TO, dest="date_to")

    parser.add_argument("--photo-root", default=PHOTO_ROOT)
    parser.add_argument("--out-dir", default=OUT_DIR)
    parser.add_argument("--photo-window-seconds", type=float, default=PHOTO_TIME_WINDOW_SECONDS)
    parser.add_argument("--limit-photos", type=int, default=LIMIT_PHOTOS)
    parser.add_argument("--device-ids", default=DEVICE_IDS, help="DeviceId через запятую, например: 8,10. Пусто = все.")

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

    dt_from = parse_dt(args.date_from)
    dt_to = parse_dt(args.date_to)
    device_ids = parse_device_ids(args.device_ids)

    photo_root = Path(args.photo_root)
    out_dir = Path(args.out_dir)

    print("Settings:")
    print(f"  server: {args.server}")
    print(f"  database: {args.database}")
    print(f"  period: {dt_from} -> {dt_to}")
    print(f"  photo_root: {photo_root}")
    print(f"  out_dir: {out_dir}")
    print(f"  camera_table: {T_CAMERAS}")
    print(f"  photo_window_seconds: {PHOTO_TIME_WINDOW_SECONDS}")
    print(f"  limit_photos: {args.limit_photos or 'no limit'}")
    print(f"  device_ids: {', '.join(device_ids) if device_ids else 'all'}")
    print(f"  dry_run: {args.dry_run}")
    print()

    print("Connecting to MSSQL...")
    with connect_db(args) as conn:
        print("Fetching photo rows...")
        rows = fetch_photo_rows(conn, dt_from, dt_to, args.limit_photos, device_ids)
        print(f"Rows fetched: {len(rows)}")

        copy_photos_and_make_datasets(rows, out_dir, photo_root, args.dry_run)
        export_all_metadata(conn, dt_from, dt_to, out_dir, args.limit_photos, device_ids)

    print()
    print("Done.")
    print(f"Photos: {out_dir / 'photos'}")
    print(f"Data CSV: {out_dir / '_data'}")
    print(f"Reports: {out_dir / '_reports'}")


if __name__ == "__main__":
    main()
