# list_amber_devices.py
# Скрипт без фото.
# Задача: вытащить все Янтари / устройства и понять, сколько их.
#
# Что создаёт:
#   OUT_DIR/_data/amber_devices.csv
#   OUT_DIR/_data/amber_devices_from_alarms.csv
#   OUT_DIR/_data/amber_summary.csv
#   OUT_DIR/_summary.txt
#
# Запуск:
#   pip install pyodbc
#   python list_amber_devices.py
#
# С периодом:
#   python list_amber_devices.py --from-date "2024-01-01" --to-date "2024-02-01"

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence

import pyodbc


# ===================== НАСТРОЙКИ =====================

DB_SERVER = r""          # пример: r"TOS01" или r"TOS01\INSTANCE"
DB_NAME = r""            # имя базы
DB_DRIVER = "ODBC Driver 17 for SQL Server"

USE_INTEGRATED_AUTH = True
DB_USER = r""
DB_PASSWORD = r""

OUT_DIR = r".\amber_devices_export"

# Можно оставить пустым — тогда считает за всё время.
DATE_FROM = ""           # например "2024-01-01"
DATE_TO = ""             # например "2024-02-01"

T_ALARMS = "Fac_Sea_Amber_Alarms"
T_DEVICES = "Fac_Sea_Amber_AlarmDevices"

ALARM_PK = "Id"
ALARM_DEVICE_ID = "DeviceId"
ALARM_TIME = "EventDateTime"

DEVICE_PK = "Id"
DEVICE_AMBER_ID = "AmberId"


# ===================== HELPERS =====================

def q(name: str) -> str:
    return ".".join(f"[{part.replace(']', ']]')}]" for part in str(name).split("."))


def parse_dt(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("T", " ").strip())


def csv_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return "" if value is None else value


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


def scalar(conn: pyodbc.Connection, sql: str, params: Sequence[Any] = ()) -> Any:
    cur = conn.cursor()
    cur.execute(sql, *params)
    row = cur.fetchone()
    return row[0] if row else None


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
    return bool(scalar(conn, "SELECT CASE WHEN OBJECT_ID(?) IS NULL THEN 0 ELSE 1 END", [table_name]))


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


def select_all_columns(conn: pyodbc.Connection, table: str, alias: str, prefix: str) -> str:
    cols = get_columns(conn, table)
    return ",\n        ".join(f"{alias}.{q(col)} AS {q(prefix + col)}" for col in cols)


def build_alarm_date_where(alias: str, params: List[Any], date_from: Optional[datetime], date_to: Optional[datetime]) -> str:
    parts = []

    if date_from:
        parts.append(f"{alias}.{q(ALARM_TIME)} >= ?")
        params.append(date_from)

    if date_to:
        parts.append(f"{alias}.{q(ALARM_TIME)} < ?")
        params.append(date_to)

    return " AND ".join(parts) if parts else "1=1"


# ===================== EXPORT =====================

def export_devices_from_alarms(
    conn: pyodbc.Connection,
    out_csv: Path,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
) -> int:
    """
    Устройства, которые реально встречаются в Fac_Sea_Amber_Alarms.DeviceId.
    Это самый важный файл, чтобы понять, сколько Янтарей участвует в событиях.
    """
    params: List[Any] = []
    where = build_alarm_date_where("a", params, date_from, date_to)

    sql = f"""
    SELECT
        CAST(a.{q(ALARM_DEVICE_ID)} AS nvarchar(100)) AS device_id,
        COUNT(*) AS alarms_total,
        MIN(a.{q(ALARM_TIME)}) AS first_alarm_time,
        MAX(a.{q(ALARM_TIME)}) AS last_alarm_time
    FROM {q(T_ALARMS)} a
    WHERE {where}
    GROUP BY a.{q(ALARM_DEVICE_ID)}
    ORDER BY a.{q(ALARM_DEVICE_ID)}
    """

    return query_to_csv(conn, sql, params, out_csv)


def export_devices_table_with_alarm_counts(
    conn: pyodbc.Connection,
    out_csv: Path,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
) -> int:
    """
    Полный справочник Fac_Sea_Amber_AlarmDevices + количество срабатываний.
    Если таблицы нет, создаёт CSV с сообщением.
    """
    if not table_exists(conn, T_DEVICES):
        return write_csv(out_csv, ["error"], [(f"Table not found: {T_DEVICES}",)])

    params: List[Any] = []
    alarm_where = build_alarm_date_where("a", params, date_from, date_to)

    device_cols = select_all_columns(conn, T_DEVICES, "d", "device_")

    sql = f"""
    SELECT
        {device_cols},
        COUNT(a.{q(ALARM_PK)}) AS alarms_total,
        MIN(a.{q(ALARM_TIME)}) AS first_alarm_time,
        MAX(a.{q(ALARM_TIME)}) AS last_alarm_time
    FROM {q(T_DEVICES)} d
    LEFT JOIN {q(T_ALARMS)} a
        ON a.{q(ALARM_DEVICE_ID)} = d.{q(DEVICE_PK)}
        AND {alarm_where}
    GROUP BY
        {", ".join("d." + q(c) for c in get_columns(conn, T_DEVICES))}
    ORDER BY
        d.{q(DEVICE_PK)}
    """

    return query_to_csv(conn, sql, params, out_csv)


def export_summary(
    conn: pyodbc.Connection,
    out_dir: Path,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
) -> None:
    params: List[Any] = []
    where = build_alarm_date_where("a", params, date_from, date_to)

    total_alarms = scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM {q(T_ALARMS)} a
        WHERE {where}
        """,
        params,
    )

    params2: List[Any] = []
    where2 = build_alarm_date_where("a", params2, date_from, date_to)

    devices_in_alarms = scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT a.{q(ALARM_DEVICE_ID)}
            FROM {q(T_ALARMS)} a
            WHERE {where2}
            GROUP BY a.{q(ALARM_DEVICE_ID)}
        ) x
        """,
        params2,
    )

    devices_in_table = None
    if table_exists(conn, T_DEVICES):
        devices_in_table = scalar(conn, f"SELECT COUNT(*) FROM {q(T_DEVICES)}")

    rows = [
        ["date_from", date_from if date_from else "", "Начало периода, включительно"],
        ["date_to", date_to if date_to else "", "Конец периода, не включительно"],
        ["total_alarms", total_alarms, "Всего срабатываний в Alarms"],
        ["devices_in_alarms", devices_in_alarms, "Сколько разных DeviceId встречается в Alarms"],
        ["devices_in_device_table", devices_in_table if devices_in_table is not None else "", f"Сколько строк в {T_DEVICES}"],
    ]

    write_csv(out_dir / "_data" / "amber_summary.csv", ["metric", "value", "description"], rows)

    text = f"""СПИСОК ЯНТАРЕЙ / УСТРОЙСТВ

Период:
  date_from: {date_from if date_from else "не задан"}
  date_to:   {date_to if date_to else "не задан"}

Итог:
  Всего срабатываний в Alarms: {total_alarms}
  Разных DeviceId в Alarms: {devices_in_alarms}
  Строк в {T_DEVICES}: {devices_in_table if devices_in_table is not None else "таблица не найдена"}

Файлы:
  {out_dir / "_data" / "amber_devices_from_alarms.csv"}
    Устройства, которые реально встречаются в Fac_Sea_Amber_Alarms.DeviceId.

  {out_dir / "_data" / "amber_devices.csv"}
    Справочник {T_DEVICES} + количество срабатываний.

  {out_dir / "_data" / "amber_summary.csv"}
    Короткая сводка.
"""
    (out_dir / "_summary.txt").write_text(text, encoding="utf-8")


# ===================== RUN =====================

def run(args: argparse.Namespace) -> None:
    date_from = parse_dt(args.date_from)
    date_to = parse_dt(args.date_to)

    out_dir = Path(args.out_dir)
    data_dir = out_dir / "_data"
    data_dir.mkdir(parents=True, exist_ok=True)

    print("Settings:")
    print(f"  server: {args.server}")
    print(f"  database: {args.database}")
    print(f"  date_from: {date_from if date_from else 'not set'}")
    print(f"  date_to: {date_to if date_to else 'not set'}")
    print(f"  out_dir: {out_dir}")
    print()

    with connect_db(args) as conn:
        print("Export devices from Alarms...")
        count1 = export_devices_from_alarms(conn, data_dir / "amber_devices_from_alarms.csv", date_from, date_to)
        print(f"  amber_devices_from_alarms.csv rows: {count1}")

        print("Export device table with alarm counts...")
        count2 = export_devices_table_with_alarm_counts(conn, data_dir / "amber_devices.csv", date_from, date_to)
        print(f"  amber_devices.csv rows: {count2}")

        print("Export summary...")
        export_summary(conn, out_dir, date_from, date_to)

    print()
    print("Готово.")
    print(f"Summary: {(out_dir / '_summary.txt').resolve()}")
    print(f"Data: {data_dir.resolve()}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="List all Amber devices and count alarms.")

    parser.add_argument("--server", default=DB_SERVER)
    parser.add_argument("--database", default=DB_NAME)
    parser.add_argument("--driver", default=DB_DRIVER)

    parser.add_argument("--integrated-auth", dest="integrated_auth", action="store_true", default=USE_INTEGRATED_AUTH)
    parser.add_argument("--sql-auth", dest="integrated_auth", action="store_false")
    parser.add_argument("--user", default=DB_USER)
    parser.add_argument("--password", default=DB_PASSWORD)

    parser.add_argument("--from-date", default=DATE_FROM, dest="date_from")
    parser.add_argument("--to-date", default=DATE_TO, dest="date_to")

    parser.add_argument("--out-dir", default=OUT_DIR)

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

    if missing:
        raise SystemExit("Не заполнены настройки:\n  - " + "\n  - ".join(missing))

    run(args)


if __name__ == "__main__":
    main()
