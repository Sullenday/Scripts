# amber_task3_full_analysis.py
# Задание 3 Янтарь: анализ связи распознавания номера контейнера с радиационными максимумами.
#
# Вход:
#   output_amber/_task2/recognition_results.csv
#   output_amber/_data/gamma_maxima.csv
# Если gamma_maxima.csv нет, скрипт попробует построить максимумы из:
#   output_amber/_data/alarm_details.csv
#
# Выход:
#   output_amber/_task3/task3_summary.txt
#   output_amber/_task3/photo_vs_gamma.csv
#   output_amber/_task3/correct_vs_gamma.csv
#   output_amber/_task3/best_by_alarm_vs_gamma.csv
#   output_amber/_task3/by_camera_frame_vs_gamma.csv
#   output_amber/_task3/by_abs_gamma_delta_bin.csv
#   output_amber/_task3/by_local_gamma_delta_bin.csv
#
# Запуск:
#   python amber_task3_full_analysis.py --out-dir "C:\Users\dtsygankov\output_amber"

from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


OUT_DIR = r".\output_amber"
SCRIPT_DIR = Path(__file__).resolve().parent

GAMMA_DELTA_BINS = [
    (-999999.0, -3.0, "< -3.0 sec"),
    (-3.0, -2.0, "-3.0..-2.0 sec"),
    (-2.0, -1.5, "-2.0..-1.5 sec"),
    (-1.5, -1.0, "-1.5..-1.0 sec"),
    (-1.0, -0.5, "-1.0..-0.5 sec"),
    (-0.5, 0.0, "-0.5..0.0 sec"),
    (0.0, 0.5, "0.0..0.5 sec"),
    (0.5, 1.0, "0.5..1.0 sec"),
    (1.0, 1.5, "1.0..1.5 sec"),
    (1.5, 2.0, "1.5..2.0 sec"),
    (2.0, 3.0, "2.0..3.0 sec"),
    (3.0, 999999.0, "> 3.0 sec"),
]


def read_csv_dicts(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Не найден файл: {path}")
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return [dict(row) for row in csv.DictReader(f, delimiter=";")]


def write_csv(path: Path, header: Sequence[str], rows: Iterable[Sequence[Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(list(header))
        for row in rows:
            writer.writerow(["" if value is None else value for value in row])
            count += 1
    return count


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def s(row: Optional[Dict[str, str]], key: str) -> str:
    if row is None:
        return ""
    return str(row.get(key, "") or "").strip()


def to_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    text = str(value or "").strip().replace(",", ".")
    if not text:
        return default
    try:
        return float(text)
    except Exception:
        return default


def parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for candidate in (text, text.replace("T", " ")):
        try:
            return datetime.fromisoformat(candidate)
        except Exception:
            pass
    return None


def round3(value: Optional[float]) -> str:
    return "" if value is None else str(round(value, 3))


def mean(values: List[float]) -> float:
    return round(sum(values) / len(values), 6) if values else 0.0


def median(values: List[float]) -> float:
    return round(float(statistics.median(values)), 6) if values else 0.0


def rate(numerator: float, denominator: float) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def percent(numerator: float, denominator: float) -> float:
    return round(numerator * 100.0 / denominator, 3) if denominator else 0.0


def is_correct(row: Dict[str, str]) -> bool:
    return s(row, "is_correct") == "1"


def is_recognized(row: Dict[str, str]) -> bool:
    return bool(s(row, "recognized_container_number"))


def has_manual(row: Dict[str, str]) -> bool:
    return bool(s(row, "manual_container_numbers"))


def is_wrong(row: Dict[str, str]) -> bool:
    return has_manual(row) and is_recognized(row) and s(row, "is_correct") == "0"


def confidence(row: Dict[str, str]) -> Optional[float]:
    return to_float(s(row, "confidence"), None)


def bin_label(value: Optional[float]) -> str:
    if value is None:
        return "unknown"
    for left, right, label in GAMMA_DELTA_BINS:
        if left <= value < right:
            return label
    return "unknown"


def bin_order() -> Dict[str, int]:
    result = {label: i for i, (_, _, label) in enumerate(GAMMA_DELTA_BINS)}
    result["unknown"] = len(result)
    return result


def detect_column(columns: Sequence[str], variants: Sequence[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in columns}
    for variant in variants:
        exact = lower_map.get(variant.lower())
        if exact:
            return exact
    for col in columns:
        for variant in variants:
            if variant.lower() in col.lower():
                return col
    return None


GAMMA_HEADER = [
    "max_type", "alarm_pk", "alarm_amber_id", "device_id", "alarm_time",
    "detail_time", "delta_seconds", "gamma_count", "neutron_count",
]


def build_gamma_maxima_from_alarm_details(alarm_details_csv: Path, output_csv: Path) -> Path:
    rows = read_csv_dicts(alarm_details_csv)
    if not rows:
        raise SystemExit(f"{alarm_details_csv} пустой, построить gamma_maxima невозможно.")

    columns = list(rows[0].keys())
    alarm_col = detect_column(columns, ["alarm_Id", "alarm_id", "alarm_pk", "DocId"])
    alarm_amber_col = detect_column(columns, ["alarm_AmberId", "alarm_amber_id", "AmberId"])
    device_col = detect_column(columns, ["alarm_DeviceId", "device_id", "DeviceId"])
    alarm_time_col = detect_column(columns, ["alarm_EventDateTime", "alarm_time", "EventDateTime"])
    detail_time_col = detect_column(columns, ["detail_EventDateTime", "detail_time", "EventDateTime"])
    delta_col = detect_column(columns, ["detail_delta_seconds", "delta_seconds"])
    gamma_col = detect_column(columns, ["detail_GammaCount", "GammaCount", "gamma_count"])
    neutron_col = detect_column(columns, ["detail_NeutronCount", "NeutronCount", "neutron_count"])

    if not alarm_col or not gamma_col:
        raise SystemExit(
            "Не удалось построить gamma_maxima.csv из alarm_details.csv.\n"
            f"Нужны alarm id и GammaCount. Доступные колонки: {columns}"
        )

    by_alarm: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        alarm_pk = s(row, alarm_col)
        gamma = to_float(s(row, gamma_col), None)
        if not alarm_pk or gamma is None:
            continue

        alarm_time = s(row, alarm_time_col or "")
        detail_time = s(row, detail_time_col or "")
        delta = to_float(s(row, delta_col or ""), None)
        if delta is None:
            alarm_dt = parse_dt(alarm_time)
            detail_dt = parse_dt(detail_time)
            if alarm_dt and detail_dt:
                delta = round((detail_dt - alarm_dt).total_seconds(), 3)

        by_alarm[alarm_pk].append({
            "alarm_pk": alarm_pk,
            "alarm_amber_id": s(row, alarm_amber_col or ""),
            "device_id": s(row, device_col or ""),
            "alarm_time": alarm_time,
            "detail_time": detail_time,
            "delta_seconds": delta,
            "gamma_count": gamma,
            "neutron_count": s(row, neutron_col or "") if neutron_col else "",
        })

    out_rows: List[List[Any]] = []
    for _, items in by_alarm.items():
        items.sort(key=lambda x: to_float(x.get("delta_seconds"), 999999999.0) or 999999999.0)
        absolute = max(items, key=lambda x: float(x["gamma_count"]))
        out_rows.append([
            "absolute", absolute["alarm_pk"], absolute["alarm_amber_id"], absolute["device_id"],
            absolute["alarm_time"], absolute["detail_time"], round3(to_float(absolute["delta_seconds"], None)),
            absolute["gamma_count"], absolute["neutron_count"],
        ])

        for i, item in enumerate(items):
            gamma = float(item["gamma_count"])
            prev_gamma = float(items[i - 1]["gamma_count"]) if i > 0 else None
            next_gamma = float(items[i + 1]["gamma_count"]) if i + 1 < len(items) else None
            left_ok = prev_gamma is None or gamma >= prev_gamma
            right_ok = next_gamma is None or gamma >= next_gamma
            strict = ((prev_gamma is not None and gamma > prev_gamma) or (next_gamma is not None and gamma > next_gamma))
            if left_ok and right_ok and strict:
                out_rows.append([
                    "local", item["alarm_pk"], item["alarm_amber_id"], item["device_id"],
                    item["alarm_time"], item["detail_time"], round3(to_float(item["delta_seconds"], None)),
                    item["gamma_count"], item["neutron_count"],
                ])

    write_csv(output_csv, GAMMA_HEADER, out_rows)
    return output_csv


def get_gamma_csv(data_dir: Path, task3_dir: Path, explicit_gamma_csv: str) -> Path:
    if explicit_gamma_csv:
        path = Path(explicit_gamma_csv)
        return path if path.is_absolute() else SCRIPT_DIR / path

    gamma_csv = data_dir / "gamma_maxima.csv"
    if gamma_csv.exists():
        return gamma_csv

    alarm_details_csv = data_dir / "alarm_details.csv"
    if alarm_details_csv.exists():
        built = task3_dir / "gamma_maxima_built.csv"
        print(f"gamma_maxima.csv не найден. Строю из alarm_details.csv: {built}")
        return build_gamma_maxima_from_alarm_details(alarm_details_csv, built)

    raise FileNotFoundError(
        "Не найден gamma_maxima.csv и не найден alarm_details.csv.\n"
        f"Искал:\n  {gamma_csv}\n  {alarm_details_csv}"
    )


def load_gamma_maxima(path: Path) -> Tuple[Dict[str, Dict[str, str]], Dict[str, List[Dict[str, str]]]]:
    rows = read_csv_dicts(path)
    absolute_by_alarm: Dict[str, Dict[str, str]] = {}
    local_by_alarm: Dict[str, List[Dict[str, str]]] = defaultdict(list)

    for row in rows:
        alarm_pk = s(row, "alarm_pk")
        max_type = s(row, "max_type").lower()
        if not alarm_pk:
            continue
        if max_type == "absolute":
            old = absolute_by_alarm.get(alarm_pk)
            if old is None:
                absolute_by_alarm[alarm_pk] = row
            else:
                new_gamma = to_float(s(row, "gamma_count"), -999999999.0) or -999999999.0
                old_gamma = to_float(s(old, "gamma_count"), -999999999.0) or -999999999.0
                if new_gamma > old_gamma:
                    absolute_by_alarm[alarm_pk] = row
        elif max_type == "local":
            local_by_alarm[alarm_pk].append(row)

    for alarm_pk in list(local_by_alarm.keys()):
        local_by_alarm[alarm_pk].sort(key=lambda r: to_float(s(r, "delta_seconds"), 999999999.0) or 999999999.0)
    return absolute_by_alarm, dict(local_by_alarm)


def nearest_local_gamma(alarm_pk: str, photo_delta: Optional[float], local_by_alarm: Dict[str, List[Dict[str, str]]]) -> Optional[Dict[str, str]]:
    if photo_delta is None:
        return None
    items = local_by_alarm.get(alarm_pk, [])
    if not items:
        return None
    return min(items, key=lambda row: abs(photo_delta - (to_float(s(row, "delta_seconds"), 999999999.0) or 999999999.0)))


PHOTO_GAMMA_HEADER = [
    "alarm_pk", "alarm_amber_id", "device_id", "alarm_time", "manual_container_numbers",
    "photo_pk", "photo_time", "photo_delta_seconds", "camera_amber_id", "photo_camera_id", "frame_no", "destination",
    "recognized_container_number", "is_correct", "confidence", "request_ok", "error",
    "abs_gamma_time", "abs_gamma_delta_seconds", "abs_gamma_count", "abs_neutron_count",
    "photo_minus_abs_gamma_seconds", "abs_photo_minus_abs_gamma_seconds", "photo_abs_gamma_bin",
    "nearest_local_gamma_time", "nearest_local_gamma_delta_seconds", "nearest_local_gamma_count", "nearest_local_neutron_count",
    "photo_minus_nearest_local_gamma_seconds", "abs_photo_minus_nearest_local_gamma_seconds", "photo_local_gamma_bin",
]


def build_photo_gamma_row(photo: Dict[str, str], absolute_by_alarm: Dict[str, Dict[str, str]], local_by_alarm: Dict[str, List[Dict[str, str]]]) -> List[Any]:
    alarm_pk = s(photo, "alarm_pk")
    photo_delta = to_float(s(photo, "delta_seconds"), None)

    abs_gamma = absolute_by_alarm.get(alarm_pk)
    abs_gamma_delta = to_float(s(abs_gamma, "delta_seconds"), None) if abs_gamma else None
    photo_minus_abs = photo_delta - abs_gamma_delta if photo_delta is not None and abs_gamma_delta is not None else None

    local_gamma = nearest_local_gamma(alarm_pk, photo_delta, local_by_alarm)
    local_delta = to_float(s(local_gamma, "delta_seconds"), None) if local_gamma else None
    photo_minus_local = photo_delta - local_delta if photo_delta is not None and local_delta is not None else None

    return [
        alarm_pk, s(photo, "alarm_amber_id"), s(photo, "device_id"), s(photo, "alarm_time"), s(photo, "manual_container_numbers"),
        s(photo, "photo_pk"), s(photo, "photo_time"), round3(photo_delta), s(photo, "camera_amber_id"), s(photo, "photo_camera_id"), s(photo, "frame_no"), s(photo, "destination"),
        s(photo, "recognized_container_number"), s(photo, "is_correct"), s(photo, "confidence"), s(photo, "request_ok"), s(photo, "error"),
        s(abs_gamma, "detail_time") if abs_gamma else "", round3(abs_gamma_delta), s(abs_gamma, "gamma_count") if abs_gamma else "", s(abs_gamma, "neutron_count") if abs_gamma else "",
        round3(photo_minus_abs), round3(abs(photo_minus_abs) if photo_minus_abs is not None else None), bin_label(photo_minus_abs),
        s(local_gamma, "detail_time") if local_gamma else "", round3(local_delta), s(local_gamma, "gamma_count") if local_gamma else "", s(local_gamma, "neutron_count") if local_gamma else "",
        round3(photo_minus_local), round3(abs(photo_minus_local) if photo_minus_local is not None else None), bin_label(photo_minus_local),
    ]


def row_to_dict(header: Sequence[str], row: Sequence[Any]) -> Dict[str, str]:
    return {str(header[i]): str(row[i]) if i < len(row) else "" for i in range(len(header))}


GROUP_METRIC_HEADER = [
    "total", "recognized", "manual_available", "correct", "wrong", "accuracy_vs_manual", "recognition_rate", "correct_rate_all",
    "avg_abs_photo_minus_abs_gamma_seconds", "median_abs_photo_minus_abs_gamma_seconds",
    "avg_abs_photo_minus_local_gamma_seconds", "median_abs_photo_minus_local_gamma_seconds",
    "avg_abs_to_abs_gamma_for_correct", "median_abs_to_abs_gamma_for_correct",
    "avg_abs_to_local_gamma_for_correct", "median_abs_to_local_gamma_for_correct",
]


def calc_group_metrics(rows: List[Dict[str, str]]) -> Dict[str, Any]:
    total = len(rows)
    recognized = sum(1 for row in rows if is_recognized(row))
    manual_available = sum(1 for row in rows if has_manual(row))
    correct = sum(1 for row in rows if is_correct(row))
    wrong = sum(1 for row in rows if is_wrong(row))

    abs_gamma_diffs = [to_float(s(row, "abs_photo_minus_abs_gamma_seconds"), None) for row in rows]
    abs_gamma_diffs = [x for x in abs_gamma_diffs if x is not None]
    abs_local_diffs = [to_float(s(row, "abs_photo_minus_nearest_local_gamma_seconds"), None) for row in rows]
    abs_local_diffs = [x for x in abs_local_diffs if x is not None]
    correct_abs_gamma = [to_float(s(row, "abs_photo_minus_abs_gamma_seconds"), None) for row in rows if is_correct(row)]
    correct_abs_gamma = [x for x in correct_abs_gamma if x is not None]
    correct_abs_local = [to_float(s(row, "abs_photo_minus_nearest_local_gamma_seconds"), None) for row in rows if is_correct(row)]
    correct_abs_local = [x for x in correct_abs_local if x is not None]

    return {
        "total": total,
        "recognized": recognized,
        "manual_available": manual_available,
        "correct": correct,
        "wrong": wrong,
        "accuracy_vs_manual": rate(correct, manual_available),
        "recognition_rate": rate(recognized, total),
        "correct_rate_all": rate(correct, total),
        "avg_abs_photo_minus_abs_gamma_seconds": mean(abs_gamma_diffs),
        "median_abs_photo_minus_abs_gamma_seconds": median(abs_gamma_diffs),
        "avg_abs_photo_minus_local_gamma_seconds": mean(abs_local_diffs),
        "median_abs_photo_minus_local_gamma_seconds": median(abs_local_diffs),
        "avg_abs_to_abs_gamma_for_correct": mean(correct_abs_gamma),
        "median_abs_to_abs_gamma_for_correct": median(correct_abs_gamma),
        "avg_abs_to_local_gamma_for_correct": mean(correct_abs_local),
        "median_abs_to_local_gamma_for_correct": median(correct_abs_local),
    }


def metrics_to_row(metrics: Dict[str, Any]) -> List[Any]:
    return [metrics[key] for key in GROUP_METRIC_HEADER]


def build_group_table(rows: List[Dict[str, str]], group_keys: Sequence[str]) -> Tuple[List[str], List[List[Any]]]:
    grouped: Dict[Tuple[str, ...], List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(s(row, key) for key in group_keys)].append(row)

    output: List[List[Any]] = []
    for key, items in grouped.items():
        output.append(list(key) + metrics_to_row(calc_group_metrics(items)))

    header = list(group_keys) + GROUP_METRIC_HEADER
    accuracy_index = header.index("accuracy_vs_manual")
    correct_index = header.index("correct")
    abs_correct_index = header.index("avg_abs_to_abs_gamma_for_correct")
    total_index = header.index("total")
    output.sort(key=lambda row: (-float(row[accuracy_index] or 0), float(row[abs_correct_index] or 999999999.0), -int(row[correct_index] or 0), -int(row[total_index] or 0), tuple(str(x) for x in row[:len(group_keys)])))
    return header, output


def build_bin_table(rows: List[Dict[str, str]], value_column: str, output_csv: Path) -> None:
    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[bin_label(to_float(s(row, value_column), None))].append(row)

    order = bin_order()
    output = [[label] + metrics_to_row(calc_group_metrics(items)) for label, items in grouped.items()]
    output.sort(key=lambda row: order.get(str(row[0]), 999))
    write_csv(output_csv, ["gamma_delta_bin"] + GROUP_METRIC_HEADER, output)


BEST_BY_ALARM_HEADER = [
    "alarm_pk", "alarm_amber_id", "device_id", "alarm_time", "manual_container_numbers", "alarm_status",
    "best_recognized_container_number", "best_is_correct", "best_confidence", "best_camera_amber_id", "best_photo_camera_id", "best_frame_no", "best_photo_time", "best_photo_delta_seconds", "best_destination",
    "abs_gamma_time", "abs_gamma_delta_seconds", "abs_gamma_count", "photo_minus_abs_gamma_seconds", "abs_photo_minus_abs_gamma_seconds",
    "nearest_local_gamma_time", "nearest_local_gamma_delta_seconds", "nearest_local_gamma_count", "photo_minus_nearest_local_gamma_seconds", "abs_photo_minus_nearest_local_gamma_seconds",
    "photos_total", "recognized_total", "correct_total", "wrong_total",
]


def build_best_by_alarm(photo_gamma_rows: List[Dict[str, str]]) -> List[List[Any]]:
    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in photo_gamma_rows:
        grouped[s(row, "alarm_pk")].append(row)

    result: List[List[Any]] = []
    for alarm_pk, items in grouped.items():
        correct = [row for row in items if is_correct(row)]
        recognized = [row for row in items if is_recognized(row)]
        wrong = [row for row in items if is_wrong(row)]

        if correct:
            best = sorted(correct, key=lambda row: (
                to_float(s(row, "abs_photo_minus_abs_gamma_seconds"), 999999999.0) or 999999999.0,
                to_float(s(row, "abs_photo_minus_nearest_local_gamma_seconds"), 999999999.0) or 999999999.0,
                -(confidence(row) or -1.0),
            ))[0]
            status = "has_correct_nearest_abs_gamma"
        elif recognized:
            best = sorted(recognized, key=lambda row: (-(confidence(row) or -1.0), to_float(s(row, "abs_photo_minus_abs_gamma_seconds"), 999999999.0) or 999999999.0))[0]
            status = "recognized_but_no_correct"
        else:
            best = sorted(items, key=lambda row: to_float(s(row, "abs_photo_minus_abs_gamma_seconds"), 999999999.0) or 999999999.0)[0]
            status = "no_recognition"

        result.append([
            alarm_pk, s(best, "alarm_amber_id"), s(best, "device_id"), s(best, "alarm_time"), s(best, "manual_container_numbers"), status,
            s(best, "recognized_container_number"), s(best, "is_correct"), s(best, "confidence"), s(best, "camera_amber_id"), s(best, "photo_camera_id"), s(best, "frame_no"), s(best, "photo_time"), s(best, "photo_delta_seconds"), s(best, "destination"),
            s(best, "abs_gamma_time"), s(best, "abs_gamma_delta_seconds"), s(best, "abs_gamma_count"), s(best, "photo_minus_abs_gamma_seconds"), s(best, "abs_photo_minus_abs_gamma_seconds"),
            s(best, "nearest_local_gamma_time"), s(best, "nearest_local_gamma_delta_seconds"), s(best, "nearest_local_gamma_count"), s(best, "photo_minus_nearest_local_gamma_seconds"), s(best, "abs_photo_minus_nearest_local_gamma_seconds"),
            len(items), len(recognized), len(correct), len(wrong),
        ])
    result.sort(key=lambda row: (row[5] != "has_correct_nearest_abs_gamma", str(row[0])))
    return result


def build_summary_text(recognition_count: int, gamma_abs_count: int, gamma_local_count: int, photo_gamma_rows: List[Dict[str, str]], best_by_alarm_rows: List[List[Any]], camera_frame_rows: List[List[Any]]) -> str:
    total = len(photo_gamma_rows)
    correct_rows = [row for row in photo_gamma_rows if is_correct(row)]
    recognized_rows = [row for row in photo_gamma_rows if is_recognized(row)]
    manual_rows = [row for row in photo_gamma_rows if has_manual(row)]
    wrong_rows = [row for row in photo_gamma_rows if is_wrong(row)]

    abs_diffs_correct = [to_float(s(row, "abs_photo_minus_abs_gamma_seconds"), None) for row in correct_rows]
    abs_diffs_correct = [x for x in abs_diffs_correct if x is not None]
    local_diffs_correct = [to_float(s(row, "abs_photo_minus_nearest_local_gamma_seconds"), None) for row in correct_rows]
    local_diffs_correct = [x for x in local_diffs_correct if x is not None]

    alarms_total = len(best_by_alarm_rows)
    alarms_with_correct = sum(1 for row in best_by_alarm_rows if str(row[5]) == "has_correct_nearest_abs_gamma")

    lines = []
    lines.append("АВТОМАТИЧЕСКИЙ АНАЛИЗ ЗАДАНИЯ 3")
    lines.append("=" * 80)
    lines.append("")
    lines.append("Смысл анализа:")
    lines.append("  Сопоставляем время фотографий и правильных распознаваний с максимумами GammaCount.")
    lines.append("  photo_minus_abs_gamma_seconds < 0: фото было ДО абсолютного максимума.")
    lines.append("  photo_minus_abs_gamma_seconds > 0: фото было ПОСЛЕ абсолютного максимума.")
    lines.append("")
    lines.append("Входные данные:")
    lines.append(f"  Строк распознавания из задания 2: {recognition_count}")
    lines.append(f"  Абсолютных максимумов GammaCount: {gamma_abs_count}")
    lines.append(f"  Локальных максимумов GammaCount: {gamma_local_count}")
    lines.append("")
    lines.append("Общие показатели:")
    lines.append(f"  Всего фото в анализе: {total}")
    lines.append(f"  Фото с ручным номером: {len(manual_rows)}")
    lines.append(f"  Фото с распознанным номером: {len(recognized_rows)}")
    lines.append(f"  Правильных распознаваний: {len(correct_rows)}")
    lines.append(f"  Ошибочных распознаваний: {len(wrong_rows)}")
    lines.append(f"  Срабатываний всего: {alarms_total}")
    lines.append(f"  Срабатываний с хотя бы одним правильным распознаванием: {alarms_with_correct} ({percent(alarms_with_correct, alarms_total)}%)")
    lines.append("")
    lines.append("Расстояние правильных распознаваний до радиационных максимумов:")
    lines.append(f"  Среднее abs(photo - absolute_gamma_max), сек: {mean(abs_diffs_correct)}")
    lines.append(f"  Медиана abs(photo - absolute_gamma_max), сек: {median(abs_diffs_correct)}")
    lines.append(f"  Среднее abs(photo - nearest_local_gamma_max), сек: {mean(local_diffs_correct)}")
    lines.append(f"  Медиана abs(photo - nearest_local_gamma_max), сек: {median(local_diffs_correct)}")
    lines.append("")
    lines.append("Лучшие camera/frame по точности и близости правильных распознаваний к GammaCount max:")
    if camera_frame_rows:
        for i, row in enumerate(camera_frame_rows[:10], 1):
            lines.append(f"  {i}. camera={row[0]}, frame={row[1]}, correct={row[5]}, total={row[2]}, accuracy={row[7]}, avg_abs_to_abs_gamma_correct={row[14]}, avg_abs_to_local_gamma_correct={row[16]}")
    else:
        lines.append("  Нет данных.")
    lines.append("")
    lines.append("Главные файлы:")
    lines.append("  photo_vs_gamma.csv — все фото + связь с absolute/local GammaCount максимумами.")
    lines.append("  correct_vs_gamma.csv — только правильные распознавания + расстояния до максимумов.")
    lines.append("  best_by_alarm_vs_gamma.csv — один лучший результат по каждому срабатыванию.")
    lines.append("  by_camera_frame_vs_gamma.csv — какая camera/frame лучше по точности и близости к максимуму.")
    lines.append("  by_abs_gamma_delta_bin.csv — в каком интервале относительно absolute GammaCount max лучше распознаётся номер.")
    lines.append("  by_local_gamma_delta_bin.csv — то же относительно ближайшего local GammaCount max.")
    return "\n".join(lines)


def run(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = SCRIPT_DIR / out_dir

    data_dir = out_dir / "_data"
    task2_dir = out_dir / "_task2"
    task3_dir = out_dir / "_task3"
    task3_dir.mkdir(parents=True, exist_ok=True)

    recognition_csv = Path(args.recognition_csv) if args.recognition_csv else task2_dir / "recognition_results.csv"
    if not recognition_csv.is_absolute():
        recognition_csv = SCRIPT_DIR / recognition_csv

    gamma_csv = get_gamma_csv(data_dir, task3_dir, args.gamma_csv)
    if not gamma_csv.is_absolute():
        gamma_csv = SCRIPT_DIR / gamma_csv

    recognition_rows = read_csv_dicts(recognition_csv)
    absolute_by_alarm, local_by_alarm = load_gamma_maxima(gamma_csv)

    print("Task 3 analysis settings:")
    print(f"  out_dir: {out_dir.resolve()}")
    print(f"  recognition_csv: {recognition_csv.resolve()}")
    print(f"  gamma_csv: {gamma_csv.resolve()}")
    print(f"  task3_dir: {task3_dir.resolve()}")
    print(f"  recognition rows: {len(recognition_rows)}")
    print(f"  absolute gamma alarms: {len(absolute_by_alarm)}")
    print(f"  local gamma rows: {sum(len(v) for v in local_by_alarm.values())}")
    print()

    photo_gamma_raw = [build_photo_gamma_row(photo, absolute_by_alarm, local_by_alarm) for photo in recognition_rows]
    write_csv(task3_dir / "photo_vs_gamma.csv", PHOTO_GAMMA_HEADER, photo_gamma_raw)

    photo_gamma_rows = [row_to_dict(PHOTO_GAMMA_HEADER, row) for row in photo_gamma_raw]
    correct_rows = [row for row in photo_gamma_rows if is_correct(row)]
    write_csv(task3_dir / "correct_vs_gamma.csv", PHOTO_GAMMA_HEADER, [[row.get(col, "") for col in PHOTO_GAMMA_HEADER] for row in correct_rows])

    best_by_alarm_rows = build_best_by_alarm(photo_gamma_rows)
    write_csv(task3_dir / "best_by_alarm_vs_gamma.csv", BEST_BY_ALARM_HEADER, best_by_alarm_rows)

    header, by_camera_frame = build_group_table(photo_gamma_rows, ["camera_amber_id", "frame_no"])
    write_csv(task3_dir / "by_camera_frame_vs_gamma.csv", header, by_camera_frame)
    header, by_camera = build_group_table(photo_gamma_rows, ["camera_amber_id"])
    write_csv(task3_dir / "by_camera_vs_gamma.csv", header, by_camera)
    header, by_frame = build_group_table(photo_gamma_rows, ["frame_no"])
    write_csv(task3_dir / "by_frame_vs_gamma.csv", header, by_frame)
    header, by_device = build_group_table(photo_gamma_rows, ["device_id"])
    write_csv(task3_dir / "by_device_vs_gamma.csv", header, by_device)

    build_bin_table(photo_gamma_rows, "photo_minus_abs_gamma_seconds", task3_dir / "by_abs_gamma_delta_bin.csv")
    build_bin_table(photo_gamma_rows, "photo_minus_nearest_local_gamma_seconds", task3_dir / "by_local_gamma_delta_bin.csv")

    summary = build_summary_text(
        recognition_count=len(recognition_rows),
        gamma_abs_count=len(absolute_by_alarm),
        gamma_local_count=sum(len(v) for v in local_by_alarm.values()),
        photo_gamma_rows=photo_gamma_rows,
        best_by_alarm_rows=best_by_alarm_rows,
        camera_frame_rows=by_camera_frame,
    )
    write_text(task3_dir / "task3_summary.txt", summary)

    print(summary)
    print()
    print("Готово.")
    print(f"Файлы задания 3: {task3_dir.resolve()}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Amber task 3: compare OCR recognition results with GammaCount maxima.")
    parser.add_argument("--out-dir", default=OUT_DIR)
    parser.add_argument("--recognition-csv", default="")
    parser.add_argument("--gamma-csv", default="")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    run(args)


if __name__ == "__main__":
    main()
