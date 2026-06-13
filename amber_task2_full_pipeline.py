# amber_task2_full_pipeline.py
# Задание 2 Янтарь: OCR + сбор CSV + автоматический анализ.
#
# Вход:
#   output_amber/_data/recognition_input.csv
#   output_amber/_data/manual_containers.csv
#
# Выход:
#   output_amber/_task2/recognition_results.csv
#   output_amber/_task2/task2_summary.txt
#   output_amber/_task2/overall_metrics.csv
#   output_amber/_task2/correct_recognitions.csv
#   output_amber/_task2/best_by_alarm.csv
#   output_amber/_task2/by_camera.csv
#   output_amber/_task2/by_camera_frame.csv
#   output_amber/_task2/by_frame.csv
#   output_amber/_task2/by_delta_bin.csv
#   output_amber/_task2/best_camera_frames.csv
#   output_amber/_task2/errors.csv
#   output_amber/_task2/mismatches.csv
#   output_amber/_task2/no_recognition.csv
#   output_amber/_task2/raw_responses.jsonl
#
# Запуск теста:
#   pip install requests tqdm
#   python amber_task2_full_pipeline.py --limit 100
#
# Полный запуск:
#   python amber_task2_full_pipeline.py
#
# Только анализ уже готового recognition_results.csv:
#   python amber_task2_full_pipeline.py --analyze-only

from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **kwargs):
        return x


# ===================== НАСТРОЙКИ =====================

OUT_DIR = r".\output_amber"
SCRIPT_DIR = Path(__file__).resolve().parent

API_URL = "http://127.0.0.1:8000/recognizeContainerNumber"
FILE_FIELD_NAME = "file"

LIMIT = 0                      # 0 = обработать всё
WORKERS = 1                    # сначала 1, если сервис держит — 2/4
REQUEST_TIMEOUT_SECONDS = 90
MAX_RETRIES = 2
RETRY_SLEEP_SECONDS = 1.0
SKIP_EXISTING = True
MIN_TOTAL_FOR_BEST_CAMERA_FRAME = 30
SAVE_RAW_JSON_IN_CSV = False

EXTRA_FORM_DATA: Dict[str, str] = {}

DELTA_BINS = [
    (-999999.0, -2.0, "< -2.0"),
    (-2.0, -1.5, "-2.0..-1.5"),
    (-1.5, -1.0, "-1.5..-1.0"),
    (-1.0, -0.5, "-1.0..-0.5"),
    (-0.5, 0.0, "-0.5..0.0"),
    (0.0, 0.5, "0.0..0.5"),
    (0.5, 1.0, "0.5..1.0"),
    (1.0, 1.5, "1.0..1.5"),
    (1.5, 2.0, "1.5..2.0"),
    (2.0, 999999.0, "> 2.0"),
]


# ===================== CSV =====================

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
            writer.writerow(["" if v is None else v for v in row])
            count += 1
    return count


def ensure_csv_header(path: Path, header: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f, delimiter=";").writerow(list(header))


def append_csv_rows(path: Path, rows: Iterable[Sequence[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        for row in rows:
            writer.writerow(["" if v is None else v for v in row])


def append_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            if row:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


# ===================== recognition_input.csv =====================

RECOGNITION_INPUT_HEADER = [
    "alarm_pk", "alarm_amber_id", "device_id", "alarm_time", "photo_pk", "photo_time",
    "delta_seconds", "camera_pk", "camera_amber_id", "photo_camera_id", "frame_no",
    "source", "destination", "copy_status",
]


def normalize_report_row(row: Dict[str, str], status: str) -> List[str]:
    return [
        row.get("alarm_pk", ""), row.get("alarm_amber_id", ""), row.get("device_id", ""),
        row.get("alarm_time", ""), row.get("photo_pk", ""), row.get("photo_time", ""),
        row.get("delta_seconds", ""), row.get("camera_pk", ""), row.get("camera_amber_id", ""),
        row.get("photo_camera_id", ""), row.get("frame_no", ""), row.get("source", ""),
        row.get("destination", ""), status,
    ]


def ensure_recognition_input(out_dir: Path, force_rebuild: bool = False) -> Path:
    data_dir = out_dir / "_data"
    reports_dir = out_dir / "_reports"
    input_csv = data_dir / "recognition_input.csv"
    if input_csv.exists() and not force_rebuild:
        return input_csv

    rows: List[List[str]] = []
    for path, status in [
        (reports_dir / "copied.csv", "copied"),
        (reports_dir / "already_exists.csv", "already_exists"),
        (reports_dir / "would_copy_dry_run.csv", "would_copy_dry_run"),
    ]:
        if not path.exists():
            continue
        for row in read_csv_dicts(path):
            out = normalize_report_row(row, status)
            if out[-3] or out[-2]:
                rows.append(out)

    if not rows:
        raise FileNotFoundError(
            "Не найден recognition_input.csv и не удалось собрать его из _reports.\n"
            f"Искал: {input_csv}"
        )

    write_csv(input_csv, RECOGNITION_INPUT_HEADER, rows)
    print(f"Создан {input_csv} из _reports, строк: {len(rows)}")
    return input_csv


# ===================== Нормализация и парсинг ответа OCR =====================

def s(row: Optional[Dict[str, str]], key: str) -> str:
    if row is None:
        return ""
    return str(row.get(key, "") or "").strip()


def normalize_container_number(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"[^A-Z0-9]", "", str(value).upper().strip())


def detect_column(columns: Sequence[str], variants: Sequence[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in columns}
    for v in variants:
        if v.lower() in lower_map:
            return lower_map[v.lower()]
    for c in columns:
        cl = c.lower()
        for v in variants:
            if v.lower() in cl:
                return c
    return None


def to_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    text = str(value or "").strip().replace(",", ".")
    if not text:
        return default
    try:
        return float(text)
    except Exception:
        return default


def find_container_like_strings(value: Any) -> List[str]:
    found: List[str] = []

    def add_from_string(text: str) -> None:
        norm = normalize_container_number(text)
        for m in re.finditer(r"[A-Z]{4}\d{7}", norm):
            found.append(m.group(0))
        if 8 <= len(norm) <= 14 and re.search(r"[A-Z]", norm) and re.search(r"\d", norm):
            found.append(norm)

    def walk(obj: Any) -> None:
        if obj is None:
            return
        if isinstance(obj, str):
            add_from_string(obj)
        elif isinstance(obj, dict):
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)

    walk(value)
    result, seen = [], set()
    for x in found:
        if x and x not in seen:
            seen.add(x)
            result.append(x)
    return result


def get_nested_value(obj: Any, key_names: Sequence[str]) -> Optional[Any]:
    wanted = {k.lower() for k in key_names}

    def walk(x: Any) -> Optional[Any]:
        if isinstance(x, dict):
            for k, v in x.items():
                if str(k).lower() in wanted:
                    return v
            for v in x.values():
                r = walk(v)
                if r is not None:
                    return r
        elif isinstance(x, list):
            for v in x:
                r = walk(v)
                if r is not None:
                    return r
        return None

    return walk(obj)


def parse_service_response(data: Dict[str, Any]) -> Dict[str, Any]:
    candidates = find_container_like_strings(data)
    primary = get_nested_value(
        data,
        [
            "containerNumber", "container_number", "recognizedContainerNumber",
            "recognized_container_number", "fullContainerNumber", "full_container_number",
            "normalized_candidate", "best_candidate", "candidate", "number", "value",
            "text", "label", "plate",
        ],
    )
    primary_candidates = find_container_like_strings(primary)
    recognized = primary_candidates[0] if primary_candidates else (candidates[0] if candidates else "")
    confidence = get_nested_value(data, ["confidence", "conf", "score", "probability", "best_score"])
    status = get_nested_value(data, ["status", "reason", "ocr_status", "result_status", "message"])
    checksum_valid = get_nested_value(
        data,
        ["checksum_valid", "checksumValid", "check_digit_valid", "checkDigitValid", "is_check_digit_valid", "check_digit"],
    )
    return {
        "recognized_container_number": normalize_container_number(recognized),
        "confidence": to_float(confidence, None),
        "service_status": "" if status is None else str(status),
        "checksum_valid": "" if checksum_valid is None else str(checksum_valid),
        "all_candidates": "|".join(candidates),
    }


# ===================== Ручной номер контейнера =====================

def load_manual_containers(path: Path) -> Dict[str, List[str]]:
    if not path.exists():
        print(f"WARNING: manual_containers.csv не найден: {path}")
        return {}
    rows = read_csv_dicts(path)
    if not rows:
        return {}

    cols = list(rows[0].keys())
    alarm_col = detect_column(cols, ["alarm_Id", "alarm_id", "alarm_pk", "alarm_container_DocId", "DocId"])
    number_col = detect_column(
        cols,
        ["cargo_FullContNumber", "FullContNumber", "full_cont_number", "fullContNumber", "ContNumber", "ContainerNumber", "container_number", "Number"],
    )

    if not alarm_col or not number_col:
        print("WARNING: не удалось определить колонки ручных контейнеров.")
        print(f"  alarm_col={alarm_col}, number_col={number_col}")
        print(f"  columns={cols}")
        return {}

    result: Dict[str, List[str]] = defaultdict(list)
    for row in rows:
        alarm_pk = str(row.get(alarm_col, "")).strip()
        number = normalize_container_number(row.get(number_col, ""))
        if alarm_pk and number and number not in result[alarm_pk]:
            result[alarm_pk].append(number)

    print(f"Manual containers loaded: alarms={len(result)}, alarm_col={alarm_col}, number_col={number_col}")
    return dict(result)


# ===================== OCR =====================

def recognize_one_photo(image_path: Path, args: argparse.Namespace) -> Tuple[bool, Dict[str, Any], str]:
    last_error = ""
    for attempt in range(1, args.max_retries + 2):
        try:
            with image_path.open("rb") as f:
                files = {args.file_field_name: (image_path.name, f, "image/jpeg")}
                resp = requests.post(args.api_url, files=files, data=EXTRA_FORM_DATA, timeout=args.timeout)
            try:
                data = resp.json()
            except Exception:
                data = {"raw_text": resp.text}
            if 200 <= resp.status_code < 300:
                return True, data, ""
            last_error = f"HTTP {resp.status_code}: {resp.text[:1000]}"
        except Exception as exc:
            last_error = repr(exc)
        if attempt <= args.max_retries:
            time.sleep(RETRY_SLEEP_SECONDS * attempt)
    return False, {}, last_error


RESULT_HEADER = [
    "photo_key", "alarm_pk", "alarm_amber_id", "device_id", "alarm_time", "photo_pk",
    "photo_time", "delta_seconds", "camera_pk", "camera_amber_id", "photo_camera_id",
    "frame_no", "source", "destination", "copy_status", "image_path_used",
    "manual_container_numbers", "recognized_container_number", "is_correct", "confidence",
    "service_status", "checksum_valid", "all_candidates", "request_ok", "error", "raw_json",
]


def photo_key(row: Dict[str, str]) -> str:
    return "|".join([
        row.get("alarm_pk", ""), row.get("photo_pk", ""), row.get("camera_amber_id", ""),
        row.get("photo_camera_id", ""), row.get("frame_no", ""),
        (row.get("destination") or row.get("source") or "").strip(),
    ])


def choose_image_path(row: Dict[str, str]) -> Path:
    dst = Path((row.get("destination") or "").strip())
    if dst.exists():
        return dst
    return Path((row.get("source") or "").strip())


def load_processed_keys(results_csv: Path) -> set[str]:
    if not results_csv.exists():
        return set()
    keys = set()
    with results_csv.open("r", newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f, delimiter=";"):
            key = row.get("photo_key", "")
            if key:
                keys.add(key)
    return keys


def build_result_row(
    row: Dict[str, str],
    key: str,
    image_path: Path,
    manual_by_alarm: Dict[str, List[str]],
    ok: bool,
    response_json: Dict[str, Any],
    error: str,
) -> List[Any]:
    parsed = parse_service_response(response_json) if ok else {
        "recognized_container_number": "", "confidence": None, "service_status": "", "checksum_valid": "", "all_candidates": "",
    }
    alarm_pk = str(row.get("alarm_pk", "")).strip()
    manual_numbers = manual_by_alarm.get(alarm_pk, [])
    recognized = normalize_container_number(parsed["recognized_container_number"])

    if recognized and manual_numbers:
        is_correct_value = "1" if recognized in manual_numbers else "0"
    elif recognized:
        is_correct_value = "no_manual_containers"
    else:
        is_correct_value = ""

    raw_json = json.dumps(response_json, ensure_ascii=False) if SAVE_RAW_JSON_IN_CSV and response_json else ""

    return [
        key, row.get("alarm_pk", ""), row.get("alarm_amber_id", ""), row.get("device_id", ""),
        row.get("alarm_time", ""), row.get("photo_pk", ""), row.get("photo_time", ""),
        row.get("delta_seconds", ""), row.get("camera_pk", ""), row.get("camera_amber_id", ""),
        row.get("photo_camera_id", ""), row.get("frame_no", ""), row.get("source", ""),
        row.get("destination", ""), row.get("copy_status", ""), str(image_path),
        "|".join(manual_numbers), recognized, is_correct_value,
        "" if parsed["confidence"] is None else parsed["confidence"],
        parsed["service_status"], parsed["checksum_valid"], parsed["all_candidates"],
        "1" if ok else "0", error, raw_json,
    ]


def process_one(row: Dict[str, str], manual_by_alarm: Dict[str, List[str]], args: argparse.Namespace) -> Tuple[List[Any], Dict[str, Any]]:
    key = photo_key(row)
    image_path = choose_image_path(row)
    if not image_path.exists():
        return build_result_row(row, key, image_path, manual_by_alarm, False, {}, f"image file not found: {image_path}"), {}
    ok, data, error = recognize_one_photo(image_path, args)
    result = build_result_row(row, key, image_path, manual_by_alarm, ok, data, error)
    raw = {"photo_key": key, "image_path": str(image_path), "request_ok": ok, "error": error, "response": data}
    return result, raw


def run_recognition(input_csv: Path, manual_csv: Path, task2_dir: Path, args: argparse.Namespace) -> Path:
    results_csv = task2_dir / "recognition_results.csv"
    raw_jsonl = task2_dir / "raw_responses.jsonl"

    rows = read_csv_dicts(input_csv)
    rows = [
        row for row in rows
        if (row.get("destination") or row.get("source"))
        and row.get("copy_status", "") in ("copied", "already_exists", "would_copy_dry_run", "")
    ]
    if args.limit:
        rows = rows[:args.limit]

    manual_by_alarm = load_manual_containers(manual_csv)
    processed = load_processed_keys(results_csv) if args.skip_existing else set()
    rows_to_process = [row for row in rows if photo_key(row) not in processed]
    ensure_csv_header(results_csv, RESULT_HEADER)

    print("Task 2 OCR settings:")
    print(f"  input_csv: {input_csv.resolve()}")
    print(f"  manual_csv: {manual_csv.resolve()}")
    print(f"  results_csv: {results_csv.resolve()}")
    print(f"  api_url: {args.api_url}")
    print(f"  workers: {args.workers}")
    print(f"  total input rows: {len(rows)}")
    print(f"  already processed: {len(processed)}")
    print(f"  to process: {len(rows_to_process)}")
    print()

    if not rows_to_process:
        print("Нет новых фото для OCR. Перехожу к анализу существующего CSV.")
        return results_csv

    pending_results: List[List[Any]] = []
    pending_raw: List[Dict[str, Any]] = []
    flush_every = 25

    if args.workers <= 1:
        for row in tqdm(rows_to_process, desc="Recognize", unit="photo"):
            result, raw = process_one(row, manual_by_alarm, args)
            pending_results.append(result)
            pending_raw.append(raw)
            if len(pending_results) >= flush_every:
                append_csv_rows(results_csv, pending_results)
                append_jsonl(raw_jsonl, pending_raw)
                pending_results.clear()
                pending_raw.clear()
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = [ex.submit(process_one, row, manual_by_alarm, args) for row in rows_to_process]
            for fut in tqdm(as_completed(futures), total=len(futures), desc="Recognize", unit="photo"):
                result, raw = fut.result()
                pending_results.append(result)
                pending_raw.append(raw)
                if len(pending_results) >= flush_every:
                    append_csv_rows(results_csv, pending_results)
                    append_jsonl(raw_jsonl, pending_raw)
                    pending_results.clear()
                    pending_raw.clear()

    if pending_results:
        append_csv_rows(results_csv, pending_results)
        append_jsonl(raw_jsonl, pending_raw)

    return results_csv


# ===================== Анализ =====================

def is_request_ok(row: Dict[str, str]) -> bool:
    return s(row, "request_ok") == "1"


def is_recognized(row: Dict[str, str]) -> bool:
    return bool(s(row, "recognized_container_number"))


def has_manual(row: Dict[str, str]) -> bool:
    return bool(s(row, "manual_container_numbers"))


def is_correct(row: Dict[str, str]) -> bool:
    return s(row, "is_correct") == "1"


def is_wrong(row: Dict[str, str]) -> bool:
    return has_manual(row) and is_recognized(row) and s(row, "is_correct") == "0"


def abs_delta(row: Dict[str, str]) -> float:
    v = to_float(s(row, "delta_seconds"), None)
    return 999999999.0 if v is None else abs(v)


def confidence_value(row: Dict[str, str]) -> Optional[float]:
    return to_float(s(row, "confidence"), None)


def mean(values: List[float]) -> float:
    return round(sum(values) / len(values), 6) if values else 0.0


def median(values: List[float]) -> float:
    return round(float(statistics.median(values)), 6) if values else 0.0


def rate(n: float, d: float) -> float:
    return round(n / d, 6) if d else 0.0


def percent(n: float, d: float) -> float:
    return round(n * 100.0 / d, 3) if d else 0.0


def delta_bin(value: Optional[float]) -> str:
    if value is None:
        return "unknown"
    for left, right, label in DELTA_BINS:
        if left <= value < right:
            return label
    return "unknown"


def calc_metrics(items: List[Dict[str, str]]) -> Dict[str, Any]:
    total = len(items)
    request_ok = sum(1 for r in items if is_request_ok(r))
    request_errors = sum(1 for r in items if not is_request_ok(r))
    recognized = sum(1 for r in items if is_recognized(r))
    manual_available = sum(1 for r in items if has_manual(r))
    correct = sum(1 for r in items if is_correct(r))
    wrong = sum(1 for r in items if is_wrong(r))
    no_recognition = sum(1 for r in items if not is_recognized(r))
    no_manual = sum(1 for r in items if not has_manual(r))
    deltas = [abs_delta(r) for r in items if abs_delta(r) < 999999999.0]
    correct_deltas = [abs_delta(r) for r in items if is_correct(r) and abs_delta(r) < 999999999.0]
    confidences = [confidence_value(r) for r in items if confidence_value(r) is not None]
    correct_conf = [confidence_value(r) for r in items if is_correct(r) and confidence_value(r) is not None]
    return {
        "total": total, "request_ok": request_ok, "request_errors": request_errors,
        "recognized": recognized, "manual_available": manual_available,
        "correct": correct, "wrong": wrong, "no_recognition": no_recognition,
        "no_manual": no_manual,
        "request_ok_rate": rate(request_ok, total),
        "recognition_rate": rate(recognized, total),
        "accuracy_vs_manual": rate(correct, manual_available),
        "accuracy_among_recognized_with_manual": rate(correct, correct + wrong),
        "wrong_rate_vs_manual": rate(wrong, manual_available),
        "correct_among_all": rate(correct, total),
        "request_ok_percent": percent(request_ok, total),
        "recognition_percent": percent(recognized, total),
        "accuracy_vs_manual_percent": percent(correct, manual_available),
        "wrong_vs_manual_percent": percent(wrong, manual_available),
        "avg_abs_delta_seconds": mean(deltas),
        "median_abs_delta_seconds": median(deltas),
        "avg_abs_delta_correct_seconds": mean(correct_deltas),
        "median_abs_delta_correct_seconds": median(correct_deltas),
        "avg_confidence": mean(confidences),
        "avg_confidence_correct": mean(correct_conf),
    }


METRIC_HEADER = [
    "total", "request_ok", "request_errors", "recognized", "manual_available", "correct",
    "wrong", "no_recognition", "no_manual", "request_ok_rate", "recognition_rate",
    "accuracy_vs_manual", "accuracy_among_recognized_with_manual", "wrong_rate_vs_manual",
    "correct_among_all", "request_ok_percent", "recognition_percent", "accuracy_vs_manual_percent",
    "wrong_vs_manual_percent", "avg_abs_delta_seconds", "median_abs_delta_seconds",
    "avg_abs_delta_correct_seconds", "median_abs_delta_correct_seconds", "avg_confidence",
    "avg_confidence_correct",
]


def metrics_row(prefix: Sequence[Any], m: Dict[str, Any]) -> List[Any]:
    return list(prefix) + [m[k] for k in METRIC_HEADER]


def group_rows(rows: List[Dict[str, str]], keys: Sequence[str]) -> Dict[Tuple[str, ...], List[Dict[str, str]]]:
    grouped: Dict[Tuple[str, ...], List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(s(row, k) for k in keys)].append(row)
    return dict(grouped)


def build_overall(rows: List[Dict[str, str]], path: Path) -> Dict[str, Any]:
    m = calc_metrics(rows)
    out = [
        ["total_photos", m["total"], "Всего фото"],
        ["request_ok", m["request_ok"], "Успешные запросы"],
        ["request_errors", m["request_errors"], "Ошибки запроса или отсутствующие файлы"],
        ["recognized", m["recognized"], "Фото с распознанным номером"],
        ["manual_available", m["manual_available"], "Фото с ручным номером"],
        ["correct", m["correct"], "Совпало с ручным вводом"],
        ["wrong", m["wrong"], "Не совпало с ручным вводом"],
        ["no_recognition", m["no_recognition"], "Сервис не вернул номер"],
        ["request_ok_percent", m["request_ok_percent"], "Процент успешных запросов"],
        ["recognition_percent", m["recognition_percent"], "Процент распознавания"],
        ["accuracy_vs_manual_percent", m["accuracy_vs_manual_percent"], "Процент правильных среди ручной проверки"],
        ["wrong_vs_manual_percent", m["wrong_vs_manual_percent"], "Процент ошибок среди ручной проверки"],
        ["avg_abs_delta_seconds", m["avg_abs_delta_seconds"], "Средний abs(delta_seconds)"],
        ["median_abs_delta_seconds", m["median_abs_delta_seconds"], "Медианный abs(delta_seconds)"],
        ["avg_abs_delta_correct_seconds", m["avg_abs_delta_correct_seconds"], "Средний abs(delta_seconds) у правильных"],
        ["median_abs_delta_correct_seconds", m["median_abs_delta_correct_seconds"], "Медианный abs(delta_seconds) у правильных"],
    ]
    write_csv(path, ["metric", "value", "description"], out)
    return m


def build_group_file(rows: List[Dict[str, str]], path: Path, keys: Sequence[str], min_total: int = 1) -> None:
    out = []
    for key, items in group_rows(rows, keys).items():
        m = calc_metrics(items)
        if m["total"] >= min_total:
            out.append(metrics_row(key, m))
    header = list(keys) + METRIC_HEADER
    acc_i, corr_i, total_i = header.index("accuracy_vs_manual"), header.index("correct"), header.index("total")
    out.sort(key=lambda x: (-float(x[acc_i] or 0), -int(x[corr_i] or 0), -int(x[total_i] or 0), tuple(str(v) for v in x[:len(keys)])))
    write_csv(path, header, out)


def build_correct(rows: List[Dict[str, str]], path: Path) -> None:
    out = []
    for r in rows:
        if is_correct(r):
            out.append([
                s(r, "alarm_pk"), s(r, "alarm_amber_id"), s(r, "device_id"), s(r, "alarm_time"),
                s(r, "manual_container_numbers"), s(r, "recognized_container_number"),
                s(r, "camera_amber_id"), s(r, "photo_camera_id"), s(r, "frame_no"),
                s(r, "delta_seconds"), s(r, "photo_time"), s(r, "confidence"), s(r, "destination"),
            ])
    out.sort(key=lambda x: (x[0], abs(to_float(x[9], 999999999.0) or 999999999.0)))
    write_csv(path, [
        "alarm_pk", "alarm_amber_id", "device_id", "alarm_time", "manual_container_numbers",
        "recognized_container_number", "camera_amber_id", "photo_camera_id", "frame_no",
        "delta_seconds", "photo_time", "confidence", "destination",
    ], out)


def build_best_by_alarm(rows: List[Dict[str, str]], path: Path) -> None:
    out = []
    for (alarm_pk,), items in group_rows(rows, ["alarm_pk"]).items():
        correct = [r for r in items if is_correct(r)]
        recognized = [r for r in items if is_recognized(r)]
        if correct:
            best = sorted(correct, key=lambda r: (abs_delta(r), -(confidence_value(r) or -1)))[0]
            reason = "correct_min_abs_delta"
        elif recognized:
            best = sorted(recognized, key=lambda r: (-(confidence_value(r) or -1), abs_delta(r)))[0]
            reason = "recognized_best_confidence"
        else:
            best = sorted(items, key=abs_delta)[0]
            reason = "no_recognition_min_abs_delta"
        m = calc_metrics(items)
        out.append([
            alarm_pk, s(best, "alarm_amber_id"), s(best, "device_id"), s(best, "alarm_time"),
            s(best, "manual_container_numbers"), s(best, "recognized_container_number"),
            s(best, "is_correct"), s(best, "confidence"), s(best, "camera_amber_id"),
            s(best, "photo_camera_id"), s(best, "frame_no"), s(best, "delta_seconds"),
            s(best, "photo_time"), s(best, "destination"), reason,
            m["total"], m["recognized"], m["correct"], m["wrong"], m["accuracy_vs_manual"],
        ])
    out.sort(key=lambda x: (x[14] != "correct_min_abs_delta", str(x[0])))
    write_csv(path, [
        "alarm_pk", "alarm_amber_id", "device_id", "alarm_time", "manual_container_numbers",
        "recognized_container_number", "is_correct", "confidence", "camera_amber_id", "photo_camera_id",
        "frame_no", "delta_seconds", "photo_time", "destination", "selection_reason",
        "photos_total", "recognized_total", "correct_total", "wrong_total", "accuracy_vs_manual",
    ], out)


def build_delta_bins(rows: List[Dict[str, str]], path: Path) -> None:
    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for r in rows:
        grouped[delta_bin(to_float(s(r, "delta_seconds"), None))].append(r)
    order = {label: i for i, (_, _, label) in enumerate(DELTA_BINS)}
    order["unknown"] = len(order)
    out = [metrics_row([label], calc_metrics(items)) for label, items in grouped.items()]
    out.sort(key=lambda x: order.get(str(x[0]), 999))
    write_csv(path, ["delta_bin_seconds"] + METRIC_HEADER, out)


def build_best_camera_frames(rows: List[Dict[str, str]], path: Path, min_total: int) -> None:
    out = []
    for key, items in group_rows(rows, ["camera_amber_id", "frame_no"]).items():
        m = calc_metrics(items)
        if m["total"] < min_total:
            continue
        score = m["accuracy_vs_manual"] * 1_000_000 + m["correct"] * 1000 + m["recognition_rate"] * 100 - m["avg_abs_delta_correct_seconds"]
        out.append([key[0], key[1], round(score, 6), "Рекомендовать" if m["correct"] > 0 else "Нет правильных"] + metrics_row([], m))
    header = ["camera_amber_id", "frame_no", "score", "recommendation"] + METRIC_HEADER
    out.sort(key=lambda x: (-float(x[2] or 0), str(x[0]), str(x[1])))
    write_csv(path, header, out)


def build_problem_files(rows: List[Dict[str, str]], task2_dir: Path) -> None:
    errors = [r for r in rows if not is_request_ok(r) or s(r, "error")]
    mismatches = [r for r in rows if is_wrong(r)]
    no_rec = [r for r in rows if is_request_ok(r) and not is_recognized(r)]
    header = [
        "alarm_pk", "alarm_amber_id", "device_id", "alarm_time", "photo_pk", "photo_time",
        "delta_seconds", "camera_amber_id", "photo_camera_id", "frame_no",
        "manual_container_numbers", "recognized_container_number", "confidence", "request_ok", "error", "destination",
    ]
    def row_out(r: Dict[str, str]) -> List[Any]:
        return [s(r, k) for k in header]
    write_csv(task2_dir / "errors.csv", header, [row_out(r) for r in errors])
    write_csv(task2_dir / "mismatches.csv", header, [row_out(r) for r in mismatches])
    write_csv(task2_dir / "no_recognition.csv", header, [row_out(r) for r in no_rec])


def top_rows(path: Path, n: int = 10) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    return read_csv_dicts(path)[:n]


def build_summary(task2_dir: Path, overall: Dict[str, Any], min_total: int) -> str:
    best_cf = top_rows(task2_dir / "best_camera_frames.csv", 10)
    by_cam = top_rows(task2_dir / "by_camera.csv", 10)
    by_delta = top_rows(task2_dir / "by_delta_bin.csv", 20)
    lines = []
    lines.append("АВТОМАТИЧЕСКИЙ АНАЛИЗ ЗАДАНИЯ 2")
    lines.append("=" * 80)
    lines.append("")
    lines.append("Общие показатели:")
    lines.append(f"  Всего фото: {overall['total']}")
    lines.append(f"  Успешных запросов: {overall['request_ok']} ({overall['request_ok_percent']}%)")
    lines.append(f"  Фото с распознанным номером: {overall['recognized']} ({overall['recognition_percent']}%)")
    lines.append(f"  Фото с ручным номером: {overall['manual_available']}")
    lines.append(f"  Правильных распознаваний: {overall['correct']} ({overall['accuracy_vs_manual_percent']}% от фото с ручным номером)")
    lines.append(f"  Ошибочных распознаваний: {overall['wrong']} ({overall['wrong_vs_manual_percent']}% от фото с ручным номером)")
    lines.append(f"  Без распознавания: {overall['no_recognition']}")
    lines.append("")
    lines.append("Время относительно EventDateTime:")
    lines.append(f"  Средний abs(delta_seconds): {overall['avg_abs_delta_seconds']}")
    lines.append(f"  Медианный abs(delta_seconds): {overall['median_abs_delta_seconds']}")
    lines.append(f"  Средний abs(delta_seconds) у правильных: {overall['avg_abs_delta_correct_seconds']}")
    lines.append(f"  Медианный abs(delta_seconds) у правильных: {overall['median_abs_delta_correct_seconds']}")
    lines.append("")
    lines.append(f"Лучшие camera/frame, минимум строк в группе: {min_total}")
    if best_cf:
        for i, row in enumerate(best_cf, 1):
            lines.append(
                f"  {i}. camera={row.get('camera_amber_id')}, frame={row.get('frame_no')}, "
                f"accuracy={row.get('accuracy_vs_manual_percent')}%, correct={row.get('correct')}, "
                f"total={row.get('total')}, recognition={row.get('recognition_percent')}%"
            )
    else:
        lines.append("  Нет групп под фильтр.")
    lines.append("")
    lines.append("Лучшие камеры:")
    for i, row in enumerate(by_cam[:10], 1):
        lines.append(
            f"  {i}. camera={row.get('camera_amber_id')}: accuracy={row.get('accuracy_vs_manual_percent')}%, "
            f"correct={row.get('correct')}, total={row.get('total')}, recognized={row.get('recognized')}"
        )
    lines.append("")
    lines.append("Интервалы delta_seconds:")
    for row in by_delta:
        lines.append(
            f"  {row.get('delta_bin_seconds')}: accuracy={row.get('accuracy_vs_manual_percent')}%, "
            f"correct={row.get('correct')}, total={row.get('total')}"
        )
    lines.append("")
    lines.append("Главные файлы:")
    lines.append("  recognition_results.csv — полный результат по каждому фото.")
    lines.append("  correct_recognitions.csv — главный файл: где номер распознался правильно, камера и секунда.")
    lines.append("  best_by_alarm.csv — один лучший результат на каждое срабатывание.")
    lines.append("  by_camera_frame.csv — качество по camera + frame.")
    lines.append("  by_delta_bin.csv — качество по секундам относительно EventDateTime.")
    lines.append("  mismatches.csv — распозналось, но не совпало с ручным номером.")
    lines.append("  no_recognition.csv — сервис ответил, но номер не найден.")
    lines.append("  errors.csv — ошибки запросов или отсутствующие файлы.")
    return "\n".join(lines)


def run_analysis(results_csv: Path, task2_dir: Path, min_total: int) -> None:
    rows = read_csv_dicts(results_csv)
    if not rows:
        raise SystemExit("recognition_results.csv пустой.")
    print("Analyze task 2 results...")
    print(f"  rows: {len(rows)}")
    overall = build_overall(rows, task2_dir / "overall_metrics.csv")
    build_correct(rows, task2_dir / "correct_recognitions.csv")
    build_best_by_alarm(rows, task2_dir / "best_by_alarm.csv")
    build_group_file(rows, task2_dir / "by_device.csv", ["device_id"])
    build_group_file(rows, task2_dir / "by_camera.csv", ["camera_amber_id"])
    build_group_file(rows, task2_dir / "by_frame.csv", ["frame_no"])
    build_group_file(rows, task2_dir / "by_camera_frame.csv", ["camera_amber_id", "frame_no"])
    build_delta_bins(rows, task2_dir / "by_delta_bin.csv")
    build_best_camera_frames(rows, task2_dir / "best_camera_frames.csv", min_total)
    build_problem_files(rows, task2_dir)
    summary = build_summary(task2_dir, overall, min_total)
    write_text(task2_dir / "task2_summary.txt", summary)
    print(summary)


# ===================== RUN =====================

def run(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = SCRIPT_DIR / out_dir
    data_dir = out_dir / "_data"
    task2_dir = out_dir / "_task2"
    task2_dir.mkdir(parents=True, exist_ok=True)

    if args.input_csv:
        input_csv = Path(args.input_csv)
        if not input_csv.is_absolute():
            input_csv = SCRIPT_DIR / input_csv
    else:
        input_csv = ensure_recognition_input(out_dir, args.rebuild_input)

    if args.manual_containers_csv:
        manual_csv = Path(args.manual_containers_csv)
        if not manual_csv.is_absolute():
            manual_csv = SCRIPT_DIR / manual_csv
    else:
        manual_csv = data_dir / "manual_containers.csv"

    print("Output:")
    print(f"  out_dir: {out_dir.resolve()}")
    print(f"  task2_dir: {task2_dir.resolve()}")
    print()

    if args.analyze_only:
        results_csv = Path(args.results_csv) if args.results_csv else task2_dir / "recognition_results.csv"
        if not results_csv.is_absolute():
            results_csv = SCRIPT_DIR / results_csv
    else:
        results_csv = run_recognition(input_csv, manual_csv, task2_dir, args)

    run_analysis(results_csv, task2_dir, args.min_total_for_best)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Amber task 2: OCR + automatic CSV analysis.")
    parser.add_argument("--out-dir", default=OUT_DIR)
    parser.add_argument("--input-csv", default="")
    parser.add_argument("--manual-containers-csv", default="")
    parser.add_argument("--results-csv", default="")
    parser.add_argument("--api-url", default=API_URL)
    parser.add_argument("--file-field-name", default=FILE_FIELD_NAME)
    parser.add_argument("--limit", type=int, default=LIMIT)
    parser.add_argument("--workers", type=int, default=WORKERS)
    parser.add_argument("--timeout", type=int, default=REQUEST_TIMEOUT_SECONDS)
    parser.add_argument("--max-retries", type=int, default=MAX_RETRIES)
    parser.add_argument("--skip-existing", dest="skip_existing", action="store_true", default=SKIP_EXISTING)
    parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false")
    parser.add_argument("--rebuild-input", action="store_true")
    parser.add_argument("--analyze-only", action="store_true")
    parser.add_argument("--min-total-for-best", type=int, default=MIN_TOTAL_FOR_BEST_CAMERA_FRAME)
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    run(args)


if __name__ == "__main__":
    main()
