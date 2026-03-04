#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт загружает таблицу Google Sheets, сохраняет текущую версию
и сравнивает с предыдущей: отчёт по добавленным/удалённым строкам
и отдельный отчёт по изменениям в столбце "Количество ресурсов, необходимое к подбору".
"""

import csv
import io
import os
import sys
import json
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from datetime import timezone
    ZoneInfo = lambda name: timezone(timedelta(hours=3))  # fallback MSK для Python 3.8

MSK = ZoneInfo("Europe/Moscow")


def now_msk():
    """Текущее время по Москве (для единообразия в ответах и в JSON)."""
    return datetime.now(MSK).replace(tzinfo=None)  # naive datetime, но в МСК

# Корректный вывод UTF-8 в консоль Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# Для явной проверки SSL-сертификата с использованием certifi (часто помогает на macOS)
import ssl
try:
    import certifi
except ImportError:
    certifi = None

# Конфигурация (ID таблицы и листа из ссылки)
# Оригинальная ссылка на таблицу:
# https://docs.google.com/spreadsheets/d/1mEn564G6sfJvm96ff5XAyBVphDHWFzfZML2N6uhXyq4/edit?gid=1963801173#gid=1963801173
SPREADSHEET_ID = "1mEn564G6sfJvm96ff5XAyBVphDHWFzfZML2N6uhXyq4"
GID = "1963801173"
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "sheet_versions"
REPORTS_DIR = BASE_DIR / "reports"
# Только два CSV: текущая и предыдущая версия (без плодения файлов)
CURRENT_SHEET_PATH = DATA_DIR / "sheet_current.csv"
PREVIOUS_SHEET_PATH = DATA_DIR / "sheet_previous.csv"
# Время последней загрузки таблицы (обновляется при каждой успешной выгрузке)
LAST_FETCH_TIME_FILE = REPORTS_DIR / "last_fetch_time.txt"
COLUMN_RESOURCES = "Количество ресурсов, необходимое к подбору"
RETENTION_DAYS = 183  # храним данные примерно за полгода

# URL экспорта в CSV (работает, если таблица доступна по ссылке или опубликована в веб)
EXPORT_URL = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/export?format=csv&gid={GID}"


def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def cleanup_old_data():
    """Удаляет старые отчёты (report_*.txt, changes_*.json) старше RETENTION_DAYS. CSV не трогаем — храним только 2 файла."""
    cutoff = now_msk() - timedelta(days=RETENTION_DAYS)
    for directory, pattern in (
        (REPORTS_DIR, "report_*.txt"),
        (REPORTS_DIR, "changes_*.json"),
    ):
        if not directory.exists():
            continue
        for path in directory.glob(pattern):
            try:
                mtime = datetime.fromtimestamp(path.stat().st_mtime)
                if mtime < cutoff:
                    path.unlink()
            except OSError:
                continue


def fetch_sheet_csv():
    """Загружает текущую версию таблицы в виде CSV."""
    req = Request(EXPORT_URL)
    req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

    # Создаём SSL-контекст. Если доступен certifi — используем его хранилище сертификатов.
    if certifi is not None:
        context = ssl.create_default_context(cafile=certifi.where())
    else:
        context = ssl.create_default_context()
    try:
        with urlopen(req, timeout=30, context=context) as resp:
            data = resp.read()
    except HTTPError as e:
        raise SystemExit(
            f"Не удалось загрузить таблицу (HTTP {e.code}).\n"
            "Убедитесь, что доступ по ссылке включён (Настройки доступа → «Все, у кого есть ссылка»)\n"
            "или опубликуйте лист в веб (Файл → Опубликовать в интернете)."
        )
    except URLError as e:
        raise SystemExit(f"Ошибка сети: {e.reason}")
    return data.decode("utf-8-sig", errors="replace")


def parse_csv(content):
    """Парсит CSV в список строк (каждая строка — список ячеек)."""
    reader = csv.reader(content.splitlines(), quoting=csv.QUOTE_MINIMAL)
    return list(reader)


def row_key(row, headers):
    """Ключ строки для сравнения: ТК + Тип ресурса (уникально идентифицируют потребность)."""
    try:
        tk_idx = headers.index("ТК") if "ТК" in headers else 1
        res_idx = headers.index("Тип ресурса") if "Тип ресурса" in headers else 7
        tk = (row[tk_idx] if len(row) > tk_idx else "").strip()
        res = (row[res_idx] if len(row) > res_idx else "").strip()
        return (tk, res)
    except (ValueError, IndexError):
        return None


def load_previous():
    """Загружает предыдущую версию из sheet_previous.csv (если есть)."""
    ensure_dirs()
    if not PREVIOUS_SHEET_PATH.exists():
        return None
    try:
        with open(PREVIOUS_SHEET_PATH, "r", encoding="utf-8-sig", newline="") as f:
            return parse_csv(f.read())
    except OSError:
        return None


def save_current(rows):
    """Сохраняет текущую версию: бывший current → previous, новые данные → current. Всегда только 2 CSV."""
    ensure_dirs()
    if CURRENT_SHEET_PATH.exists():
        try:
            CURRENT_SHEET_PATH.replace(PREVIOUS_SHEET_PATH)
        except OSError:
            pass
    with open(CURRENT_SHEET_PATH, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)
    return CURRENT_SHEET_PATH


def _report_date_from_filename(path: Path):
    """Извлекает дату из имени файла отчёта report_YYYYmmdd_HHMMSS.txt."""
    name = path.name
    if not name.startswith("report_") or not name.endswith(".txt"):
        return None
    ts_part = name[len("report_") : -len(".txt")]
    try:
        dt = datetime.strptime(ts_part, "%Y%m%d_%H%M%S")
        return dt.date()
    except ValueError:
        return None


def list_reports_in_range(start_date, end_date):
    """
    Возвращает список путей к отчётам за указанный период (по дате в имени файла).
    Даты включительные, формат date.
    """
    ensure_dirs()
    reports = []
    for path in sorted(REPORTS_DIR.glob("report_*.txt")):
        d = _report_date_from_filename(path)
        if d is None:
            continue
        if start_date <= d <= end_date:
            reports.append(path)
    return reports


def parse_report_stats(report_path: Path):
    """
    Быстро парсит текст отчёта и возвращает словарь:
    {
      "reports": 1,
      "added": <кол-во добавленных строк>,
      "removed": <кол-во удалённых строк>,
      "resources_changes": <кол-во изменений в столбце ресурсов>,
    }
    """
    stats = {"reports": 1, "added": 0, "removed": 0, "resources_changes": 0}

    try:
        with open(report_path, "r", encoding="utf-8") as f:
            lines = [line.rstrip("\n") for line in f]
    except OSError:
        return stats

    section = None  # "added", "removed", "resources"
    for line in lines:
        if line.strip() == "--- ДОБАВЛЕННЫЕ СТРОКИ ---":
            section = "added"
            continue
        if line.strip() == "--- УДАЛЁННЫЕ СТРОКИ ---":
            section = "removed"
            continue
        if 'ОТЧЁТ ПО СТОЛБЦУ "Количество ресурсов, необходимое к подбору"' in line:
            section = "resources"
            continue

        if section == "added" and line.startswith("  ТК "):
            stats["added"] += 1
        elif section == "removed" and line.startswith("  ТК "):
            stats["removed"] += 1
        elif section == "resources" and line.startswith("  Строка: ТК "):
            stats["resources_changes"] += 1

    return stats


def list_resource_changes_in_range(start_date, end_date):
    """
    Возвращает список всех изменений в столбце ресурсов за период.
    Каждое изменение — dict с ключами:
      date, tk_type, tk, city, old, new.
    Строится на основе JSON‑файлов, сохранённых при каждом запуске /report.
    """
    ensure_dirs()
    changes = []

    for path in REPORTS_DIR.glob("changes_*.json"):
        name = path.name
        # ожидаем формат changes_YYYYmmdd_HHMMSS.json
        ts_part = name[len("changes_") : -len(".json")]
        try:
            dt = datetime.strptime(ts_part, "%Y%m%d_%H%M%S")
        except ValueError:
            continue
        d = dt.date()
        if not (start_date <= d <= end_date):
            continue

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue

        for item in data.get("resources_changes", []):
            changes.append(
                {
                    "date": d,
                    "tk_type": item.get("tk_type", ""),
                    "tk": item.get("tk", ""),
                    "city": item.get("city", ""),
                    "resource_type": item.get("resource_type", ""),
                    "old": item.get("old", ""),
                    "new": item.get("new", ""),
                }
            )

    # сортируем по дате, потом по ТК
    changes.sort(key=lambda x: (x["date"], x["tk"]))
    return changes


def has_reports_in_range(start_date, end_date):
    """
    Проверяет, есть ли хотя бы один сохранённый отчёт (changes_*.json)
    за указанный период. Нужно для понятного сообщения:
    «изменений не было» vs «отчётов за период ещё не собиралось».
    """
    ensure_dirs()
    for path in REPORTS_DIR.glob("changes_*.json"):
        name = path.name
        ts_part = name[len("changes_") : -len(".json")]
        try:
            dt = datetime.strptime(ts_part, "%Y%m%d_%H%M%S")
        except ValueError:
            continue
        d = dt.date()
        if start_date <= d <= end_date:
            return True
    return False


def get_last_update_time():
    """
    Возвращает datetime последней загрузки данных из Google Sheets.
    Берётся из файла last_fetch_time.txt (обновляется при каждой выгрузке),
    иначе — по самому новому changes_*.json.
    """
    ensure_dirs()
    if LAST_FETCH_TIME_FILE.exists():
        try:
            with open(LAST_FETCH_TIME_FILE, "r", encoding="utf-8") as f:
                line = f.read().strip()
            if line:
                return datetime.strptime(line, "%Y-%m-%d %H:%M:%S")
        except (OSError, ValueError):
            pass
    latest_dt = None
    for path in REPORTS_DIR.glob("changes_*.json"):
        name = path.name
        ts_part = name[len("changes_") : -len(".json")]
        try:
            dt = datetime.strptime(ts_part, "%Y%m%d_%H%M%S")
        except ValueError:
            continue
        if latest_dt is None or dt > latest_dt:
            latest_dt = dt
    return latest_dt


def find_column_index(headers, name):
    """Индекс столбца по имени (или по частичному совпадению)."""
    for i, h in enumerate(headers):
        if name in (h or "").strip():
            return i
    return -1


def find_resources_column_index(headers):
    """
    Индекс столбца «Количество ресурсов, необходимое к подбору».
    Сначала точное совпадение, затем по подстроке «Количество ресурсов».
    При отсутствии — возвращает -1 (вызывающий код подставит столбец по умолчанию и выведет предупреждение).
    """
    idx = find_column_index(headers, COLUMN_RESOURCES)
    if idx >= 0:
        return idx
    idx = find_column_index(headers, "Количество ресурсов")
    return idx


def build_row_map(rows, headers):
    """Словарь ключ строки -> полная строка (для быстрого поиска)."""
    key_to_row = {}
    for r in rows:
        k = row_key(r, headers)
        if k is not None:
            key_to_row[k] = r
    return key_to_row


def report_diff(current_rows, previous_rows):
    """Сравнивает две версии и формирует отчёты."""
    if not current_rows or not previous_rows:
        return None
    headers_cur = [h.strip() if h else "" for h in current_rows[0]]
    headers_prev = [h.strip() if h else "" for h in previous_rows[0]]
    data_cur = current_rows[1:]
    data_prev = previous_rows[1:]

    map_cur = build_row_map(data_cur, headers_cur)
    map_prev = build_row_map(data_prev, headers_prev)

    keys_cur = set(map_cur.keys())
    keys_prev = set(map_prev.keys())

    added_keys = keys_cur - keys_prev
    removed_keys = keys_prev - keys_cur
    common_keys = keys_cur & keys_prev

    res_col = find_resources_column_index(headers_cur)
    if res_col < 0:
        print(
            f"ВНИМАНИЕ: столбец «{COLUMN_RESOURCES}» не найден. Заголовки: {headers_cur}. Используется столбец по умолчанию (индекс 8).",
            file=sys.stderr,
        )
        res_col = 8  # столбец I по умолчанию

    # Индексы нужных для истории полей
    tk_idx = find_column_index(headers_cur, "ТК")
    tk_type_idx = find_column_index(headers_cur, "Тип ТК")
    city_idx = find_column_index(headers_cur, "Город")

    changes_resources = []
    for k in common_keys:
        row_cur = map_cur[k]
        row_prev = map_prev[k]
        v_cur = row_cur[res_col].strip() if len(row_cur) > res_col else ""
        v_prev = row_prev[res_col].strip() if len(row_prev) > res_col else ""
        if v_cur != v_prev:
            # Идентификация строки для отчёта
            tk = k[0]
            res_type = k[1]
            tk_type = (
                row_cur[tk_type_idx].strip()
                if tk_type_idx >= 0 and len(row_cur) > tk_type_idx
                else ""
            )
            city = (
                row_cur[city_idx].strip()
                if city_idx >= 0 and len(row_cur) > city_idx
                else ""
            )
            changes_resources.append(
                {
                    "tk": tk,
                    "resource_type": res_type,
                    "tk_type": tk_type,
                    "city": city,
                    "old": v_prev,
                    "new": v_cur,
                    # полный набор колонок для гибкого использования в будущем
                    "row": list(row_cur),
                }
            )

    return {
        "added": [(k, map_cur[k]) for k in sorted(added_keys)],
        "removed": [(k, map_prev[k]) for k in sorted(removed_keys)],
        "resources_changes": changes_resources,
        "headers": headers_cur,
        "res_col": res_col,
    }


def write_report(diff_result, report_path):
    """Пишет текстовый отчёт в файл и выводит в консоль."""
    lines = []
    lines.append("=" * 60)
    lines.append("ОТЧЁТ ПО ИЗМЕНЕНИЯМ ТАБЛИЦЫ")
    lines.append(now_msk().strftime("%Y-%m-%d %H:%M:%S"))
    lines.append("=" * 60)

    added = diff_result["added"]
    removed = diff_result["removed"]
    res_changes = diff_result["resources_changes"]

    lines.append("")
    lines.append("--- ДОБАВЛЕННЫЕ СТРОКИ ---")
    if not added:
        lines.append("Нет новых строк.")
    else:
        for k, row in added:
            lines.append(f"  ТК {k[0]}, Тип ресурса: {k[1]}")
            lines.append(f"    Строка: {row[:6]}")

    lines.append("")
    lines.append("--- УДАЛЁННЫЕ СТРОКИ ---")
    if not removed:
        lines.append("Нет удалённых строк.")
    else:
        for k, row in removed:
            lines.append(f"  ТК {k[0]}, Тип ресурса: {k[1]}")
            lines.append(f"    Строка: {row[:6]}")

    lines.append("")
    lines.append("=" * 60)
    lines.append('ОТЧЁТ ПО СТОЛБЦУ "Количество ресурсов, необходимое к подбору"')
    lines.append("=" * 60)
    if not res_changes:
        lines.append("Изменений в этом столбце нет.")
    else:
        for c in res_changes:
            lines.append("")
            lines.append(f"  Строка: ТК {c['tk']}, Тип ресурса: {c['resource_type']}")
            lines.append(f"    Было:  {c['old']!r}")
            lines.append(f"    Стало: {c['new']!r}")

    text = "\n".join(lines)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(text)
    # Дублирование отчёта в консоль
    print(f"\n--- Отчёт (также сохранён: {report_path}) ---\n")
    print(text)
    return text


def _write_changes_json(ts: str, resources_changes: list) -> None:
    """Пишет changes_<ts>.json; при ошибке логирует в stderr."""
    path = REPORTS_DIR / f"changes_{ts}.json"
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {"timestamp": ts, "resources_changes": resources_changes},
                f,
                ensure_ascii=False,
                indent=2,
            )
    except OSError as e:
        print(f"Ошибка записи {path}: {e}", file=sys.stderr)


def run_diff_and_get_report_path():
    """
    Выполняет полный цикл:
    - скачивает текущую версию таблицы
    - сохраняет её
    - сравнивает с предыдущей
    - сохраняет изменения по столбцу ресурсов в JSON

    Возвращает dict diff_result или None, если сравнение выполнить нельзя
    (например, это первый запуск и предыдущей версии ещё нет).
    """
    ensure_dirs()
    cleanup_old_data()
    print("Загрузка таблицы...")
    csv_content = fetch_sheet_csv()
    rows = parse_csv(csv_content)
    if not rows:
        print("Таблица пуста или не удалось распарсить CSV.")
        return None

    current_path = save_current(rows)
    print(f"Текущая версия сохранена: {current_path}")

    # Время последней загрузки (МСК) — для /last_update и единообразия
    try:
        with open(LAST_FETCH_TIME_FILE, "w", encoding="utf-8") as f:
            f.write(now_msk().strftime("%Y-%m-%d %H:%M:%S"))
    except OSError:
        pass

    previous_rows = load_previous()
    ts = now_msk().strftime("%Y%m%d_%H%M%S")

    if previous_rows is None:
        # Всё равно пишем пустой отчёт за этот запуск — чтобы /history_today показывал «изменений не было», а не «отчётов не найдено»
        _write_changes_json(ts, [])
        print("Предыдущей версии нет — это первый запуск. Отчёт по изменениям будет после следующего запуска.")
        return None

    diff_result = report_diff(rows, previous_rows)
    if diff_result is None:
        print("Не удалось сформировать сравнение.")
        return None

    # Сохраняем структурированные изменения ресурсов для истории (только JSON)
    _write_changes_json(ts, diff_result["resources_changes"])
    print(
        f"\nИзменений в столбце ресурсов: {len(diff_result['resources_changes'])}. "
        f"JSON: {REPORTS_DIR / f'changes_{ts}.json'}"
    )
    return diff_result


def main():
    diff_result = run_diff_and_get_report_path()
    if diff_result is None:
        # Нет предыдущей версии (первый запуск) — выход 0; иначе ошибка — 1
        if not PREVIOUS_SHEET_PATH.exists():
            sys.exit(0)
        sys.exit(1)


if __name__ == "__main__":
    main()
