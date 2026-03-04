#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Простой Telegram‑бот, который по команде /report запускает sheet_diff.py
и отправляет последний отчёт в чат.
"""

import json
import logging
import os
from datetime import datetime, timedelta, date, time
from pathlib import Path

from telegram import Update, BotCommand
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import Conflict

import sheet_diff

# Токен бота берём из переменной окружения (удобно для Railway и других хостингов)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

# Файл с подписками на рассылку (chat_id)
SUBSCRIPTIONS_FILE = Path(__file__).resolve().parent / "subscriptions.json"


def _load_subscriptions() -> list:
    """Список chat_id подписчиков на рассылку."""
    if not SUBSCRIPTIONS_FILE.exists():
        return []
    try:
        with open(SUBSCRIPTIONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return list(data.get("chat_ids", []))
    except (OSError, json.JSONDecodeError):
        return []


def _save_subscriptions(chat_ids: list) -> None:
    chat_ids = list(dict.fromkeys(chat_ids))  # без дубликатов
    try:
        with open(SUBSCRIPTIONS_FILE, "w", encoding="utf-8") as f:
            json.dump({"chat_ids": chat_ids}, f, ensure_ascii=False)
    except OSError:
        pass


def _add_subscription(chat_id: int) -> bool:
    ids = _load_subscriptions()
    if chat_id in ids:
        return False
    ids.append(chat_id)
    _save_subscriptions(ids)
    return True


def _remove_subscription(chat_id: int) -> bool:
    ids = _load_subscriptions()
    if chat_id not in ids:
        return False
    ids.remove(chat_id)
    _save_subscriptions(ids)
    return True


def _is_work_hours_msk() -> bool:
    """Понедельник–пятница, 9:30–17:30 по МСК (17:30 не входит)."""
    now = sheet_diff.now_msk()
    if now.weekday() >= 5:  # 5=суббота, 6=воскресенье
        return False
    t = now.time()
    start = time(9, 30)
    end = time(17, 30)
    return start <= t < end


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def _main_keyboard():
    """Клавиатура под сообщением (сейчас пустая — без кнопок)."""
    return None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "Привет! Я бот отчётов по таблице Google Sheets.\n\n"
        "Доступные команды:\n\n"
        "/update – обновить данные\n\n"
        "/history_today – история за сегодня\n\n"
        "/history_yesterday – история за вчера\n\n"
        "/last_update – когда последний раз загружались данные\n\n"
        "/since_last_request – изменения с последнего /update\n\n"
        "/subscribe – подписаться на рассылку (пн–пт 9:30–17:30 МСК каждые 30 мин)\n\n"
        "/unsubscribe – отписаться от рассылки\n\n"
        "/help – подсказка по командам"
    )
    await update.message.reply_text(text, reply_markup=_main_keyboard())


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "Команды бота:\n"
        "/update – обновить данные (то же, что /report).\n"
        "/report – сформировать свежий отчёт по изменениям в таблице и вывести строки "
        "в формате:\n"
        "  Дата: ...\n"
        "  Тип ТК - ТК (Город) было → стало\n"
        "/history [period] – история за произвольный период.\n"
        "/history_today – история за сегодня.\n"
        "/history_yesterday – история за вчера.\n"
        "/last_update – когда последний раз были получены данные из Google Sheets.\n"
        "/since_last_request – изменения в таблице с момента последнего /update (без нового /update).\n"
        "/subscribe – рассылка пн–пт 9:30–17:30 МСК каждые 30 мин (за день + с последнего запроса).\n"
        "/unsubscribe – отписаться от рассылки.\n"
        "\n"
        "/history [period] показывает строки с изменениями в столбце "
        "\"Количество ресурсов, необходимое к подбору\" за период.\n"
        "Период можно указать так:\n"
        "  today / сегодня\n"
        "  yesterday / вчера\n"
        "  week / неделя\n"
        "  month / месяц\n"
        "  YYYY-MM-DD YYYY-MM-DD (например, 2026-03-01 2026-03-10)."
    )
    await update.message.reply_text(text)


async def report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await context.bot.send_message(chat_id, "Готовлю отчёт за день, подождите...")

    try:
        diff_result, rows = sheet_diff.run_diff_for_day()
    except SystemExit as e:
        await context.bot.send_message(
            chat_id,
            f"Ошибка при загрузке таблицы или формировании отчёта: {e}",
        )
        return
    except Exception as e:
        logger.exception("Ошибка при формировании отчёта")
        await context.bot.send_message(chat_id, f"Не удалось сформировать отчёт: {e}")
        return

    if diff_result is None:
        await context.bot.send_message(
            chat_id,
            "Нет снимка на начало дня (ожидайте 00:30 МСК или повторите позже). "
            "Стартовый снимок создаётся в 00:30 каждого дня.",
        )
        return
    text = _format_report_message(diff_result)
    await context.bot.send_message(chat_id, text, reply_markup=_main_keyboard())
    # Сохраняем эти данные как «текущие» (последний запрос пользователя)
    if rows is not None:
        sheet_diff.save_last_user_request(rows)


def _format_report_message(diff_result: dict) -> str:
    """Формирует текст отчёта для отправки в чат или в рассылку."""
    changes = diff_result.get("resources_changes", [])
    added_rows = diff_result.get("added", [])
    removed_rows = diff_result.get("removed", [])
    headers = diff_result.get("headers", [])
    today_str = sheet_diff.now_msk().date().isoformat()
    lines = [f"Дата: {today_str}", ""]

    # Изменения за сегодня в столбце «Количество ресурсов, необходимое к подбору»
    lines.append("Изменения за сегодня (Количество ресурсов, необходимое к подбору):")
    if changes:
        for c in changes:
            tk_type = c.get("tk_type") or "-"
            tk = c.get("tk") or "-"
            city = c.get("city") or "-"
            resource_type = (c.get("resource_type") or "").strip() or "-"
            old_raw = (c.get("old", "") or "").strip()
            new_raw = (c.get("new", "") or "").strip()
            old = old_raw if old_raw else "0"
            new = new_raw if new_raw else "0"
            lines.append(f"  {tk_type} - {tk} ({city}, {resource_type}) {old} → {new}")
    else:
        lines.append("  Изменений за сегодня нет.")

    if added_rows or removed_rows:
        def idx(col_name, default=-1):
            try:
                return headers.index(col_name)
            except ValueError:
                return default
        tk_idx = idx("ТК")
        tk_type_idx = idx("Тип ТК")
        city_idx = idx("Город")
        res_type_idx = idx("Тип ресурса", 7)

        def fmt_row(row):
            tk = (row[tk_idx].strip() if tk_idx >= 0 and len(row) > tk_idx else "-")
            tk_type = (row[tk_type_idx].strip() if tk_type_idx >= 0 and len(row) > tk_type_idx else "-")
            city = (row[city_idx].strip() if city_idx >= 0 and len(row) > city_idx else "-")
            resource_type = (row[res_type_idx].strip() if res_type_idx >= 0 and len(row) > res_type_idx else "-")
            return f"{tk_type} - {tk} ({city}, {resource_type})"

        lines.append("")
        lines.append("=== ДОБАВЛЕННЫЕ / УДАЛЁННЫЕ СТРОКИ ===")
        if added_rows:
            lines.append("Новые строки:")
            for _, row in added_rows:
                lines.append(f"  {fmt_row(row)}")
        if removed_rows:
            lines.append("Удалённые строки:")
            for _, row in removed_rows:
                lines.append(f"  {fmt_row(row)}")

    return "\n".join(lines)


async def subscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Подписаться на рассылку (пн–пт 9:30–17:30 МСК)."""
    chat_id = update.effective_chat.id
    if _add_subscription(chat_id):
        await update.message.reply_text(
            "Вы подписаны на рассылку. Каждые 30 минут (пн–пт 9:30–17:30 МСК) приходит отчёт: "
            "за сегодня и изменения с последнего запроса."
        )
    else:
        await update.message.reply_text("Вы уже подписаны на рассылку.")


async def unsubscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отписаться от рассылки."""
    chat_id = update.effective_chat.id
    if _remove_subscription(chat_id):
        await update.message.reply_text("Вы отписаны от рассылки.")
    else:
        await update.message.reply_text("Вы не были подписаны на рассылку.")


async def scheduled_tasks_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Задачи по расписанию (МСК). Запускается каждую минуту, чтобы точно попадать в 12:00, 12:30 и т.д.:
    - 00:30–05:00: создать снимок на начало дня (повтор при ошибке каждые 10 мин)
    - 23:00–23:10: запрос таблицы и сохранение снимка на конец дня
    - 09:10 пн–пт: утренний отчёт подписчикам
    - 9:30–17:30 пн–пт в :00 и :30: рассылка (за сегодня + с последнего запроса)
    """
    try:
        now = sheet_diff.now_msk()
        today = now.date()
        yesterday = today - timedelta(days=1)
        t = now.time()

        # 00:30–05:00 МСК: стартовый снимок дня (повтор каждые 10 мин пока не получится)
        if (t.hour == 0 and t.minute >= 30) or (1 <= t.hour < 5):
            if not sheet_diff.has_start_of_day(today):
                if sheet_diff.ensure_start_of_day_snapshot():
                    logger.info("Снимок на начало дня создан: %s", today)

        # 23:00–23:10 МСК: снимок на конец дня
        if t.hour == 23 and t.minute < 10:
            if not sheet_diff.has_end_of_day(today):
                rows = sheet_diff.fetch_and_parse_safe()
                if rows:
                    sheet_diff.save_end_of_day_snapshot(rows)
                    sheet_diff.update_last_fetch_time()
                    logger.info("Снимок на конец дня сохранён: %s", today)

        # 09:10–09:20 МСК пн–пт: утренний отчёт подписчикам (изменения с последнего запроса до конца вчера)
        if t.hour == 9 and 10 <= t.minute < 20 and today.weekday() < 5:
            sent_file = sheet_diff.REPORTS_DIR / "last_morning_report.txt"
            try:
                last_sent = sent_file.read_text(encoding="utf-8").strip() if sent_file.exists() else ""
                if last_sent != today.isoformat():
                    diff_result = sheet_diff.build_morning_report(yesterday)
                    if diff_result is not None:
                        subscribers = _load_subscriptions()
                        if subscribers:
                            title = f"С момента последнего вашего запроса до конца дня {yesterday} были внесены следующие изменения:"
                            text = _format_changes_only(diff_result, title)
                            for chat_id in subscribers:
                                try:
                                    await context.bot.send_message(chat_id, f"📋 Утренний отчёт:\n\n{text}")
                                except Exception as e:
                                    logger.warning("Не удалось отправить утренний отчёт в %s: %s", chat_id, e)
                        sent_file.parent.mkdir(parents=True, exist_ok=True)
                        sent_file.write_text(today.isoformat(), encoding="utf-8")
            except Exception:
                logger.exception("Ошибка при формировании утреннего отчёта")

        # 9:30–17:30 МСК пн–пт каждые 30 мин: рассылка подписчикам (за сегодня + с последнего запроса)
        is_work_window = (t.hour > 9 or (t.hour == 9 and t.minute >= 30)) and (
            t.hour < 17 or (t.hour == 17 and t.minute <= 30)
        )
        if is_work_window and (t.minute == 0 or t.minute == 30) and today.weekday() < 5:
            subscribers = _load_subscriptions()
            if subscribers:
                try:
                    diff_day, diff_since = sheet_diff.get_diffs_for_subscription()
                    if diff_day is not None or diff_since is not None:
                        text = _format_subscription_message(diff_day, diff_since)
                        for chat_id in subscribers:
                            try:
                                await context.bot.send_message(chat_id, f"📋 Рассылка:\n\n{text}")
                            except Exception as e:
                                logger.warning("Не удалось отправить рассылку в %s: %s", chat_id, e)
                except Exception:
                    logger.exception("Ошибка при формировании рассылки 9:30–17:30")
    except Exception:
        logger.exception("Ошибка в задаче по расписанию")


def _parse_history_range(args):
    """
    Разбор аргументов команды /history.
    Поддерживается:
      /history
      /history today
      /history yesterday
      /history week
      /history month
      /history YYYY-MM-DD YYYY-MM-DD
    Возвращает (start_date, end_date) или (None, None) при ошибке.
    Для относительных периодов (today, yesterday, week, month) используется дата по МСК.
    """
    today = sheet_diff.now_msk().date()
    if not args:
        return today, today

    key = args[0].lower()
    if key in ("today", "сегодня"):
        return today, today
    if key in ("yesterday", "вчера"):
        d = today - timedelta(days=1)
        return d, d
    if key in ("week", "неделя", "7d"):
        start = today - timedelta(days=7)
        return start, today
    if key in ("month", "месяц", "30d"):
        start = today - timedelta(days=30)
        return start, today

    if len(args) >= 2:
        try:
            start = datetime.strptime(args[0], "%Y-%m-%d").date()
            end = datetime.strptime(args[1], "%Y-%m-%d").date()
        except ValueError:
            return None, None
        if end < start:
            start, end = end, start
        return start, end

    return None, None


async def history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Показывает список строк, где менялось
    \"Количество ресурсов, необходимое к подбору\" за период.

    Формат строки:
      Дата — Тип ТК / ТК / Город / было → стало

    Примеры:
      /history
      /history today
      /history yesterday
      /history week
      /history month
      /history 2026-02-01 2026-02-15
    """
    chat_id = update.effective_chat.id
    text = update.message.text or ""
    parts = text.split()
    args = parts[1:]

    start_date, end_date = _parse_history_range(args)
    if start_date is None:
        await context.bot.send_message(
            chat_id,
            "Не понял период.\n"
            "Используй: /history [today|yesterday|week|month] или /history YYYY-MM-DD YYYY-MM-DD",
        )
        return

    # «За сегодня»: снимок начала дня (JSON) + запрос в Google, выводим изменения по маске
    today_msk = sheet_diff.now_msk().date()
    if start_date == end_date and start_date == today_msk:
        await _send_history_today(chat_id, context)
        return
    await _send_history_for_range(chat_id, start_date, end_date, context)


async def _send_history_for_range(
    chat_id: int, start_date: date, end_date: date, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    История за период: JSON начала первой даты + JSON конца последней даты, diff по маске.
    Один день — get_diff_for_day, несколько дней — get_diff_for_range.
    """
    if start_date == end_date:
        diff_result = sheet_diff.get_diff_for_day(start_date)
    else:
        diff_result = sheet_diff.get_diff_for_range(start_date, end_date)

    if diff_result is None:
        await context.bot.send_message(
            chat_id,
            f"Нет снимков на начало и/или конец дня за период {start_date} — {end_date}. "
            "Снимки создаются в 00:30 (начало дня) и 23:00 (конец дня) МСК.",
        )
        return

    title = f"Период: {start_date} — {end_date}. Количество ресурсов, необходимое к подбору:"
    text = _format_changes_only(diff_result, title)
    await context.bot.send_message(chat_id, text)


async def _send_history_today(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    """История за сегодня: JSON начала дня + запрос в Google, выводим строки что изменились по маске."""
    await context.bot.send_message(chat_id, "Загружаю таблицу и сравниваю с началом дня...")
    try:
        diff_result = sheet_diff.get_diff_for_today()
    except Exception as e:
        logger.exception("Ошибка при истории за сегодня")
        await context.bot.send_message(chat_id, f"Не удалось загрузить данные: {e}")
        return
    if diff_result is None:
        await context.bot.send_message(
            chat_id,
            "Нет снимка на начало дня или не удалось загрузить таблицу. "
            "Снимок создаётся в 00:30 МСК или при первом /update за день.",
        )
        return
    today_str = sheet_diff.now_msk().date().isoformat()
    title = f"За сегодня ({today_str}). Количество ресурсов, необходимое к подбору:"
    text = _format_changes_only(diff_result, title)
    await context.bot.send_message(chat_id, text)


async def history_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """История за сегодня: снимок начала дня (JSON) + запрос в Google, выводим что изменилось по маске."""
    chat_id = update.effective_chat.id
    await _send_history_today(chat_id, context)


async def history_yesterday(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """История изменений за вчера (дата по МСК)."""
    chat_id = update.effective_chat.id
    d = sheet_diff.now_msk().date() - timedelta(days=1)
    await _send_history_for_range(chat_id, d, d, context)


def _format_last_update_message():
    """Формирует текст о последнем обновлении данных (для команды и для кнопки)."""
    dt = sheet_diff.get_last_update_time()
    if dt is None:
        return "Данные из Google Sheets ещё ни разу не загружались (нет сохранённых отчётов)."
    return f"Последнее обновление данных из Google Sheets (МСК): {dt.strftime('%Y-%m-%d %H:%M:%S')}"


async def last_update(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает, когда в последний раз были получены данные из Google Sheets."""
    chat_id = update.effective_chat.id
    text = _format_last_update_message()
    await context.bot.send_message(chat_id, text)


def _format_changes_only(diff_result: dict, title: str) -> str:
    """Форматирует только блок изменений ресурсов (для «с последнего запроса» и утреннего отчёта)."""
    changes = diff_result.get("resources_changes", [])
    lines = [title, ""]
    if not changes:
        lines.append("  Изменений нет.")
    else:
        for c in changes:
            tk_type = c.get("tk_type") or "-"
            tk = c.get("tk") or "-"
            city = c.get("city") or "-"
            resource_type = (c.get("resource_type") or "").strip() or "-"
            old = (c.get("old", "") or "").strip() or "0"
            new = (c.get("new", "") or "").strip() or "0"
            lines.append(f"  {tk_type} - {tk} ({city}, {resource_type}) {old} → {new}")
    return "\n".join(lines)


def _format_subscription_message(diff_day, diff_since) -> str:
    """
    Формат рассылки 9:30–17:30: блок «За сегодня» и блок «Изменения с последнего запроса».
    Пример:
      За сегодня:
        ГМ - 177 (Ижевск, Авто курьер) 6 → 4
        ПВЗ - 42 (Москва, Пеший курьер) 2 → 3

      ——————————————
      Изменения с последнего запроса:
        ПВЗ - 42 (Москва, Пеший курьер) 2 → 3
    """
    lines = ["За сегодня:", ""]
    if diff_day and diff_day.get("resources_changes"):
        for c in diff_day["resources_changes"]:
            tk_type = c.get("tk_type") or "-"
            tk = c.get("tk") or "-"
            city = c.get("city") or "-"
            resource_type = (c.get("resource_type") or "").strip() or "-"
            old = (c.get("old", "") or "").strip() or "0"
            new = (c.get("new", "") or "").strip() or "0"
            lines.append(f"  {tk_type} - {tk} ({city}, {resource_type}) {old} → {new}")
    else:
        lines.append("  Изменений нет.")
    lines.append("")
    lines.append("——————————————")
    lines.append("Изменения с последнего запроса:")
    lines.append("")
    if diff_since and diff_since.get("resources_changes"):
        for c in diff_since["resources_changes"]:
            tk_type = c.get("tk_type") or "-"
            tk = c.get("tk") or "-"
            city = c.get("city") or "-"
            resource_type = (c.get("resource_type") or "").strip() or "-"
            old = (c.get("old", "") or "").strip() or "0"
            new = (c.get("new", "") or "").strip() or "0"
            lines.append(f"  {tk_type} - {tk} ({city}, {resource_type}) {old} → {new}")
    else:
        lines.append("  Изменений нет.")
    return "\n".join(lines)


async def since_last_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /since_last_request: сравнение текущей таблицы с последним /update (без обновления снимка)."""
    chat_id = update.effective_chat.id
    await context.bot.send_message(chat_id, "Сравниваю с последним запросом...")
    try:
        diff_result = sheet_diff.run_diff_since_last_request()
    except Exception as e:
        logger.exception("Ошибка при сравнении с последним запросом")
        await context.bot.send_message(chat_id, f"Не удалось загрузить данные: {e}")
        return
    if diff_result is None:
        await context.bot.send_message(
            chat_id,
            "Нет сохранённых данных последнего запроса. Сделайте /update, затем используйте /since_last_request.",
        )
        return
    _, ts = sheet_diff.load_last_user_request()
    title = f"Изменения с последнего запроса ({ts or '—'}):"
    text = _format_changes_only(diff_result, title)
    await context.bot.send_message(chat_id, text)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Глобальный обработчик ошибок бота.
    Глушим Conflict (двойной запуск бота) и логируем остальные ошибки.
    """
    err = context.error
    if isinstance(err, Conflict):
        logger.warning(
            "Получен Conflict от Telegram (возможно, запущен второй экземпляр бота). "
            "Ошибка проигнорирована, но убедитесь, что запущен только один процесс бота."
        )
        return

    logger.exception("Необработанная ошибка в боте", exc_info=err)


def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise SystemExit(
            "Не задан TELEGRAM_BOT_TOKEN. На Railway добавь переменную окружения "
            "TELEGRAM_BOT_TOKEN с токеном бота от BotFather."
        )

    async def post_init(app: Application) -> None:
        # Настраиваем меню команд в Telegram‑клиенте
        await app.bot.set_my_commands(
            [
                BotCommand("start", "Начать работу с ботом"),
                BotCommand("update", "Обновить: скачать новые данные и показать свежие изменения"),
                BotCommand("history_today", "История за сегодня"),
                BotCommand("history_yesterday", "История за вчера"),
                BotCommand("last_update", "Когда последний раз обновлялись данные"),
                BotCommand("since_last_request", "Изменения с последнего /update"),
                BotCommand("subscribe", "Подписаться на рассылку"),
                BotCommand("unsubscribe", "Отписаться от рассылки"),
                BotCommand("help", "Показать помощь по командам"),
            ]
        )
        # Задачи по расписанию: каждую минуту (чтобы точно попадать в 12:00, 12:30 и т.д.)
        jq = getattr(app, "job_queue", None)
        if jq is None:
            logger.warning(
                "JobQueue не настроен, расписание отключено. "
                "Установите: python-telegram-bot[job-queue]."
            )
        else:
            jq.run_repeating(
                scheduled_tasks_job,
                interval=60,
                first=60,
                name="scheduled_tasks",
            )

    application = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    # Обновить (обёртка вокруг report)
    application.add_handler(CommandHandler("update", report))
    application.add_handler(CommandHandler("report", report))
    # История: произвольный период / сегодня / вчера
    application.add_handler(CommandHandler("history", history))
    application.add_handler(CommandHandler("history_today", history_today))
    application.add_handler(CommandHandler("history_yesterday", history_yesterday))
    application.add_handler(CommandHandler("last_update", last_update))
    application.add_handler(CommandHandler("since_last_request", since_last_request))
    application.add_handler(CommandHandler("subscribe", subscribe_command))
    application.add_handler(CommandHandler("unsubscribe", unsubscribe_command))
    application.add_error_handler(error_handler)

    logger.info("Бот запущен. Нажми Ctrl+C для остановки.")
    application.run_polling()


if __name__ == "__main__":
    main()

