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

from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
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
    """Клавиатура: последние данные и просмотр текущих значений столбца ресурсов."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Посмотреть последние данные", callback_data="last_data")],
        [InlineKeyboardButton("Просмотр значений", callback_data="view_values")],
    ])


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "Привет! Я бот отчётов по таблице Google Sheets.\n\n"
        "Доступные команды:\n\n"
        "/update – обновить данные\n\n"
        "/history_today – история за сегодня\n\n"
        "/history_yesterday – история за вчера\n\n"
        "/last_update – когда последний раз загружались данные\n\n"
        "/subscribe – подписаться на рассылку (пн–пт 9:30–17:30 МСК)\n\n"
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
        "/subscribe – подписаться на рассылку отчётов (пн–пт 9:30–17:30 МСК).\n"
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
    await context.bot.send_message(chat_id, "Готовлю отчёт, подождите...")

    try:
        diff_result = sheet_diff.run_diff_and_get_report_path()
    except SystemExit as e:
        await context.bot.send_message(
            chat_id,
            f"Ошибка при загрузке таблицы или формировании отчёта: {e}",
        )
        return
    except Exception as e:  # на всякий случай логируем любые другие ошибки
        logger.exception("Ошибка при формировании отчёта")
        await context.bot.send_message(chat_id, f"Не удалось сформировать отчёт: {e}")
        return

    if diff_result is None:
        await context.bot.send_message(
            chat_id,
            "Пока нечего сравнивать (скорее всего, это первый запуск). "
            "Запусти команду ещё раз позже, когда в таблице будут изменения.",
        )
        return
    text = _format_report_message(diff_result)
    await context.bot.send_message(chat_id, text, reply_markup=_main_keyboard())


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
            "Вы подписаны на рассылку. Буду присылать отчёты по изменениям в таблице "
            "в рабочие часы (пн–пт 9:30–17:30 МСК) после каждого автообновления."
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


async def auto_update_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Фоновая задача: раз в 30 минут подтягивает новую версию таблицы,
    при наличии предыдущей пишет сравнение и историю.
    В рабочие часы (пн–пт 9:30–17:30 МСК) шлёт отчёт подписчикам.
    """
    try:
        now_str = sheet_diff.now_msk().strftime("%Y-%m-%d %H:%M:%S")
        banner = f"******** АВТО-ОБНОВЛЕНИЕ КАЖДЫЕ 30 МИН — ЗАПРОС ДАННЫХ ИЗ GOOGLE SHEETS (МСК) {now_str} ********"
        print(banner)
        logger.info(banner)
        diff_result = sheet_diff.run_diff_and_get_report_path()

        # Рассылка подписчикам только в рабочие часы по МСК
        if _is_work_hours_msk() and diff_result is not None:
            subscribers = _load_subscriptions()
            if subscribers:
                text = _format_report_message(diff_result)
                for chat_id in subscribers:
                    try:
                        await context.bot.send_message(chat_id, f"📋 Рассылка (автообновление):\n\n{text}")
                    except Exception as e:
                        logger.warning("Не удалось отправить рассылку в %s: %s", chat_id, e)
    except Exception:
        logger.exception("Ошибка в автоматическом обновлении из Google Sheets")


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
    """
    today = date.today()
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

    await _send_history_for_range(chat_id, start_date, end_date, context)


async def _send_history_for_range(
    chat_id: int, start_date: date, end_date: date, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Общий помощник: отправляет историю за указанный период."""
    changes = sheet_diff.list_resource_changes_in_range(start_date, end_date)
    if not changes:
        if sheet_diff.has_reports_in_range(start_date, end_date):
            await context.bot.send_message(
                chat_id,
                f"За период {start_date} — {end_date} изменений в столбце «Количество ресурсов, необходимое к подбору» не было.",
            )
        else:
            await context.bot.send_message(
                chat_id,
                f"Отчётов за период {start_date} — {end_date} не найдено. "
                "Сделайте /update или подождите автообновления.",
            )
        return

    header = f"Период: {start_date} — {end_date}"
    lines = [header, "", "Количество ресурсов, необходимое к подбору:"]

    for ch in changes:
        tk_type = ch["tk_type"] or "-"
        tk = ch["tk"] or "-"
        city = ch["city"] or "-"
        resource_type = (ch.get("resource_type") or "").strip() or "-"
        old_raw = (ch.get("old", "") or "").strip()
        new_raw = (ch.get("new", "") or "").strip()
        old = old_raw if old_raw else "0"
        new = new_raw if new_raw else "0"
        lines.append(f"  {tk_type} - {tk} ({city}, {resource_type}) {old} → {new}")

    text_out = "\n".join(lines)
    await context.bot.send_message(chat_id, text_out)


async def history_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """История изменений за сегодня."""
    chat_id = update.effective_chat.id
    d = date.today()
    await _send_history_for_range(chat_id, d, d, context)


async def history_yesterday(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """История изменений за вчера."""
    chat_id = update.effective_chat.id
    d = date.today() - timedelta(days=1)
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


async def button_last_data(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик кнопки «Посмотреть последние данные»."""
    query = update.callback_query
    await query.answer()
    text = _format_last_update_message()
    await query.edit_message_text(text=text, reply_markup=_main_keyboard())


def _format_values_message(snapshot: list) -> str:
    """Форматирует текущие значения: Тип ТК - ТК (Город, тип ресурса) значение."""
    if not snapshot:
        return "Нет сохранённых данных. Выполните /update, затем нажмите «Просмотр значений»."
    lines = ["Количество ресурсов, необходимое к подбору (текущие значения):", ""]
    for s in snapshot:
        tk_type = s.get("tk_type") or "-"
        tk = s.get("tk") or "-"
        city = s.get("city") or "-"
        resource_type = (s.get("resource_type") or "").strip() or "-"
        val = (s.get("value") or "").strip() or "0"
        lines.append(f"  {tk_type} - {tk} ({city}, {resource_type}) {val}")
    return "\n".join(lines)


async def button_view_values(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик кнопки «Просмотр значений» — показывает текущие значения по всем строкам."""
    query = update.callback_query
    await query.answer()
    try:
        snapshot = sheet_diff.get_current_resources_snapshot()
    except Exception as e:
        logger.exception("Ошибка при получении текущих значений")
        await query.edit_message_text(
            text=f"Не удалось загрузить данные: {e}",
            reply_markup=_main_keyboard(),
        )
        return
    text = _format_values_message(snapshot)
    # Telegram лимит 4096 символов — разбиваем на части при необходимости
    max_len = 4000
    if len(text) <= max_len:
        await query.edit_message_text(text=text, reply_markup=_main_keyboard())
    else:
        lines = ["Количество ресурсов, необходимое к подбору (текущие значения):", ""]
        for s in snapshot:
            tk_type = s.get("tk_type") or "-"
            tk = s.get("tk") or "-"
            city = s.get("city") or "-"
            resource_type = (s.get("resource_type") or "").strip() or "-"
            val = (s.get("value") or "").strip() or "0"
            lines.append(f"  {tk_type} - {tk} ({city}, {resource_type}) {val}")
        parts = []
        chunk = []
        for line in lines:
            if chunk and len("\n".join(chunk)) + len(line) + 1 > max_len:
                parts.append("\n".join(chunk))
                chunk = []
            chunk.append(line)
        if chunk:
            parts.append("\n".join(chunk))
        await query.edit_message_text(text=parts[0], reply_markup=_main_keyboard())
        for p in parts[1:]:
            await context.bot.send_message(chat_id=query.message.chat_id, text=p)


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
                BotCommand("subscribe", "Подписаться на рассылку"),
                BotCommand("unsubscribe", "Отписаться от рассылки"),
                BotCommand("help", "Показать помощь по командам"),
            ]
        )
        # Сразу при запуске бота делаем один запрос к Google Sheets
        try:
            now_str = sheet_diff.now_msk().strftime("%Y-%m-%d %H:%M:%S")
            banner = (
                f"******** ПЕРВИЧНЫЙ ЗАПРОС ДАННЫХ ИЗ GOOGLE SHEETS ПРИ ЗАПУСКЕ БОТА (МСК) {now_str} ********"
            )
            print(banner)
            logger.info(banner)
            sheet_diff.run_diff_and_get_report_path()
        except Exception:
            logger.exception("Ошибка при первичном обновлении из Google Sheets")

        # Запускаем фоновую задачу автообновления каждые 30 минут,
        # если JobQueue доступен (установлен extra 'job-queue')
        jq = getattr(app, "job_queue", None)
        if jq is None:
            logger.warning(
                "JobQueue не настроен, автообновление каждые 30 минут отключено. "
                "Чтобы включить, установите: python-telegram-bot[job-queue]."
            )
        else:
            jq.run_repeating(
                auto_update_job,
                interval=30 * 60,
                first=30 * 60,
                name="auto_update_from_sheets",
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
    application.add_handler(CommandHandler("subscribe", subscribe_command))
    application.add_handler(CommandHandler("unsubscribe", unsubscribe_command))
    application.add_handler(CallbackQueryHandler(button_last_data, pattern="^last_data$"))
    application.add_handler(CallbackQueryHandler(button_view_values, pattern="^view_values$"))
    application.add_error_handler(error_handler)

    logger.info("Бот запущен. Нажми Ctrl+C для остановки.")
    application.run_polling()


if __name__ == "__main__":
    main()

