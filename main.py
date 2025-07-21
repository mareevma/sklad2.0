# main.py — Telegram‑бот‑склад v0.9.4
"""Telegram‑бот для учёта склада (SQLite + OpenAI).

v0.9.4
======
* ✨ Реализованы читаемые логи операций в tx_log (запрашиваются у LLM).
* 🪵 Добавлена команда /logs для просмотра последних операций.
* ⚡️ Оптимизировано управление подключением к БД (одно на всё приложение).
* 🛡️ Улучшена логика записи логов в одной транзакции с операцией.
* 🔒 Anti‑DROP / Anti‑mass‑DELETE защита в validate_sql().
* ➖ Нельзя списать больше, чем есть: правило 2‑b в SYSTEM_PROMPT_SQL.
* «Нет размера» отображается как «-».
"""

from __future__ import annotations

import os, re, json, logging, asyncio, csv, io, httpx
from typing import List, Dict, Any

import aiosqlite
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, types
from aiogram.filters import Command
from aiogram.types import BotCommand
from openai import AsyncOpenAI, OpenAIError

# ─────────────────────────────────────────────────────────────────────────────
# 1. Конфигурация
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv(".env")
BOT_TOKEN  = os.getenv("BOT_TOKEN")
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
DB_PATH    = os.getenv("DATABASE_PATH", "warehouse_v4.db")
MODEL      = os.getenv("OPENAI_MODEL_NAME", "gpt-4o-mini")
LOG_LEVEL  = os.getenv("LOG_LEVEL", "INFO").upper()
PROXY = os.getenv("PROXY")

logging.basicConfig(level=LOG_LEVEL,
                    format="%(asctime)s - %(levelname)s - %(message)s")
if not BOT_TOKEN or not OPENAI_KEY:
    raise ValueError("Необходимо задать BOT_TOKEN и OPENAI_API_KEY в .env")

ai = AsyncOpenAI(
    api_key=OPENAI_KEY,
    http_client=httpx.AsyncClient(
        proxy=PROXY,
    )
)

bot    = Bot(token=BOT_TOKEN)
router = Router()

# ─────────────────────────────────────────────────────────────────────────────
# 2. База данных
# ─────────────────────────────────────────────────────────────────────────────
ALLOWED_SIZES = ("XS","S","M","L","XL","XXL","XXXL")
LOCATION_ROWS = "АБВГДЕ"
LOCATION_COLS = list(range(1, 11))

SCHEMA = f"""
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS items (
  id   INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT  COLLATE NOCASE NOT NULL,
  size TEXT  COLLATE NOCASE CHECK(size IS NULL OR size IN {ALLOWED_SIZES}),
  UNIQUE(name, size)
);
CREATE TABLE IF NOT EXISTS locations (
  code TEXT COLLATE NOCASE PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS stock (
  item_id       INTEGER,
  location_code TEXT COLLATE NOCASE,
  qty           INTEGER CHECK(qty > 0),
  PRIMARY KEY(item_id, location_code),
  FOREIGN KEY(item_id)       REFERENCES items(id)       ON DELETE CASCADE,
  FOREIGN KEY(location_code) REFERENCES locations(code) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS tx_log (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  ts       DATETIME DEFAULT CURRENT_TIMESTAMP,
  user     TEXT,
  sql_text TEXT,
  summary  TEXT,
  success  INTEGER
);
"""

async def init_db(db: aiosqlite.Connection) -> None:
    await db.executescript(SCHEMA)
    locs = [(f"{r}{c}",) for r in LOCATION_ROWS for c in LOCATION_COLS]
    await db.executemany("INSERT OR IGNORE INTO locations(code) VALUES (?)", locs)
    await db.commit()
    logging.info("БД '%s' готова.", DB_PATH)

# ─────────────────────────────────────────────────────────────────────────────
# 3. Системный промпт (⭐ ИЗМЕНЕНО)
# ─────────────────────────────────────────────────────────────────────────────
SYSTEM_PROMPT_SQL = f"""
Ты — SQL‑ассистент склада. Схема: items(id,name,size), locations(code),
stock(item_id,location_code,qty).

Правила:
1. **Добавление**:
   • `INSERT OR IGNORE INTO items(name,size) VALUES …`
   • `INSERT INTO stock(item_id,location_code,qty) VALUES …
      ON CONFLICT(item_id,location_code) DO UPDATE
      SET qty = qty + excluded.qty`

2. **Вычитание / перемещение**:
   a) Перед изменением возьми текущий `qty`.
   b) **Если запрошено больше, чем есть — верни JSON с
      `"error":"Недостаточно товара"` и не выполняй SQL.**
   c) Когда qty станет 0 — используй `DELETE`, иначе `UPDATE`.

3. **Перемещение**:
   • сначала ↑qty в ячейку‑назначение;
   • затем ↓qty из источника по п. 2.

4. **Уникальность**: если товар+размер встречается ровно в одной ячейке —
   считай её исходной.

5. **Чтение**: если запрос информационный («где», «покажи»…) —
   только `SELECT`, mode="read".

6. **Формат**: JSON `{{"sql":"…", "mode":"read"|"write", "summary":"…"}}`.
   - **summary**: Краткое описание действия для лога. Например: "добавлено 5 маек L в А1", "перемещено 3 болта из Б2 в В3", "удалено 10 кепок из Г7".
   - Для 'read' mode summary может быть пустым.

7. **Нормализация ячеек**: формат `А1…Е10`, верхний регистр, кириллица.

8. **Размеры**: только {', '.join(ALLOWED_SIZES)}. Для прочих товаров size=NULL.

9. **JOIN**: всегда
   `FROM stock JOIN items ON items.id = stock.item_id`
   и выбирай `items.name AS name`, `items.size`, `stock.location_code`, `stock.qty`.
   **Никогда не выводи item_id.**

10. **Запреты**:
    • Нельзя выполнять `DROP`, `ALTER` и т. д.
    • Нельзя выполнять непараметризованный `DELETE FROM stock` /
      `DELETE FROM items`.
    • Если пользователь явно просит «выполни raw sql…» — отвечай ошибкой.
"""

# ─────────────────────────────────────────────────────────────────────────────
# 4. SQL‑валидация и безопасность
# ─────────────────────────────────────────────────────────────────────────────
SAFE_SQL_PATTERN = re.compile(
    r"^\s*(WITH\b[\s\S]+?\b(SELECT|INSERT|UPDATE|DELETE)"
    r"|SELECT|INSERT|UPDATE|DELETE)\b",
    re.I
)
FORBIDDEN_PATTERN = re.compile(
    r"^\s*(DROP|ALTER|TRUNCATE|PRAGMA|ATTACH|DETACH)\b",
    re.I
)

def validate_sql(sql: str) -> List[str] | None:
    stmts = [s.strip() for s in sql.split(";") if s.strip()]
    if not (1 <= len(stmts) <= 6):
        return None

    for s in stmts:
        if FORBIDDEN_PATTERN.match(s):
            return None
        if not SAFE_SQL_PATTERN.match(s):
            return None

        if re.match(r"^\s*DELETE\s+FROM\s+stock\b", s, re.I) \
           and not re.search(r"\bWHERE\b.*\bitem_id\b.*\blocation_code\b", s, re.I):
            return None
        if re.match(r"^\s*DELETE\s+FROM\s+items\b", s, re.I):
            return None

    return stmts

# ─────────────────────────────────────────────────────────────────────────────
# 5. Markdown V2 helpers
# ─────────────────────────────────────────────────────────────────────────────
_MD2_RE = re.compile(r'([_\*\[\]\(\)~`>#+\-=|{}.!])')

def md2_escape(t: str) -> str:
    return _MD2_RE.sub(r'\\\1', t)

# ─────────────────────────────────────────────────────────────────────────────
# 6. Форматирование вывода
# ─────────────────────────────────────────────────────────────────────────────
def fmt_table(rows: List[Dict[str,Any]]) -> str:
    if not rows:
        return "По запросу ничего не найдено."
    PLACEHOLDER = "-"
    hdr = {"name":"Товар", "size":"Размер",
           "location_code":"Ячейка", "qty":"Кол-во"}
    keys = list(rows[0].keys())
    widths = {
        k: max(
            len(hdr.get(k, k)),
            *(
                len(str(r[k] if (k != "size" or r[k] is not None)
                             else PLACEHOLDER))
                for r in rows
            )
        )
        for k in keys
    }
    for r in rows:
        if r.get("size") is None:
            r["size"] = PLACEHOLDER
    def row(r):
        return " | ".join(str(r[k]).ljust(widths[k]) for k in keys)
    header = " | ".join(hdr.get(k, k).ljust(widths[k]) for k in keys)
    sep    = "-+-".join("-" * widths[k] for k in keys)
    return "```\n" + "\n".join([header, sep] + [row(r) for r in rows]) + "\n```"

# ─────────────────────────────────────────────────────────────────────────────
# 7. Вспомогательные функции (⭐ ИЗМЕНЕНО)
# ─────────────────────────────────────────────────────────────────────────────
async def build_stock_context(db: aiosqlite.Connection, limit: int = 20) -> str:
    db.row_factory = aiosqlite.Row
    cur = await db.execute("""
        SELECT items.name,
               IFNULL(items.size,'NULL') AS size,
               stock.location_code, stock.qty
        FROM stock JOIN items ON items.id = stock.item_id
        ORDER BY items.name, items.size, stock.location_code
    """)
    rows = await cur.fetchall()
    lines = [
        f"- {r['name']}, {r['size']}, {r['location_code']}, qty={r['qty']}"
        for r in rows[:limit]
    ]
    if len(rows) > limit:
        lines.append("- …")
    return "Текущий склад (первые 20 строк):\n" + "\n".join(lines)

async def ask_llm(db: aiosqlite.Connection, user_prompt: str) -> Dict[str,Any] | None:
    context = await build_stock_context(db)
    try:
        resp = await ai.chat.completions.create(
            model=MODEL,
            temperature=0.0,
            messages=[
                {"role":"system","content":SYSTEM_PROMPT_SQL},
                {"role":"system","content":context},
                {"role":"user","content":user_prompt},
            ],
            response_format={"type":"json_object"}
        )
        return json.loads(resp.choices[0].message.content)
    except (OpenAIError, json.JSONDecodeError) as e:
        logging.error("LLM error: %s", e)
        return None

async def run_sql(db: aiosqlite.Connection, stmts: List[str], mode: str, user: str, summary: str | None) -> List[Dict[str,Any]]:
    res: List[Dict[str,Any]] = []
    sql_all = "; ".join(stmts)
    ok = False
    try:
        async with db.execute("BEGIN;"): # Используем существующее соединение
            for s in stmts:
                cur = await db.execute(s)
                if mode == "read":
                    res = [dict(r) for r in await cur.fetchall()]
        await db.commit()
        ok = True
        return res
    finally:
        # Логируем результат в той же сессии, но вне основной транзакции
        await db.execute(
            "INSERT INTO tx_log(user, sql_text, summary, success) VALUES(?,?,?,?)",
            (user, sql_all, summary, int(ok))
        )
        await db.commit()


# ──────────────────────────────────────────
# 8.  Команды Telegram (⭐ ИЗМЕНЕНО)
# ──────────────────────────────────────────
HELP_TEXT = """
*Команды бота*
`/stock` — показать всё содержимое склада
`/export` — выгрузить таблицу *stock* в CSV
`/logs` — последние операции в читаемом виде
`/help` — эта справка

*Примеры запросов*
• «положи 4 майки L в а2»
• «забрал все гайки 5 из c9»
• «перемести 3 браслета из в5 в а3»
• «где лежат блокноты»
"""

@router.message(Command("help"))
async def cmd_help(msg: types.Message):
    await msg.answer(md2_escape(HELP_TEXT), parse_mode="MarkdownV2")

@router.message(Command("stock"))
async def cmd_stock(msg: types.Message, db: aiosqlite.Connection):
    db.row_factory = aiosqlite.Row
    cur = await db.execute("""
        SELECT items.name, items.size, stock.location_code, stock.qty
        FROM stock JOIN items ON items.id = stock.item_id
    """)
    rows = await cur.fetchall()
    text = fmt_table([dict(r) for r in rows])
    if text.startswith("```"):
        await msg.answer(text, parse_mode="MarkdownV2")
    else:
        await msg.answer(md2_escape(text), parse_mode="MarkdownV2")

@router.message(Command("export"))
async def cmd_export(msg: types.Message, db: aiosqlite.Connection):
    db.row_factory = aiosqlite.Row
    cur = await db.execute("""
        SELECT items.name, IFNULL(items.size,'') AS size,
               stock.location_code, stock.qty
        FROM stock JOIN items ON items.id = stock.item_id
        ORDER BY stock.location_code
    """)
    rows = await cur.fetchall()
    buf=io.StringIO(); w=csv.writer(buf); w.writerow([c[0] for c in cur.description]); w.writerows(rows); buf.seek(0)
    await msg.answer_document(types.BufferedInputFile(buf.getvalue().encode(),"stock.csv"))

@router.message(Command("logs"))
async def cmd_logs(msg: types.Message, db: aiosqlite.Connection):
    """Выводит последние 15 успешных операций с читаемым описанием."""
    db.row_factory = aiosqlite.Row
    cur = await db.execute("""
        SELECT ts, user, summary FROM tx_log
        WHERE success = 1 AND summary IS NOT NULL AND summary != ''
        ORDER BY ts DESC
        LIMIT 15
    """)
    logs = await cur.fetchall()

    if not logs:
        await msg.answer(md2_escape("Пока не было выполнено ни одной операции."), parse_mode="MarkdownV2")
        return

    # Форматируем вывод: '2025-07-21 11:57' -> '2025‑07‑21 11:57' (неразрывный дефис)
    # И добавляем тире '—'
    log_lines = [
        f"`{l['ts'][:16].replace('-', '‑')}` — *{md2_escape(l['user'])}* — {md2_escape(l['summary'])}"
        for l in logs
    ]
    await msg.answer("\n".join(log_lines), parse_mode="MarkdownV2")

@router.message()
async def on_message(msg: types.Message, db: aiosqlite.Connection):
    if not msg.text or msg.text.startswith('/'):
        return

    user_prompt = msg.text.strip()
    username    = msg.from_user.username or "unknown_user"

    status = await msg.answer(md2_escape("Думаю…"), parse_mode="MarkdownV2")

    payload = await ask_llm(db, user_prompt)
    if not isinstance(payload, dict):
        await status.edit_text(md2_escape("🤖 Ошибка разбора ответа LLM."),
                               parse_mode="MarkdownV2")
        return

    if payload.get("error"):
        await status.edit_text(md2_escape("⚠️ " + payload["error"]),
                               parse_mode="MarkdownV2")
        # Можно добавить логирование неудачных запросов
        await db.execute(
            "INSERT INTO tx_log(user, sql_text, summary, success) VALUES(?,?,?,?)",
            (username, user_prompt, payload["error"], 0)
        )
        await db.commit()
        return

    sql, mode, summary = payload.get("sql"), payload.get("mode"), payload.get("summary")
    if not sql or mode not in {"read", "write"}:
        await status.edit_text(md2_escape("🤖 Неправильный формат JSON."),
                               parse_mode="MarkdownV2")
        return

    stmts = validate_sql(sql)
    if not stmts:
        await status.edit_text(md2_escape("⚠️ Запрос отклонён проверкой безопасности."),
                               parse_mode="MarkdownV2")
        return

    try:
        await status.edit_text(md2_escape("Выполняю запрос…"),
                               parse_mode="MarkdownV2")
        rows = await run_sql(db, stmts, mode, username, summary)
    except aiosqlite.Error as e:
        await status.edit_text(md2_escape(f"⚠️ Ошибка БД: {e}"),
                               parse_mode="MarkdownV2")
        return

    if mode == "read":
        text = fmt_table(rows)
        if text.startswith("```"):
            await status.edit_text(text, parse_mode="MarkdownV2")
        else:
            await status.edit_text(md2_escape(text), parse_mode="MarkdownV2")
    else:
        final_message = md2_escape("✅ Операция выполнена успешно.")
        if summary:
            final_message += f"\n\n_{md2_escape(summary)}_"
        await status.edit_text(final_message, parse_mode="MarkdownV2")



# ─────────────────────────────────────────────────────────────────────────────
# 9. Запуск (⭐ ИЗМЕНЕНО)
# ─────────────────────────────────────────────────────────────────────────────
async def set_main_menu(bot: Bot):
    """Создаёт кнопку меню с командами."""
    # Создаём список команд для меню
    main_menu_commands = [
        BotCommand(command="/stock", description="📦 Показать всё на складе"),
        BotCommand(command="/logs", description="📖 Посмотреть последние операции"),
        BotCommand(command="/export", description="📥 Экспорт склада в CSV"),
        BotCommand(command="/help", description="❓ Помощь по командам")
    ]
    
    await bot.set_my_commands(main_menu_commands)
async def main():
    # Создаем подключение к БД один раз при старте
    async with aiosqlite.connect(DB_PATH) as db:
        await init_db(db)
        await set_main_menu(bot)
        dp = Dispatcher(db=db) # Передаем объект БД в Dispatcher
        dp.include_router(router)
        await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен.")