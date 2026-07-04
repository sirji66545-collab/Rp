#!/usr/bin/env python3
"""
════════════════════════════════════════════════════════════════════
  UPTIME GUARDIAN — All-in-One Telegram Bot Hosting + Uptime Monitor
════════════════════════════════════════════════════════════════════
  Single-file, production-ready.
  • Host your own .py bots (auto venv + pip install)
  • UptimeRobot-style 1-min HTTP/process checks + response times
  • Glassmorphism UI, small-caps font, animated status
  • SQLite persistence, Flask /health + /status for Render
════════════════════════════════════════════════════════════════════
"""

import asyncio
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import psutil
from flask import Flask, jsonify
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)
from threading import Thread

# ─────────────────────────── CONFIG ───────────────────────────────
BOT_TOKEN   = os.environ.get("BOT_TOKEN", "PUT_TOKEN_HERE")
OWNER_ID    = int(os.environ.get("OWNER_ID", "0"))   # your Telegram user id — upload gate
PING_EVERY  = 60          # seconds between uptime checks
MAX_BOTS    = 5           # per user
MAX_FILE_MB = 10
BASE_DIR    = Path("hosted_bots")
DB_PATH     = "guardian.db"
BASE_DIR.mkdir(exist_ok=True)

# ─────────────────────────── LOGGING ──────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(name)s │ %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("guardian.log")],
)
log = logging.getLogger("guardian")


# ═══════════════════════════════════════════════════════════════════
#  ANIMATIONS  — small-caps font + fluid frames
# ═══════════════════════════════════════════════════════════════════
class Animations:
    _MAP = str.maketrans(
        "abcdefghijklmnopqrstuvwxyz",
        "ᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘqʀsᴛᴜᴠᴡxʏᴢ",
    )

    @classmethod
    def sc(cls, text: str) -> str:
        """Convert to small-caps (digits/symbols untouched)."""
        return text.lower().translate(cls._MAP)

    SPINNER = ("⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧")
    PULSE   = ("▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰")

    @classmethod
    async def run(cls, msg, base: str, frames=None, cycles: int = 1):
        frames = frames or cls.SPINNER
        for _ in range(cycles):
            for f in frames:
                try:
                    await msg.edit_text(f"{f} {cls.sc(base)}")
                    await asyncio.sleep(0.16)
                except Exception:
                    return

    @staticmethod
    def bar(pct: float, width: int = 12) -> str:
        """Text visualization bar for response-time / uptime graphs."""
        filled = int(round(pct / 100 * width))
        return "█" * filled + "░" * (width - filled)


sc = Animations.sc  # shorthand used everywhere


# ═══════════════════════════════════════════════════════════════════
#  DATABASE  — bots + uptime logs
# ═══════════════════════════════════════════════════════════════════
class DatabaseManager:
    def __init__(self, path=DB_PATH):
        self.path = path
        self._init()

    def _conn(self):
        c = sqlite3.connect(self.path)
        c.row_factory = sqlite3.Row
        return c

    def _init(self):
        with self._conn() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS bots (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                owner     INTEGER,
                name      TEXT,
                path      TEXT,
                kind      TEXT DEFAULT 'telegram',   -- telegram | web
                url       TEXT,                       -- for web/uptime monitoring
                pid       INTEGER,
                status    TEXT DEFAULT 'stopped',
                created   TEXT
            );
            CREATE TABLE IF NOT EXISTS uptime (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_id    INTEGER,
                ts        TEXT,
                ok        INTEGER,
                ms        INTEGER,
                note      TEXT
            );
            """)

    # -- bots --
    def add_bot(self, owner, name, path, kind, url=None):
        with self._conn() as c:
            cur = c.execute(
                "INSERT INTO bots (owner,name,path,kind,url,created) VALUES (?,?,?,?,?,?)",
                (owner, name, path, kind, url, datetime.now(timezone.utc).isoformat()),
            )
            return cur.lastrowid

    def bots_of(self, owner):
        with self._conn() as c:
            return c.execute("SELECT * FROM bots WHERE owner=? ORDER BY id", (owner,)).fetchall()

    def get_bot(self, bot_id):
        with self._conn() as c:
            return c.execute("SELECT * FROM bots WHERE id=?", (bot_id,)).fetchone()

    def all_bots(self):
        with self._conn() as c:
            return c.execute("SELECT * FROM bots").fetchall()

    def set_status(self, bot_id, status, pid=None):
        with self._conn() as c:
            c.execute("UPDATE bots SET status=?, pid=? WHERE id=?", (status, pid, bot_id))

    def del_bot(self, bot_id):
        with self._conn() as c:
            c.execute("DELETE FROM bots WHERE id=?", (bot_id,))
            c.execute("DELETE FROM uptime WHERE bot_id=?", (bot_id,))

    # -- uptime --
    def log_uptime(self, bot_id, ok, ms, note=""):
        with self._conn() as c:
            c.execute("INSERT INTO uptime (bot_id,ts,ok,ms,note) VALUES (?,?,?,?,?)",
                      (bot_id, datetime.now(timezone.utc).isoformat(), int(ok), ms, note))

    def stats(self, bot_id, limit=200):
        with self._conn() as c:
            rows = c.execute("SELECT ok,ms FROM uptime WHERE bot_id=? ORDER BY id DESC LIMIT ?",
                             (bot_id, limit)).fetchall()
        if not rows:
            return {"sla": 0.0, "avg_ms": 0, "samples": 0, "recent": []}
        oks = [r["ok"] for r in rows]
        ms  = [r["ms"] for r in rows if r["ms"]]
        sla = round(sum(oks) / len(oks) * 100, 2)
        return {
            "sla": sla,
            "avg_ms": int(sum(ms) / len(ms)) if ms else 0,
            "samples": len(rows),
            "recent": list(reversed(rows[:20])),
        }


db = DatabaseManager()


# ═══════════════════════════════════════════════════════════════════
#  PROCESS MANAGER  — venv, pip install, run/stop, auto-restart
# ═══════════════════════════════════════════════════════════════════
class ProcessManager:
    procs: dict[int, subprocess.Popen] = {}

    @staticmethod
    def _venv_python(bot_dir: Path) -> Path:
        return bot_dir / ("Scripts" if os.name == "nt" else "bin") / \
               ("python.exe" if os.name == "nt" else "python")

    @classmethod
    def setup_env(cls, bot_dir: Path) -> str:
        """Create venv + install requirements.txt if present. Returns a status note."""
        venv = bot_dir / "venv"
        if not venv.exists():
            subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
        py = cls._venv_python(venv)
        req = bot_dir / "requirements.txt"
        if req.exists():
            subprocess.run([str(py), "-m", "pip", "install", "--quiet",
                            "--upgrade", "pip"], check=False)
            r = subprocess.run([str(py), "-m", "pip", "install", "--quiet",
                                "-r", str(req)], capture_output=True, text=True)
            return "deps installed" if r.returncode == 0 else f"pip warn: {r.stderr[:120]}"
        return "no requirements.txt"

    @classmethod
    def detect_kind(cls, script: Path) -> str:
        """Naive detection: web server vs telegram bot."""
        try:
            src = script.read_text(errors="ignore").lower()
        except Exception:
            return "telegram"
        if any(k in src for k in ("flask", "fastapi", "uvicorn", "aiohttp.web", "app.run")):
            return "web"
        return "telegram"

    @classmethod
    def start(cls, bot_id: int) -> bool:
        b = db.get_bot(bot_id)
        if not b:
            return False
        bot_dir = Path(b["path"])
        script  = bot_dir / "bot.py"
        py = cls._venv_python(bot_dir / "venv")
        py = str(py) if py.exists() else sys.executable
        logf = open(bot_dir / "runtime.log", "a")
        proc = subprocess.Popen([py, str(script)], cwd=str(bot_dir),
                                stdout=logf, stderr=subprocess.STDOUT)
        cls.procs[bot_id] = proc
        db.set_status(bot_id, "online", proc.pid)
        log.info(f"started bot #{bot_id} pid={proc.pid}")
        return True

    @classmethod
    def stop(cls, bot_id: int) -> bool:
        proc = cls.procs.pop(bot_id, None)
        b = db.get_bot(bot_id)
        pid = proc.pid if proc else (b["pid"] if b else None)
        if pid:
            try:
                p = psutil.Process(pid)
                for child in p.children(recursive=True):
                    child.terminate()
                p.terminate()
                try:
                    p.wait(timeout=8)
                except psutil.TimeoutExpired:
                    p.kill()  # graceful → forced
            except psutil.NoSuchProcess:
                pass
        db.set_status(bot_id, "stopped", None)
        log.info(f"stopped bot #{bot_id}")
        return True

    @classmethod
    def restart(cls, bot_id: int) -> bool:
        cls.stop(bot_id)
        time.sleep(1)
        return cls.start(bot_id)

    @classmethod
    def is_alive(cls, bot_id: int) -> bool:
        b = db.get_bot(bot_id)
        if not b or not b["pid"]:
            return False
        return psutil.pid_exists(b["pid"])

    @classmethod
    def watchdog(cls):
        """Auto-restart crashed 'online' bots."""
        for b in db.all_bots():
            if b["status"] == "online" and not cls.is_alive(b["id"]):
                log.warning(f"bot #{b['id']} crashed — auto-restarting")
                cls.start(b["id"])


# ═══════════════════════════════════════════════════════════════════
#  UPTIME MONITOR  — 1-min HTTP + process checks, response times
# ═══════════════════════════════════════════════════════════════════
class UptimeMonitor:
    @staticmethod
    async def check_one(session, b):
        bot_id, kind, url = b["id"], b["kind"], b["url"]
        if kind == "web" and url:
            try:
                t0 = time.monotonic()
                async with session.get(url, timeout=30) as r:
                    ms = int((time.monotonic() - t0) * 1000)
                    ok = r.status < 400
                    db.log_uptime(bot_id, ok, ms, f"http {r.status}")
            except Exception as e:
                db.log_uptime(bot_id, False, 0, type(e).__name__)
        else:
            alive = ProcessManager.is_alive(bot_id)
            db.log_uptime(bot_id, alive, 0, "process")

    @classmethod
    async def loop(cls):
        log.info("uptime monitor started")
        async with aiohttp.ClientSession() as session:
            while True:
                ProcessManager.watchdog()
                bots = db.all_bots()
                await asyncio.gather(*(cls.check_one(session, b) for b in bots),
                                     return_exceptions=True)
                await asyncio.sleep(PING_EVERY)


# ═══════════════════════════════════════════════════════════════════
#  UI  — glassmorphism cards + inline keyboards
# ═══════════════════════════════════════════════════════════════════
class UI:
    @staticmethod
    def main_menu():
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 " + sc("upload bot file"), callback_data="upload")],
            [InlineKeyboardButton("📋 " + sc("my hosted bots"), callback_data="list")],
            [InlineKeyboardButton("📊 " + sc("uptime monitor"), callback_data="uptime")],
            [InlineKeyboardButton("⚙️ " + sc("advanced settings"), callback_data="settings")],
        ])

    @staticmethod
    def bot_menu(bot_id, running):
        toggle = ("⏹️ " + sc("stop"), f"stop:{bot_id}") if running else \
                 ("▶️ " + sc("start"), f"start:{bot_id}")
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(*toggle),
             InlineKeyboardButton("🔄 " + sc("restart"), callback_data=f"restart:{bot_id}")],
            [InlineKeyboardButton("📋 " + sc("view logs"), callback_data=f"logs:{bot_id}"),
             InlineKeyboardButton("📊 " + sc("stats"), callback_data=f"stats:{bot_id}")],
            [InlineKeyboardButton("🗑️ " + sc("delete"), callback_data=f"del:{bot_id}")],
            [InlineKeyboardButton("« " + sc("back"), callback_data="list")],
        ])

    @staticmethod
    def dot(status):
        return {"online": "🟢", "stopped": "🔴", "loading": "🟡"}.get(status, "⚪")

    @staticmethod
    def card_home():
        return (
            "╭─────────────────────────╮\n"
            f"│   ⚡ {sc('uptime guardian')}   │\n"
            "╰─────────────────────────╯\n\n"
            f"◆ {sc('host your python bots')}\n"
            f"◆ {sc('keep web apps awake 24x7')}\n"
            f"◆ {sc('1-minute uptime checks')}\n\n"
            f"➤ {sc('choose an option below')}"
        )


# ═══════════════════════════════════════════════════════════════════
#  HANDLERS
# ═══════════════════════════════════════════════════════════════════
async def start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(UI.card_home(), reply_markup=UI.main_menu())


async def on_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user.id
    # ── owner-only upload gate (prevents arbitrary RCE from strangers) ──
    if OWNER_ID and user != OWNER_ID:
        await update.message.reply_text("🔒 " + sc("uploads are restricted to the owner."))
        return

    doc = update.message.document
    if not doc.file_name.endswith(".py"):
        await update.message.reply_text("⚠️ " + sc("only .py files are accepted."))
        return
    if doc.file_size > MAX_FILE_MB * 1024 * 1024:
        await update.message.reply_text("⚠️ " + sc(f"file too large (max {MAX_FILE_MB}mb)."))
        return
    if len(db.bots_of(user)) >= MAX_BOTS:
        await update.message.reply_text("⚠️ " + sc(f"bot limit reached (max {MAX_BOTS})."))
        return

    msg = await update.message.reply_text(sc("receiving file"))
    await Animations.run(msg, "deploying your bot", Animations.PULSE, cycles=1)

    # save into isolated dir
    name = doc.file_name[:-3]
    bot_dir = BASE_DIR / f"{user}_{name}_{int(time.time())}"
    bot_dir.mkdir(parents=True, exist_ok=True)
    tg_file = await doc.get_file()
    await tg_file.download_to_drive(str(bot_dir / "bot.py"))

    kind = ProcessManager.detect_kind(bot_dir / "bot.py")
    await msg.edit_text("⚙️ " + sc("setting up environment + installing deps"))
    note = await asyncio.to_thread(ProcessManager.setup_env, bot_dir)

    bot_id = db.add_bot(user, name, str(bot_dir), kind)
    await msg.edit_text(
        "╭─────────────────────────╮\n"
        f"│   ✅ {sc('deployed')}   │\n"
        "╰─────────────────────────╯\n\n"
        f"🤖 {sc('name')}: {name}\n"
        f"🧩 {sc('type')}: {sc(kind)}\n"
        f"📦 {sc('env')}: {sc(note)}\n\n"
        f"{sc('use the menu to start it.')}",
        reply_markup=UI.bot_menu(bot_id, running=False),
    )


async def render_list(user):
    bots = db.bots_of(user)
    if not bots:
        return "📭 " + sc("no hosted bots yet. upload a .py file to begin."), None
    lines = ["╭─────────────────────────╮",
             f"│   📋 {sc('your bots')}   │",
             "╰─────────────────────────╯\n"]
    kb = []
    for b in bots:
        live = ProcessManager.is_alive(b["id"])
        status = "online" if live else "stopped"
        lines.append(f"{UI.dot(status)} {b['name']}  ·  {sc(b['kind'])}")
        kb.append([InlineKeyboardButton(f"{UI.dot(status)} {b['name']}",
                                        callback_data=f"open:{b['id']}")])
    kb.append([InlineKeyboardButton("« " + sc("back"), callback_data="home")])
    return "\n".join(lines), InlineKeyboardMarkup(kb)


def render_stats(bot_id):
    b = db.get_bot(bot_id)
    s = db.stats(bot_id)
    graph = ""
    for r in s["recent"]:
        mark = "🟩" if r["ok"] else "🟥"
        graph += mark
    sla_bar = Animations.bar(s["sla"])
    return (
        "╭─────────────────────────╮\n"
        f"│   📊 {sc('uptime stats')}   │\n"
        "╰─────────────────────────╯\n\n"
        f"🤖 {b['name']}\n"
        f"🎯 {sc('sla')}: {s['sla']}%  [{sla_bar}]\n"
        f"⚡ {sc('avg response')}: {s['avg_ms']}ms\n"
        f"🔁 {sc('checks')}: {s['samples']}\n\n"
        f"{sc('recent')}:\n{graph or sc('no data yet')}\n\n"
        f"🎯 {sc('target')}: 99.9% {sc('sla')}"
    )


async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    user = q.from_user.id

    if data == "home":
        await q.edit_message_text(UI.card_home(), reply_markup=UI.main_menu()); return
    if data == "upload":
        await q.edit_message_text("📤 " + sc("send me a .py file (max 10mb) to host it.")); return
    if data in ("list", "uptime"):
        text, kb = await render_list(user)
        await q.edit_message_text(text, reply_markup=kb or UI.main_menu()); return
    if data == "settings":
        await q.edit_message_text(
            "⚙️ " + sc("advanced settings") + "\n\n" +
            f"◆ {sc('max bots')}: {MAX_BOTS}\n"
            f"◆ {sc('check interval')}: 60s\n"
            f"◆ {sc('max file')}: {MAX_FILE_MB}mb\n"
            f"◆ {sc('auto-restart')}: {sc('on')}",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("« " + sc("back"), callback_data="home")]])); return

    # actions with :id
    if ":" in data:
        action, bid = data.split(":"); bid = int(bid)
        b = db.get_bot(bid)
        if not b or b["owner"] != user:
            await q.answer(sc("not found"), show_alert=True); return

        if action == "open":
            live = ProcessManager.is_alive(bid)
            await q.edit_message_text(
                f"{UI.dot('online' if live else 'stopped')} {b['name']}\n"
                f"🧩 {sc(b['kind'])}  ·  {sc('online' if live else 'stopped')}",
                reply_markup=UI.bot_menu(bid, live)); return
        if action == "start":
            await q.edit_message_text("🟡 " + sc("starting"))
            await asyncio.to_thread(ProcessManager.start, bid)
            await q.edit_message_text("🟢 " + sc(f"{b['name']} is online"),
                                      reply_markup=UI.bot_menu(bid, True)); return
        if action == "stop":
            await asyncio.to_thread(ProcessManager.stop, bid)
            await q.edit_message_text("🔴 " + sc(f"{b['name']} stopped"),
                                      reply_markup=UI.bot_menu(bid, False)); return
        if action == "restart":
            await q.edit_message_text("🔄 " + sc("restarting"))
            await asyncio.to_thread(ProcessManager.restart, bid)
            await q.edit_message_text("🟢 " + sc(f"{b['name']} restarted"),
                                      reply_markup=UI.bot_menu(bid, True)); return
        if action == "logs":
            logf = Path(b["path"]) / "runtime.log"
            tail = ""
            if logf.exists():
                tail = "".join(logf.read_text(errors="ignore").splitlines(keepends=True)[-15:])
            await q.edit_message_text(
                "📋 " + sc("recent logs") + f"\n\n<pre>{tail or sc('empty')}</pre>",
                parse_mode="HTML",
                reply_markup=UI.bot_menu(bid, ProcessManager.is_alive(bid))); return
        if action == "stats":
            await q.edit_message_text(render_stats(bid),
                                      reply_markup=UI.bot_menu(bid, ProcessManager.is_alive(bid))); return
        if action == "del":
            ProcessManager.stop(bid)
            shutil.rmtree(b["path"], ignore_errors=True)
            db.del_bot(bid)
            text, kb = await render_list(user)
            await q.edit_message_text("🗑️ " + sc("deleted.") + "\n\n" + text,
                                      reply_markup=kb or UI.main_menu()); return


# ═══════════════════════════════════════════════════════════════════
#  FLASK  — /health + /status for Render
# ═══════════════════════════════════════════════════════════════════
flask_app = Flask(__name__)


@flask_app.route("/health")
def health():
    return jsonify(status="ok", ts=datetime.now(timezone.utc).isoformat())


@flask_app.route("/status")
def status():
    bots = db.all_bots()
    return jsonify(
        total=len(bots),
        online=sum(1 for b in bots if ProcessManager.is_alive(b["id"])),
        bots=[{"name": b["name"], "kind": b["kind"],
               "alive": ProcessManager.is_alive(b["id"])} for b in bots],
    )


def run_flask():
    port = int(os.environ.get("PORT", 8080))
    flask_app.run(host="0.0.0.0", port=port)


# ═══════════════════════════════════════════════════════════════════
#  BOOT
# ═══════════════════════════════════════════════════════════════════
async def post_init(app: Application):
    app.create_task(UptimeMonitor.loop())


def main():
    if BOT_TOKEN == "PUT_TOKEN_HERE":
        log.error("set BOT_TOKEN env var first."); sys.exit(1)

    Thread(target=run_flask, daemon=True).start()   # Flask for Render

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(MessageHandler(filters.Document.ALL, on_document))
    app.add_handler(CallbackQueryHandler(on_callback))

    log.info("ɢᴜᴀʀᴅɪᴀɴ ɪs ʟɪᴠᴇ")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
