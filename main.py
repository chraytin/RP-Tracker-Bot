import os
import time
import sqlite3
import asyncio
import traceback
from typing import Optional, List, Tuple

import discord
from discord.ext import commands
from aiohttp import web

# =========================
# CONFIG
# =========================
TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is not set.")

DB_FILE = "rp_tracker.db"

print("Booting RP Tracker...", flush=True)

# =========================
# DATABASE
# =========================
def db():
    return sqlite3.connect(DB_FILE)

def ensure_schema():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            message_id INTEGER PRIMARY KEY,
            state INTEGER,
            started_at REAL,
            run_seconds REAL,
            channel_id INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS participants (
            message_id INTEGER,
            user_id INTEGER,
            character TEXT,
            level INTEGER,
            seconds REAL,
            last_tick REAL,
            capped INTEGER,
            PRIMARY KEY (message_id, user_id)
        )
    """)

    conn.commit()
    conn.close()

ensure_schema()

# =========================
# REWARD RULES
# =========================
def reward_hours(seconds: float) -> int:
    return int((max(0.0, seconds) + 900) // 3600)

def xp_per_hour(level: int) -> int:
    if 2 <= level <= 4: return 300
    if 5 <= level <= 8: return 600
    if 9 <= level <= 12: return 800
    if 13 <= level <= 16: return 1000
    if 17 <= level <= 20: return 1200
    return 0

def gp_per_hour(level: int) -> int:
    return level * 10

# =========================
# THEME
# =========================
def apply_theme(embed: discord.Embed) -> discord.Embed:
    embed.color = discord.Color(int(os.getenv("THEME_COLOR", "C9A227"), 16))
    embed.set_author(name=os.getenv("THEME_NAME", "Adventurer’s Guild Ledger"))
    embed.set_footer(text=os.getenv(
        "THEME_FOOTER_TEXT",
        "Filed by the Guild Registrar • Rewards granted on 45-minute marks"
    ))
    if os.getenv("THEME_THUMBNAIL_URL"):
        embed.set_thumbnail(url=os.getenv("THEME_THUMBNAIL_URL"))
    if os.getenv("THEME_BANNER_URL"):
        embed.set_image(url=os.getenv("THEME_BANNER_URL"))
    return embed

# =========================
# BOT
# =========================
intents = discord.Intents.default()
intents.guilds = True
bot = commands.Bot(command_prefix="!", intents=intents)

# =========================
# KEEPALIVE
# =========================
async def start_web_server():
    app = web.Application()
    app.router.add_get("/", lambda _: web.Response(text="OK"))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "8080")))
    await site.start()

# =========================
# HELPERS
# =========================
def get_session(message_id):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT state, started_at, run_seconds, channel_id FROM sessions WHERE message_id=?", (message_id,))
    row = cur.fetchone()
    conn.close()
    return row if row else (0, None, 0.0, None)

def list_participants(message_id):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        SELECT user_id, character, level, seconds, capped
        FROM participants WHERE message_id=?
    """, (message_id,))
    rows = cur.fetchall()
    conn.close()
    return rows

def tracker_link(guild_id, channel_id, message_id):
    return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"

# =========================
# JOIN MODAL
# =========================
class JoinModal(discord.ui.Modal, title="Adventurer Sign-In"):
    name = discord.ui.TextInput(label="Character Name")
    level = discord.ui.TextInput(label="Level (1-20)")
    capped = discord.ui.TextInput(label="Capped? (yes/no)", required=False)

    def __init__(self, mid):
        super().__init__()
        self.mid = mid

    async def on_submit(self, interaction: discord.Interaction):
        lvl = int(self.level.value)
        is_capped = self.capped.value.lower() in ("yes", "y", "true", "1", "capped")

        conn = db()
        cur = conn.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO participants
            VALUES (?, ?, ?, ?, COALESCE(
                (SELECT seconds FROM participants WHERE message_id=? AND user_id=?), 0
            ), ?, ?)
        """, (
            self.mid, interaction.user.id, self.name.value, lvl,
            self.mid, interaction.user.id,
            time.time(), int(is_capped)
        ))
        conn.commit()
        conn.close()

        await interaction.response.send_message("✅ Signed in.", ephemeral=True)

# =========================
# VIEW
# =========================
class RPView(discord.ui.View):
    def __init__(self, mid):
        super().__init__(timeout=None)
        self.mid = mid

    @discord.ui.button(label="Join", style=discord.ButtonStyle.success)
    async def join(self, interaction, _):
        await interaction.response.send_modal(JoinModal(self.mid))

    @discord.ui.button(label="End", style=discord.ButtonStyle.danger)
    async def end(self, interaction, _):
        state, started_at, run_seconds, channel_id = get_session(self.mid)
        if state == 1:
            run_seconds += time.time() - started_at

        conn = db()
        cur = conn.cursor()
        cur.execute("UPDATE sessions SET state=0, run_seconds=? WHERE message_id=?", (run_seconds, self.mid))
        conn.commit()
        conn.close()

        parts = list_participants(self.mid)
        lines = []

        for uid, char, lvl, secs, cap in parts:
            hrs = reward_hours(secs)
            gp = gp_per_hour(lvl) * hrs
            if cap:
                lines.append(f"<@{uid}> — **{char}** → 🗝️ {hrs} | {gp} GP")
            else:
                xp = xp_per_hour(lvl) * hrs
                lines.append(f"<@{uid}> — **{char}** → {xp} XP | {gp} GP")

        start = tracker_link(interaction.guild_id, channel_id, self.mid)

        await interaction.response.send_message(
            f"🏁 **Guild Ledger Closed**\n"
            f"🔗 Start: {start}\n\n" +
            "\n".join(lines)
        )

        rewards_msg = await interaction.original_response()
        await rewards_msg.edit(
            content=rewards_msg.content + f"\n\n🔗 End: {rewards_msg.jump_url}"
        )

# =========================
# SLASH COMMAND
# =========================
@bot.tree.command(name="rpbegin", description="Begin an RP session")
async def rpbegin(interaction: discord.Interaction):
    embed = apply_theme(discord.Embed(
        title="📜 Adventurer’s Guild — RP Session Log",
        description="Attendance and session time"
    ))
    await interaction.response.send_message(embed=embed)
    msg = await interaction.original_response()

    conn = db()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO sessions VALUES (?, 1, ?, 0, ?)",
        (msg.id, time.time(), msg.channel.id)
    )
    conn.commit()
    conn.close()

    view = RPView(msg.id)
    await msg.edit(view=view)
    bot.add_view(view)

    try:
        await msg.pin()
    except:
        pass

# =========================
# READY
# =========================
@bot.event
async def on_ready():
    await bot.tree.sync()
    print(f"Logged in as {bot.user}", flush=True)

# =========================
# MAIN
# =========================
async def main():
    await start_web_server()
    await bot.start(TOKEN)

asyncio.run(main())
