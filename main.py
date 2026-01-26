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
    raise RuntimeError("DISCORD_TOKEN is not set. Add it in Railway → Variables.")

DB_FILE = "rp_tracker.db"

print("Booting RP Tracker...", flush=True)


# =========================
# DATABASE + SCHEMA MIGRATION
# =========================
def db():
    return sqlite3.connect(DB_FILE)


def ensure_schema():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            message_id INTEGER PRIMARY KEY,
            active INTEGER,
            started_at REAL,
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
            PRIMARY KEY (message_id, user_id)
        )
    """)

    # Migration: ensure channel_id exists
    cur.execute("PRAGMA table_info(sessions)")
    cols = {row[1] for row in cur.fetchall()}
    if "channel_id" not in cols:
        cur.execute("ALTER TABLE sessions ADD COLUMN channel_id INTEGER")

    conn.commit()
    conn.close()


ensure_schema()


# =========================
# REWARD RULES
# =========================
def reward_hours(seconds: float) -> int:
    """
    Whole-hour rewards with a 45-minute threshold.
    0h until 00:45:00, 1h until 01:45:00, etc.
    """
    return int((max(0.0, seconds) + 900) // 3600)


def xp_per_hour_for_level(level: int) -> int:
    # Level brackets
    if 2 <= level <= 4:
        return 300
    if 5 <= level <= 8:
        return 600
    if 9 <= level <= 12:
        return 800
    if 13 <= level <= 16:
        return 1000
    if 17 <= level <= 20:
        return 1200
    # Outside your chart
    return 0


def gp_per_hour_for_level(level: int) -> int:
    # “gp is based on exact level x10”
    return max(0, int(level)) * 10


def next_hour_threshold_seconds(awarded: int) -> float:
    """
    Seconds required to earn the NEXT reward hour after `awarded`.
    Next threshold is at (awarded+1) hours minus 15 minutes => HH:45:00
    """
    return (awarded + 1) * 3600 - 900


# =========================
# BOT SETUP
# =========================
intents = discord.Intents.default()
intents.guilds = True
bot = commands.Bot(command_prefix="!", intents=intents)


# =========================
# KEEPALIVE WEB SERVER (Railway)
# =========================
async def handle_root(_: web.Request) -> web.Response:
    return web.Response(text="RP Tracker is running.")

async def handle_health(_: web.Request) -> web.Response:
    return web.Response(text="ok")

async def start_web_server():
    port = int(os.getenv("PORT", "8080"))
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"Web server listening on 0.0.0.0:{port}", flush=True)


# =========================
# HELPERS
# =========================
def get_session(message_id: int) -> Tuple[int, Optional[float], Optional[int]]:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT active, started_at, channel_id FROM sessions WHERE message_id=?", (message_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return 0, None, None
    return int(row[0] or 0), (float(row[1]) if row[1] is not None else None), (int(row[2]) if row[2] is not None else None)


def list_participants(message_id: int) -> List[Tuple[int, str, int, float]]:
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        SELECT user_id, character, level, COALESCE(seconds, 0)
        FROM participants
        WHERE message_id=?
        ORDER BY user_id
    """, (message_id,))
    rows = [(int(uid), str(ch), int(lvl), float(secs)) for (uid, ch, lvl, secs) in cur.fetchall()]
    conn.close()
    return rows


def fmt_hms(seconds: float) -> str:
    seconds = max(0.0, seconds)
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def build_embed(message_id: int) -> discord.Embed:
    active, started_at, _ = get_session(message_id)
    participants = list_participants(message_id)

    status = "🟢 Running" if active else "⚪ Not running"
    elapsed = ""
    if active and started_at:
        elapsed = f"\nElapsed: **{fmt_hms(time.time() - started_at)}**"

    embed = discord.Embed(
        title="🎭 RP Tracker",
        description=(
            f"{status}{elapsed}\n\n"
            f"**Reward rule:** Earn 1 hour at **00:45**, 2 hours at **01:45**, etc.\n"
            f"**XP/hr:** by level bracket • **GP/hr:** level × 10"
        )
    )

    if participants:
        lines = []
        for uid, char, lvl, secs in participants:
            awarded = reward_hours(secs)
            xp = xp_per_hour_for_level(lvl) * awarded
            gp = gp_per_hour_for_level(lvl) * awarded

            next_t = next_hour_threshold_seconds(awarded)
            to_next = max(0.0, next_t - secs)
            next_txt = f" • Next hour in **{fmt_hms(to_next)}**" if active else ""

            lines.append(
                f"<@{uid}> — **{char}** (Lv {lvl}) • "
                f"Time: **{fmt_hms(secs)}** • Rewards: **{awarded}h** → "
                f"**{xp} XP** • **{gp} GP**{next_txt}"
            )

        embed.add_field(name="Participants", value="\n".join(lines)[:1024], inline=False)
    else:
        embed.add_field(name="Participants", value="(none yet) — click **Join RP**", inline=False)

    embed.set_footer(text="Join RP → Start RP → End RP")
    return embed


async def update_tracker_message(message_id: int):
    _, _, channel_id = get_session(message_id)
    if not channel_id:
        return

    channel = bot.get_channel(channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(channel_id)
        except Exception:
            return

    try:
        msg = await channel.fetch_message(message_id)
    except Exception:
        return

    view = RPView(message_id)
    await msg.edit(embed=build_embed(message_id), view=view)

    # Register persistent view for restarts
    bot.add_view(view)


# =========================
# TIME TICKER (background)
# =========================
def tick_active_sessions():
    now = time.time()
    conn = db()
    cur = conn.cursor()

    cur.execute("SELECT message_id FROM sessions WHERE active=1")
    session_ids = [int(r[0]) for r in cur.fetchall()]

    for mid in session_ids:
        cur.execute("""
            SELECT user_id, last_tick, seconds
            FROM participants
            WHERE message_id=? AND last_tick IS NOT NULL
        """, (mid,))
        for uid, last_tick, secs in cur.fetchall():
            if last_tick is None:
                continue
            delta = max(0.0, now - float(last_tick))
            new_secs = float(secs or 0) + delta
            cur.execute("""
                UPDATE participants
                SET seconds=?, last_tick=?
                WHERE message_id=? AND user_id=?
            """, (new_secs, now, mid, int(uid)))

    conn.commit()
    conn.close()


async def ticker_loop():
    while True:
        try:
            tick_active_sessions()

            conn = db()
            cur = conn.cursor()
            cur.execute("SELECT message_id FROM sessions WHERE active=1")
            mids = [int(r[0]) for r in cur.fetchall()]
            conn.close()

            for mid in mids:
                await update_tracker_message(mid)

        except Exception:
            print("Ticker loop error:", flush=True)
            traceback.print_exc()

        await asyncio.sleep(15)


# =========================
# JOIN MODAL
# =========================
class JoinModal(discord.ui.Modal, title="Join RP"):
    name = discord.ui.TextInput(label="Character Name", max_length=64)
    level = discord.ui.TextInput(label="Level (1-20)", max_length=3)

    def __init__(self, message_id: int):
        super().__init__()
        self.message_id = message_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            lvl = int(str(self.level.value).strip())
            if not (1 <= lvl <= 20):
                raise ValueError
        except ValueError:
            return await interaction.response.send_message(
                "Level must be a number between 1 and 20.",
                ephemeral=True,
            )

        cname = str(self.name.value).strip()
        if not cname:
            return await interaction.response.send_message("Name can’t be empty.", ephemeral=True)

        conn = db()
        cur = conn.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO participants
            (message_id, user_id, character, level, seconds, last_tick)
            VALUES (?, ?, ?, ?, COALESCE(
                (SELECT seconds FROM participants WHERE message_id=? AND user_id=?), 0
            ), NULL)
        """, (
            self.message_id,
            interaction.user.id,
            cname,
            lvl,
            self.message_id,
            interaction.user.id
        ))
        conn.commit()
        conn.close()

        await interaction.response.send_message(
            f"✅ Joined as **{cname}** (Level {lvl})",
            ephemeral=True
        )

        await update_tracker_message(self.message_id)


# =========================
# VIEW (dynamic buttons with unique custom_id)
# =========================
class RPView(discord.ui.View):
    def __init__(self, message_id: int):
        super().__init__(timeout=None)
        self.message_id = message_id

        join_btn = discord.ui.Button(
            label="Join RP",
            style=discord.ButtonStyle.success,
            custom_id=f"rp_join:{message_id}",
        )
        start_btn = discord.ui.Button(
            label="Start RP",
            style=discord.ButtonStyle.primary,
            custom_id=f"rp_start:{message_id}",
        )
        end_btn = discord.ui.Button(
            label="End RP",
            style=discord.ButtonStyle.danger,
            custom_id=f"rp_end:{message_id}",
        )

        join_btn.callback = self.join_cb
        start_btn.callback = self.start_cb
        end_btn.callback = self.end_cb

        self.add_item(join_btn)
        self.add_item(start_btn)
        self.add_item(end_btn)

    async def join_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(JoinModal(self.message_id))

    async def start_cb(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return

        now = time.time()
        conn = db()
        cur = conn.cursor()

        cur.execute("INSERT OR IGNORE INTO sessions VALUES (?, 0, NULL, ?)", (self.message_id, interaction.channel_id))
        cur.execute("UPDATE sessions SET active=1, started_at=?, channel_id=? WHERE message_id=?",
                    (now, interaction.channel_id, self.message_id))

        cur.execute("UPDATE participants SET last_tick=? WHERE message_id=?", (now, self.message_id))

        conn.commit()
        conn.close()

        await interaction.response.send_message("▶️ RP Started", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def end_cb(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return

        tick_active_sessions()

        conn = db()
        cur = conn.cursor()
        cur.execute("UPDATE sessions SET active=0, started_at=NULL WHERE message_id=?", (self.message_id,))
        cur.execute("UPDATE participants SET last_tick=NULL WHERE message_id=?", (self.message_id,))
        conn.commit()
        conn.close()

        parts = list_participants(self.message_id)
        lines = []
        for uid, char, lvl, secs in parts:
            awarded = reward_hours(secs)
            xp = xp_per_hour_for_level(lvl) * awarded
            gp = gp_per_hour_for_level(lvl) * awarded
            lines.append(f"<@{uid}> — **{char}** (Lv {lvl}) → **{awarded}h**: {xp} XP | {gp} GP")

        await interaction.response.send_message(
            "**🏁 RP Session Ended**\n" + ("\n".join(lines) if lines else "(no participants)")
        )
        await update_tracker_message(self.message_id)


# =========================
# SLASH COMMAND
# =========================
@bot.tree.command(name="post_rp_tracker", description="Post an RP tracker with Join/Start/End buttons.")
async def post_tracker(interaction: discord.Interaction):
    await interaction.response.send_message(embed=discord.Embed(title="🎭 RP Tracker", description="Creating tracker…"))
    msg = await interaction.original_response()

    conn = db()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO sessions VALUES (?, 0, NULL, ?)", (msg.id, msg.channel.id))
    cur.execute("UPDATE sessions SET channel_id=? WHERE message_id=?", (msg.channel.id, msg.id))
    conn.commit()
    conn.close()

    view = RPView(msg.id)
    await msg.edit(embed=build_embed(msg.id), view=view)
    bot.add_view(view)


# =========================
# ERROR HANDLER
# =========================
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: Exception):
    original = getattr(error, "original", None)
    shown = original if original else error

    print("Slash command error:", repr(shown), flush=True)
    traceback.print_exception(type(shown), shown, shown.__traceback__)

    msg = f"❌ Error: `{type(shown).__name__}` — {shown}"
    try:
        if interaction.response.is_done():
            await interaction.followup.send(msg[:1900], ephemeral=True)
        else:
            await interaction.response.send_message(msg[:1900], ephemeral=True)
    except Exception:
        pass


# =========================
# READY
# =========================
@bot.event
async def on_ready():
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT message_id FROM sessions")
    msg_ids = [int(r[0]) for r in cur.fetchall()]
    conn.close()

    for mid in msg_ids:
        bot.add_view(RPView(mid))

    try:
        await bot.tree.sync()
        print("Slash commands synced.", flush=True)
    except Exception as e:
        print("Command sync failed:", repr(e), flush=True)
        traceback.print_exc()

    print(f"Logged in as {bot.user} (guilds={len(bot.guilds)})", flush=True)


# =========================
# MAIN
# =========================
async def main():
    await start_web_server()
    asyncio.create_task(ticker_loop())
    await bot.start(TOKEN)

if __name__ == "__main__":
    asyncio.run(main())
