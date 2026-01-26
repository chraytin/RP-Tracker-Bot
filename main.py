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

    # sessions:
    # state: 0=stopped, 1=running, 2=paused
    # started_at: when running began (for current run segment)
    # run_seconds: accumulated running time across start/pause/continue
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
            PRIMARY KEY (message_id, user_id)
        )
    """)

    # Migrations for older DBs
    cur.execute("PRAGMA table_info(sessions)")
    cols = {row[1] for row in cur.fetchall()}

    if "active" in cols and "state" not in cols:
        cur.execute("ALTER TABLE sessions ADD COLUMN state INTEGER")
        cur.execute("UPDATE sessions SET state = CASE WHEN active=1 THEN 1 ELSE 0 END WHERE state IS NULL")

    if "channel_id" not in cols:
        cur.execute("ALTER TABLE sessions ADD COLUMN channel_id INTEGER")

    if "run_seconds" not in cols:
        cur.execute("ALTER TABLE sessions ADD COLUMN run_seconds REAL")
        cur.execute("UPDATE sessions SET run_seconds = COALESCE(run_seconds, 0)")

    if "started_at" not in cols:
        cur.execute("ALTER TABLE sessions ADD COLUMN started_at REAL")

    cur.execute("UPDATE sessions SET state = COALESCE(state, 0)")
    cur.execute("UPDATE sessions SET run_seconds = COALESCE(run_seconds, 0)")

    conn.commit()
    conn.close()

ensure_schema()

# =========================
# REWARD RULES
# =========================
def reward_hours(seconds: float) -> int:
    """0h until 00:45:00, 1h at 00:45, 2h at 01:45, etc."""
    return int((max(0.0, seconds) + 900) // 3600)

def xp_per_hour_for_level(level: int) -> int:
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
    return 0

def gp_per_hour_for_level(level: int) -> int:
    return max(0, int(level)) * 10

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
def get_session(message_id: int) -> Tuple[int, Optional[float], float, Optional[int]]:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT state, started_at, COALESCE(run_seconds,0), channel_id FROM sessions WHERE message_id=?", (message_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return 0, None, 0.0, None
    state = int(row[0] or 0)
    started_at = float(row[1]) if row[1] is not None else None
    run_seconds = float(row[2] or 0.0)
    channel_id = int(row[3]) if row[3] is not None else None
    return state, started_at, run_seconds, channel_id

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

def session_elapsed_seconds(message_id: int) -> float:
    state, started_at, run_seconds, _ = get_session(message_id)
    if state == 1 and started_at is not None:
        return run_seconds + max(0.0, time.time() - started_at)
    return run_seconds

def fmt_hm(seconds: float) -> str:
    seconds = max(0.0, seconds)
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    return f"{h}h {m}m"

def build_embed(message_id: int) -> discord.Embed:
    elapsed = session_elapsed_seconds(message_id)
    parts = list_participants(message_id)

    embed = discord.Embed(
        title=f"RP Session - Time: {fmt_hm(elapsed)}",
        description="Players Joined:"
    )

    if parts:
        lines = [f"<@{uid}> - {char} (lvl {lvl})" for uid, char, lvl, _ in parts]
        embed.add_field(name="\u200b", value="\n".join(lines)[:1024], inline=False)
    else:
        embed.add_field(name="\u200b", value="(none yet)", inline=False)

    return embed

async def update_tracker_message(message_id: int):
    _, _, _, channel_id = get_session(message_id)
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

    # Persistent view registration for restarts
    bot.add_view(view)

# =========================
# TIME TICKER (background)
# =========================
def tick_running_sessions():
    """
    Update participants.seconds for sessions that are RUNNING.
    Only participants with last_tick NOT NULL accrue time.
    """
    now = time.time()
    conn = db()
    cur = conn.cursor()

    cur.execute("SELECT message_id FROM sessions WHERE state=1")
    running_ids = [int(r[0]) for r in cur.fetchall()]

    for mid in running_ids:
        cur.execute("""
            SELECT user_id, last_tick, seconds
            FROM participants
            WHERE message_id=? AND last_tick IS NOT NULL
        """, (mid,))
        for uid, last_tick, secs in cur.fetchall():
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
            tick_running_sessions()

            conn = db()
            cur = conn.cursor()
            cur.execute("SELECT message_id FROM sessions WHERE state=1")
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

        # Preserve accumulated seconds if editing
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

        # If session is running, auto-start their personal timer now
        state, _, _, _ = get_session(self.message_id)
        if state == 1:
            now = time.time()
            cur.execute("""
                UPDATE participants SET last_tick=?
                WHERE message_id=? AND user_id=?
            """, (now, self.message_id, interaction.user.id))

        conn.commit()
        conn.close()

        await interaction.response.send_message(
            f"✅ Joined as **{cname}** (lvl {lvl})",
            ephemeral=True
        )

        await update_tracker_message(self.message_id)

# =========================
# VIEW (buttons + Leave/Rejoin)
# =========================
class RPView(discord.ui.View):
    def __init__(self, message_id: int):
        super().__init__(timeout=None)
        self.message_id = message_id

        # Row 0
        self.join_btn = discord.ui.Button(
            label="Join RP", style=discord.ButtonStyle.success,
            custom_id=f"rp_join:{message_id}"
        )
        self.start_btn = discord.ui.Button(
            label="Start RP", style=discord.ButtonStyle.primary,
            custom_id=f"rp_start:{message_id}"
        )
        self.pause_btn = discord.ui.Button(
            label="Pause RP", style=discord.ButtonStyle.secondary,
            custom_id=f"rp_pause:{message_id}"
        )

        # Row 1
        self.continue_btn = discord.ui.Button(
            label="Continue RP", style=discord.ButtonStyle.success,
            custom_id=f"rp_continue:{message_id}"
        )
        self.end_btn = discord.ui.Button(
            label="End RP", style=discord.ButtonStyle.danger,
            custom_id=f"rp_end:{message_id}"
        )

        # Row 2 (per-player controls)
        self.leave_btn = discord.ui.Button(
            label="Leave RP", style=discord.ButtonStyle.secondary,
            custom_id=f"rp_leave:{message_id}"
        )
        self.rejoin_btn = discord.ui.Button(
            label="Rejoin RP", style=discord.ButtonStyle.secondary,
            custom_id=f"rp_rejoin:{message_id}"
        )

        self.join_btn.callback = self.join_cb
        self.start_btn.callback = self.start_cb
        self.pause_btn.callback = self.pause_cb
        self.continue_btn.callback = self.continue_cb
        self.end_btn.callback = self.end_cb
        self.leave_btn.callback = self.leave_cb
        self.rejoin_btn.callback = self.rejoin_cb

        self.add_item(self.join_btn)
        self.add_item(self.start_btn)
        self.add_item(self.pause_btn)

        self.continue_btn.row = 1
        self.end_btn.row = 1
        self.add_item(self.continue_btn)
        self.add_item(self.end_btn)

        self.leave_btn.row = 2
        self.rejoin_btn.row = 2
        self.add_item(self.leave_btn)
        self.add_item(self.rejoin_btn)

    async def join_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(JoinModal(self.message_id))

    async def start_cb(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return

        state, _, run_seconds, _ = get_session(self.message_id)
        if state == 1:
            await interaction.response.send_message("Already running.", ephemeral=True)
            return

        now = time.time()
        conn = db()
        cur = conn.cursor()

        cur.execute(
            "INSERT OR IGNORE INTO sessions (message_id, state, started_at, run_seconds, channel_id) VALUES (?, 0, NULL, 0, ?)",
            (self.message_id, interaction.channel_id)
        )

        cur.execute(
            "UPDATE sessions SET state=1, started_at=?, channel_id=?, run_seconds=? WHERE message_id=?",
            (now, interaction.channel_id, float(run_seconds or 0.0), self.message_id)
        )

        # Everyone currently participating begins accruing time
        cur.execute("UPDATE participants SET last_tick=? WHERE message_id=?", (now, self.message_id))

        conn.commit()
        conn.close()

        await interaction.response.send_message("▶️ RP Started", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def pause_cb(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return

        state, started_at, run_seconds, _ = get_session(self.message_id)
        if state != 1 or started_at is None:
            await interaction.response.send_message("Not currently running.", ephemeral=True)
            return

        tick_running_sessions()

        now = time.time()
        add = max(0.0, now - started_at)
        new_run = float(run_seconds or 0.0) + add

        conn = db()
        cur = conn.cursor()
        cur.execute("UPDATE sessions SET state=2, started_at=NULL, run_seconds=? WHERE message_id=?", (new_run, self.message_id))
        # Stop everyone (session pause)
        cur.execute("UPDATE participants SET last_tick=NULL WHERE message_id=?", (self.message_id,))
        conn.commit()
        conn.close()

        await interaction.response.send_message("⏸️ RP Paused", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def continue_cb(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return

        state, _, run_seconds, _ = get_session(self.message_id)
        if state != 2:
            await interaction.response.send_message("Not currently paused.", ephemeral=True)
            return

        now = time.time()
        conn = db()
        cur = conn.cursor()
        cur.execute("UPDATE sessions SET state=1, started_at=?, run_seconds=? WHERE message_id=?",
                    (now, float(run_seconds or 0.0), self.message_id))

        # Resume only for players who have NOT "left":
        # We'll resume timers for everyone who has joined (seconds exists),
        # but NOT for people who previously clicked Leave RP and are currently last_tick NULL.
        # However pause sets all last_tick NULL, so we need a separate rule:
        # We'll resume for everyone, because session is continuing and the player can choose Leave afterward.
        cur.execute("UPDATE participants SET last_tick=? WHERE message_id=?", (now, self.message_id))

        conn.commit()
        conn.close()

        await interaction.response.send_message("▶️ RP Continued", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def leave_cb(self, interaction: discord.Interaction):
        """
        Stops time tracking ONLY for the clicker.
        Does not remove them from the participants list or erase seconds.
        """
        # First, tick once so they get credit up to now if currently accruing
        now = time.time()
        conn = db()
        cur = conn.cursor()

        cur.execute("""
            SELECT last_tick, seconds FROM participants
            WHERE message_id=? AND user_id=?
        """, (self.message_id, interaction.user.id))
        row = cur.fetchone()

        if not row:
            conn.close()
            await interaction.response.send_message("You haven’t joined this RP yet.", ephemeral=True)
            return

        last_tick, secs = row[0], float(row[1] or 0.0)

        # If they were accruing, add time up to now, then stop (last_tick NULL)
        if last_tick is not None:
            delta = max(0.0, now - float(last_tick))
            secs += delta

        cur.execute("""
            UPDATE participants
            SET seconds=?, last_tick=NULL
            WHERE message_id=? AND user_id=?
        """, (secs, self.message_id, interaction.user.id))

        conn.commit()
        conn.close()

        await interaction.response.send_message("⏹️ You left RP. Your timer is paused for you only.", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def rejoin_cb(self, interaction: discord.Interaction):
        """
        Restarts time tracking ONLY for the clicker (if session is running).
        """
        state, _, _, _ = get_session(self.message_id)

        conn = db()
        cur = conn.cursor()
        cur.execute("""
            SELECT 1 FROM participants WHERE message_id=? AND user_id=?
        """, (self.message_id, interaction.user.id))
        exists = cur.fetchone() is not None

        if not exists:
            conn.close()
            await interaction.response.send_message("You haven’t joined this RP yet. Click **Join RP** first.", ephemeral=True)
            return

        if state != 1:
            # Session not running: keep last_tick NULL; they will start accruing when Start/Continue happens
            cur.execute("""
                UPDATE participants SET last_tick=NULL
                WHERE message_id=? AND user_id=?
            """, (self.message_id, interaction.user.id))
            conn.commit()
            conn.close()
            await interaction.response.send_message("You rejoined, but RP isn’t running yet.", ephemeral=True)
            await update_tracker_message(self.message_id)
            return

        now = time.time()
        cur.execute("""
            UPDATE participants SET last_tick=?
            WHERE message_id=? AND user_id=?
        """, (now, self.message_id, interaction.user.id))

        conn.commit()
        conn.close()

        await interaction.response.send_message("▶️ You rejoined RP. Your timer is running again.", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def end_cb(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return

        state, started_at, run_seconds, _ = get_session(self.message_id)

        # Final tick for anyone still accruing
        if state == 1 and started_at is not None:
            tick_running_sessions()
            now = time.time()
            run_seconds = float(run_seconds or 0.0) + max(0.0, now - started_at)

        conn = db()
        cur = conn.cursor()
        cur.execute("UPDATE sessions SET state=0, started_at=NULL, run_seconds=? WHERE message_id=?",
                    (float(run_seconds or 0.0), self.message_id))
        cur.execute("UPDATE participants SET last_tick=NULL WHERE message_id=?", (self.message_id,))
        conn.commit()
        conn.close()

        parts = list_participants(self.message_id)
        lines = []
        for uid, char, lvl, secs in parts:
            awarded = reward_hours(secs)
            xp = xp_per_hour_for_level(lvl) * awarded
            gp = gp_per_hour_for_level(lvl) * awarded
            lines.append(f"<@{uid}> — **{char}** (lvl {lvl}) → **{awarded}h**: {xp} XP | {gp} GP")

        await interaction.response.send_message(
            "**🏁 RP Ended — Rewards**\n" + ("\n".join(lines) if lines else "(no participants)")
        )
        await update_tracker_message(self.message_id)

# =========================
# SLASH COMMAND
# =========================
@bot.tree.command(name="post_rp_tracker", description="Post an RP tracker with buttons.")
async def post_tracker(interaction: discord.Interaction):
    await interaction.response.send_message(embed=discord.Embed(title="RP Tracker", description="Creating…"))
    msg = await interaction.original_response()

    conn = db()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO sessions (message_id, state, started_at, run_seconds, channel_id) VALUES (?, 0, NULL, 0, ?)",
        (msg.id, msg.channel.id)
    )
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
    # Re-register persistent views for existing trackers
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
