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
    # started_at: when running began (current segment)
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

    # participants:
    # seconds: total accrued time for this participant in this session
    # last_tick: if not NULL, they are currently accruing time (personal timer running)
    # capped: 0/1 (if 1, earn 🗝️ per hour instead of XP; GP still applies)
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

    # Migrations for older DBs
    cur.execute("PRAGMA table_info(sessions)")
    scols = {row[1] for row in cur.fetchall()}

    if "active" in scols and "state" not in scols:
        cur.execute("ALTER TABLE sessions ADD COLUMN state INTEGER")
        cur.execute("UPDATE sessions SET state = CASE WHEN active=1 THEN 1 ELSE 0 END WHERE state IS NULL")

    if "channel_id" not in scols:
        cur.execute("ALTER TABLE sessions ADD COLUMN channel_id INTEGER")

    if "run_seconds" not in scols:
        cur.execute("ALTER TABLE sessions ADD COLUMN run_seconds REAL")
        cur.execute("UPDATE sessions SET run_seconds = COALESCE(run_seconds, 0)")

    if "started_at" not in scols:
        cur.execute("ALTER TABLE sessions ADD COLUMN started_at REAL")

    cur.execute("UPDATE sessions SET state = COALESCE(state, 0)")
    cur.execute("UPDATE sessions SET run_seconds = COALESCE(run_seconds, 0)")

    # Participants migration: add capped if missing
    cur.execute("PRAGMA table_info(participants)")
    pcols = {row[1] for row in cur.fetchall()}
    if "capped" not in pcols:
        cur.execute("ALTER TABLE participants ADD COLUMN capped INTEGER")
        cur.execute("UPDATE participants SET capped = COALESCE(capped, 0)")

    conn.commit()
    conn.close()

ensure_schema()

# =========================
# REWARD RULES
# =========================
def reward_hours(seconds: float) -> int:
    """
    Whole-hour rewards with a 45-minute threshold.
    0h until 00:45:00, 1h at 00:45, 2h at 01:45, etc.
    """
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
# THEME (Adventurer's Guild vibe)
# =========================
def theme_color() -> discord.Color:
    # Default: warm "guild ledger" gold
    raw = os.getenv("THEME_COLOR", "#C9A227").lstrip("#")
    try:
        return discord.Color(int(raw, 16))
    except Exception:
        return discord.Color.gold()

def apply_theme(embed: discord.Embed) -> discord.Embed:
    embed.color = theme_color()

    # Optional theme art
    thumb = os.getenv("THEME_THUMBNAIL_URL")  # e.g., guild crest
    if thumb:
        embed.set_thumbnail(url=thumb)

    banner = os.getenv("THEME_BANNER_URL")  # e.g., parchment/banner strip
    if banner:
        embed.set_image(url=banner)

    # Author + footer defaults
    guild_name = os.getenv("THEME_NAME", "Adventurer’s Guild Ledger")
    embed.set_author(name=guild_name)

    footer_text = os.getenv("THEME_FOOTER_TEXT", "Stamped & filed by the Guild Registrar • Session rewards awarded on 45-minute marks")
    footer_icon = os.getenv("THEME_FOOTER_ICON_URL")
    if footer_icon:
        embed.set_footer(text=footer_text, icon_url=footer_icon)
    else:
        embed.set_footer(text=footer_text)

    return embed

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
    cur.execute(
        "SELECT state, started_at, COALESCE(run_seconds,0), channel_id FROM sessions WHERE message_id=?",
        (message_id,)
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return 0, None, 0.0, None
    state = int(row[0] or 0)
    started_at = float(row[1]) if row[1] is not None else None
    run_seconds = float(row[2] or 0.0)
    channel_id = int(row[3]) if row[3] is not None else None
    return state, started_at, run_seconds, channel_id

def list_participants(message_id: int) -> List[Tuple[int, str, int, float, int]]:
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        SELECT user_id, character, level, COALESCE(seconds, 0), COALESCE(capped, 0)
        FROM participants
        WHERE message_id=?
        ORDER BY user_id
    """, (message_id,))
    rows = [(int(uid), str(ch), int(lvl), float(secs), int(cap)) for (uid, ch, lvl, secs, cap) in cur.fetchall()]
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

def state_label(state: int) -> str:
    if state == 1:
        return "🟢 Active"
    if state == 2:
        return "🟡 Paused"
    return "⚪ Not Started"

def build_embed(message_id: int) -> discord.Embed:
    state, _, _, _ = get_session(message_id)
    elapsed = session_elapsed_seconds(message_id)
    parts = list_participants(message_id)

    embed = discord.Embed(
        title="📜 Adventurer’s Guild — RP Session Log",
        description="The Grandmaster’s record of attendance and session time."
    )

    embed.add_field(name="Status", value=state_label(state), inline=True)
    embed.add_field(name="Session Time", value=f"⏳ **{fmt_hm(elapsed)}**", inline=True)

    if parts:
        roster_lines = [f"<@{uid}> — **{char}** (lvl {lvl})" for uid, char, lvl, _secs, _cap in parts]
        roster = "\n".join(roster_lines)
    else:
        roster = "*No adventurers signed in yet.*"

    embed.add_field(name="Roster", value=roster[:1024], inline=False)

    return apply_theme(embed)

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
class JoinModal(discord.ui.Modal, title="Adventurer Sign-In"):
    name = discord.ui.TextInput(label="Character Name", max_length=64)
    level = discord.ui.TextInput(label="Level (1-20)", max_length=3)
    capped = discord.ui.TextInput(
        label="Capped? (yes/no)",
        required=False,
        max_length=5,
        placeholder="no"
    )

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

        cap_raw = (str(self.capped.value).strip().lower() if self.capped.value else "no")
        is_capped = 1 if cap_raw in ("y", "yes", "true", "1", "cap", "capped") else 0

        conn = db()
        cur = conn.cursor()

        # Preserve accumulated seconds if editing
        cur.execute("""
            INSERT OR REPLACE INTO participants
            (message_id, user_id, character, level, seconds, last_tick, capped)
            VALUES (?, ?, ?, ?, COALESCE(
                (SELECT seconds FROM participants WHERE message_id=? AND user_id=?), 0
            ), NULL, ?)
        """, (
            self.message_id,
            interaction.user.id,
            cname,
            lvl,
            self.message_id,
            interaction.user.id,
            is_capped
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

        cap_txt = " (Capped: 🗝️/hr)" if is_capped else ""
        await interaction.response.send_message(
            f"✅ Signed in: **{cname}** (lvl {lvl}){cap_txt}",
            ephemeral=True
        )

        await update_tracker_message(self.message_id)

# =========================
# VIEW (buttons: labels + rows)
# =========================
class RPView(discord.ui.View):
    def __init__(self, message_id: int):
        super().__init__(timeout=None)
        self.message_id = message_id

        # Row 0: player actions
        self.join_btn = discord.ui.Button(
            label="✅ Join", style=discord.ButtonStyle.success,
            custom_id=f"rp_join:{message_id}", row=0
        )
        self.leave_btn = discord.ui.Button(
            label="⏹ Leave", style=discord.ButtonStyle.secondary,
            custom_id=f"rp_leave:{message_id}", row=0
        )
        self.rejoin_btn = discord.ui.Button(
            label="🔁 Rejoin", style=discord.ButtonStyle.secondary,
            custom_id=f"rp_rejoin:{message_id}", row=0
        )

        # Row 1: DM actions
        self.start_btn = discord.ui.Button(
            label="▶️ Start", style=discord.ButtonStyle.primary,
            custom_id=f"rp_start:{message_id}", row=1
        )
        self.pause_btn = discord.ui.Button(
            label="⏸ Pause", style=discord.ButtonStyle.secondary,
            custom_id=f"rp_pause:{message_id}", row=1
        )
        self.resume_btn = discord.ui.Button(
            label="⏵ Resume", style=discord.ButtonStyle.success,
            custom_id=f"rp_resume:{message_id}", row=1
        )

        # Row 2: DM only
        self.end_btn = discord.ui.Button(
            label="🏁 End", style=discord.ButtonStyle.danger,
            custom_id=f"rp_end:{message_id}", row=2
        )

        self.join_btn.callback = self.join_cb
        self.leave_btn.callback = self.leave_cb
        self.rejoin_btn.callback = self.rejoin_cb
        self.start_btn.callback = self.start_cb
        self.pause_btn.callback = self.pause_cb
        self.resume_btn.callback = self.resume_cb
        self.end_btn.callback = self.end_cb

        self.add_item(self.join_btn)
        self.add_item(self.leave_btn)
        self.add_item(self.rejoin_btn)
        self.add_item(self.start_btn)
        self.add_item(self.pause_btn)
        self.add_item(self.resume_btn)
        self.add_item(self.end_btn)

    # -------- Player buttons --------
    async def join_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(JoinModal(self.message_id))

    async def leave_cb(self, interaction: discord.Interaction):
        """Stop time tracking ONLY for the clicker."""
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

        await interaction.response.send_message("⏹ You’ve been signed out (timer paused for you only).", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def rejoin_cb(self, interaction: discord.Interaction):
        """Restart time tracking ONLY for the clicker if session is running."""
        state, _, _, _ = get_session(self.message_id)

        conn = db()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM participants WHERE message_id=? AND user_id=?",
                    (self.message_id, interaction.user.id))
        exists = cur.fetchone() is not None

        if not exists:
            conn.close()
            await interaction.response.send_message("You haven’t joined yet. Click **✅ Join** first.", ephemeral=True)
            return

        if state != 1:
            cur.execute("UPDATE participants SET last_tick=NULL WHERE message_id=? AND user_id=?",
                        (self.message_id, interaction.user.id))
            conn.commit()
            conn.close()
            await interaction.response.send_message("You’re marked present, but the session isn’t running.", ephemeral=True)
            await update_tracker_message(self.message_id)
            return

        now = time.time()
        cur.execute("UPDATE participants SET last_tick=? WHERE message_id=? AND user_id=?",
                    (now, self.message_id, interaction.user.id))

        conn.commit()
        conn.close()

        await interaction.response.send_message("🔁 You’re back in. Your timer is running again.", ephemeral=True)
        await update_tracker_message(self.message_id)

    # -------- DM buttons --------
    def _require_dm(self, interaction: discord.Interaction) -> bool:
        return interaction.user.guild_permissions.manage_guild

    async def start_cb(self, interaction: discord.Interaction):
        if not self._require_dm(interaction):
            await interaction.response.send_message("DM/Staff only.", ephemeral=True)
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

        # Start accruing for everyone currently participating
        cur.execute("UPDATE participants SET last_tick=? WHERE message_id=?", (now, self.message_id))

        conn.commit()
        conn.close()

        await interaction.response.send_message("▶️ Session started. The guild clock is running.", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def pause_cb(self, interaction: discord.Interaction):
        if not self._require_dm(interaction):
            await interaction.response.send_message("DM/Staff only.", ephemeral=True)
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
        cur.execute("UPDATE participants SET last_tick=NULL WHERE message_id=?", (self.message_id,))
        conn.commit()
        conn.close()

        await interaction.response.send_message("⏸ Session paused. Quills down.", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def resume_cb(self, interaction: discord.Interaction):
        if not self._require_dm(interaction):
            await interaction.response.send_message("DM/Staff only.", ephemeral=True)
            return

        state, _, run_seconds, _ = get_session(self.message_id)
        if state != 2:
            await interaction.response.send_message("Not currently paused.", ephemeral=True)
            return

        now = time.time()
        conn = db()
        cur = conn.cursor()
        cur.execute(
            "UPDATE sessions SET state=1, started_at=?, run_seconds=? WHERE message_id=?",
            (now, float(run_seconds or 0.0), self.message_id)
        )
        # Resume for everyone; individual players can opt out via Leave
        cur.execute("UPDATE participants SET last_tick=? WHERE message_id=?", (now, self.message_id))

        conn.commit()
        conn.close()

        await interaction.response.send_message("⏵ Session resumed. The guild clock continues.", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def end_cb(self, interaction: discord.Interaction):
        if not self._require_dm(interaction):
            await interaction.response.send_message("DM/Staff only.", ephemeral=True)
            return

        state, started_at, run_seconds, _ = get_session(self.message_id)

        # Final tick for anyone still accruing
        if state == 1 and started_at is not None:
            tick_running_sessions()
            now = time.time()
            run_seconds = float(run_seconds or 0.0) + max(0.0, now - started_at)

        conn = db()
        cur = conn.cursor()
        cur.execute(
            "UPDATE sessions SET state=0, started_at=NULL, run_seconds=? WHERE message_id=?",
            (float(run_seconds or 0.0), self.message_id)
        )
        cur.execute("UPDATE participants SET last_tick=NULL WHERE message_id=?", (self.message_id,))
        conn.commit()
        conn.close()

        # Rewards summary (XP or 🗝️, plus GP always)
        parts = list_participants(self.message_id)
        lines = []
        for uid, char, lvl, secs, cap in parts:
            awarded = reward_hours(secs)
            gp = gp_per_hour_for_level(lvl) * awarded

            if cap:
                keys = awarded  # 1 🗝️ per rewarded hour
                lines.append(f"<@{uid}> — **{char}** (lvl {lvl}) → **{awarded}h**: 🗝️ {keys} | {gp} GP")
            else:
                xp = xp_per_hour_for_level(lvl) * awarded
                lines.append(f"<@{uid}> — **{char}** (lvl {lvl}) → **{awarded}h**: {xp} XP | {gp} GP")

        await interaction.response.send_message(
            "🏁 **Guild Ledger Closed — Rewards Issued**\n" + ("\n".join(lines) if lines else "(no participants)")
        )
        await update_tracker_message(self.message_id)

# =========================
# SLASH COMMAND
# =========================
@bot.tree.command(name="post_rp_tracker", description="Post an Adventurer’s Guild RP tracker.")
async def post_tracker(interaction: discord.Interaction):
    # Post initial embed
    temp = apply_theme(discord.Embed(title="📜 Opening a new Guild Ledger…", description="Preparing the session log."))
    await interaction.response.send_message(embed=temp)
    msg = await interaction.original_response()

    # Store session
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO sessions (message_id, state, started_at, run_seconds, channel_id) VALUES (?, 0, NULL, 0, ?)",
        (msg.id, msg.channel.id)
    )
    cur.execute("UPDATE sessions SET channel_id=? WHERE message_id=?", (msg.channel.id, msg.id))
    conn.commit()
    conn.close()

    # Attach view and themed embed
    view = RPView(msg.id)
    await msg.edit(embed=build_embed(msg.id), view=view)
    bot.add_view(view)

    # Pin the message (requires Manage Messages in that channel/thread)
    try:
        await msg.pin(reason="Adventurer’s Guild RP Tracker")
    except discord.Forbidden:
        # If missing permissions, don't fail the command
        await interaction.followup.send(
            "ℹ️ I couldn’t pin the tracker (missing **Manage Messages** permission here).",
            ephemeral=True
        )
    except discord.HTTPException:
        pass

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
