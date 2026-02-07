import os
import time
import asyncio
import traceback
from typing import Optional, List, Tuple

import psycopg
import discord
from discord.ext import commands
from aiohttp import web

print("Booting RP Tracker...", flush=True)

# =========================
# CONFIG
# =========================
TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is not set. Add it in Railway → Variables.")

def db():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set. In Railway, add a Variable Reference from the Postgres service into the RP-Tracker-Bot service."
        )
    return psycopg.connect(database_url)

# =========================
# DATABASE + SCHEMA (Postgres)
# =========================
def ensure_schema():
    with db() as conn:
        with conn.cursor() as cur:
            # Core tables
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    message_id BIGINT PRIMARY KEY,
                    state INT,
                    started_at DOUBLE PRECISION,
                    run_seconds DOUBLE PRECISION,
                    channel_id BIGINT,
                    guild_id BIGINT
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS participants (
                    message_id BIGINT,
                    user_id BIGINT,
                    character TEXT,
                    level INT,
                    seconds DOUBLE PRECISION,
                    last_tick DOUBLE PRECISION,
                    capped INT DEFAULT 0,
                    xp_dip INT DEFAULT 0,
                    gp_dip INT DEFAULT 0,
                    PRIMARY KEY (message_id, user_id)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS keys (
                    guild_id BIGINT,
                    user_id BIGINT,
                    current INT DEFAULT 0,
                    lifetime INT DEFAULT 0,
                    PRIMARY KEY (guild_id, user_id)
                )
            """)

            # Migrations (safe adds)
            cur.execute("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS guild_id BIGINT")
            cur.execute("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS run_seconds DOUBLE PRECISION")
            cur.execute("ALTER TABLE participants ADD COLUMN IF NOT EXISTS capped INT DEFAULT 0")
            cur.execute("ALTER TABLE participants ADD COLUMN IF NOT EXISTS last_tick DOUBLE PRECISION")
            cur.execute("ALTER TABLE participants ADD COLUMN IF NOT EXISTS xp_dip INT DEFAULT 0")
            cur.execute("ALTER TABLE participants ADD COLUMN IF NOT EXISTS gp_dip INT DEFAULT 0")

            # Ensure NULLs become 0 where expected
            cur.execute("UPDATE sessions SET run_seconds = COALESCE(run_seconds, 0) WHERE run_seconds IS NULL")
            cur.execute("UPDATE participants SET seconds = COALESCE(seconds, 0) WHERE seconds IS NULL")
            cur.execute("UPDATE participants SET capped = COALESCE(capped, 0) WHERE capped IS NULL")
            cur.execute("UPDATE participants SET xp_dip = COALESCE(xp_dip, 0) WHERE xp_dip IS NULL")
            cur.execute("UPDATE participants SET gp_dip = COALESCE(gp_dip, 0) WHERE gp_dip IS NULL")

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
# THEME
# =========================
def theme_color() -> discord.Color:
    raw = os.getenv("THEME_COLOR", "#C9A227").lstrip("#")
    try:
        return discord.Color(int(raw, 16))
    except Exception:
        return discord.Color.gold()

def apply_theme(embed: discord.Embed, *, footer_text_override: Optional[str] = None) -> discord.Embed:
    embed.color = theme_color()

    thumb = os.getenv("THEME_THUMBNAIL_URL")
    if thumb:
        embed.set_thumbnail(url=thumb)

    banner = os.getenv("THEME_BANNER_URL")
    if banner:
        embed.set_image(url=banner)

    guild_name = os.getenv("THEME_NAME", "Adventurer’s Guild Ledger")
    embed.set_author(name=guild_name)

    footer_text = footer_text_override if footer_text_override is not None else os.getenv(
        "THEME_FOOTER_TEXT",
        "Stamped & filed by the Guild Registrar • Rewards granted on 45-minute marks"
    )

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
intents.message_content = True  # for !key
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
# KEY LEDGER HELPERS
# =========================
def keys_get(guild_id: int, user_id: int) -> Tuple[int, int]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current, lifetime FROM keys WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
            row = cur.fetchone()
    if not row:
        return (0, 0)
    return (int(row[0] or 0), int(row[1] or 0))

def keys_add(guild_id: int, user_id: int, amount: int):
    amount = int(amount)
    if amount <= 0:
        return
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO keys (guild_id, user_id, current, lifetime)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (guild_id, user_id)
                DO UPDATE SET
                    current = keys.current + EXCLUDED.current,
                    lifetime = keys.lifetime + EXCLUDED.lifetime
            """, (guild_id, user_id, amount, amount))

def keys_sub(guild_id: int, user_id: int, amount: int):
    amount = int(amount)
    if amount <= 0:
        return
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO keys (guild_id, user_id, current, lifetime)
                VALUES (%s, %s, 0, 0)
                ON CONFLICT (guild_id, user_id) DO NOTHING
            """, (guild_id, user_id))
            cur.execute("""
                UPDATE keys
                SET current = GREATEST(current - %s, 0)
                WHERE guild_id=%s AND user_id=%s
            """, (amount, guild_id, user_id))

def build_key_embed(member: discord.Member, current: int, lifetime: int) -> discord.Embed:
    title = f"🗝️ {member.display_name}'s Keyring"
    embed = discord.Embed(title=title, description="", color=theme_color())
    embed.add_field(name="Current Keys", value=str(current), inline=False)
    embed.add_field(name="Lifetime Keys", value=str(lifetime), inline=False)

    key_thumb = os.getenv("KEY_THUMBNAIL_URL") or os.getenv("THEME_THUMBNAIL_URL")
    if key_thumb:
        embed.set_thumbnail(url=key_thumb)

    return apply_theme(embed, footer_text_override="Stamped & filed by the Guild Registrar")

# =========================
# HELPERS (SESSIONS)
# =========================
def get_session(message_id: int) -> Tuple[int, Optional[float], float, Optional[int], Optional[int]]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT state, started_at, COALESCE(run_seconds,0), channel_id, guild_id FROM sessions WHERE message_id=%s",
                (message_id,)
            )
            row = cur.fetchone()

    if not row:
        return 0, None, 0.0, None, None

    state = int(row[0] or 0)
    started_at = float(row[1]) if row[1] is not None else None
    run_seconds = float(row[2] or 0.0)
    channel_id = int(row[3]) if row[3] is not None else None
    guild_id = int(row[4]) if row[4] is not None else None
    return state, started_at, run_seconds, channel_id, guild_id

def list_participants(message_id: int) -> List[Tuple[int, str, int, float, int, int, int]]:
    """
    Returns:
      (user_id, character, level, seconds, capped, xp_dip, gp_dip)
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT user_id, character, level, COALESCE(seconds, 0), COALESCE(capped, 0),
                       COALESCE(xp_dip, 0), COALESCE(gp_dip, 0)
                FROM participants
                WHERE message_id=%s
                ORDER BY user_id
            """, (message_id,))
            rows = cur.fetchall()

    return [(int(uid), str(ch), int(lvl), float(secs), int(cap), int(xd), int(gd)) for (uid, ch, lvl, secs, cap, xd, gd) in rows]

def session_elapsed_seconds(message_id: int) -> float:
    state, started_at, run_seconds, _, _ = get_session(message_id)
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

def tracker_url(guild_id: int, channel_id: int, message_id: int) -> str:
    return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"

def build_embed(message_id: int) -> discord.Embed:
    state, _, _, _, _ = get_session(message_id)
    elapsed = session_elapsed_seconds(message_id)
    parts = list_participants(message_id)

    embed = discord.Embed(
        title="📜 The Grandmaster's Guild — RP Session Log",
        description="The registrar’s record of attendance and session time."
    )

    embed.add_field(name="Status", value=state_label(state), inline=True)
    embed.add_field(name="Session Time", value=f"⏳ **{fmt_hm(elapsed)}**", inline=True)

    if parts:
        roster_lines = []
        for uid, char, lvl, _secs, cap, xp_dip, gp_dip in parts:
            tags = []
            if cap:
                tags.append("Capped")
            if xp_dip:
                tags.append("XP DIP")
            if gp_dip:
                tags.append("GP DIP")
            suffix = f" *({', '.join(tags)})*" if tags else ""
            roster_lines.append(f"<@{uid}> — **{char}** (lvl {lvl}){suffix}")
        roster = "\n".join(roster_lines)
    else:
        roster = "*No adventurers signed in yet.*"

    embed.add_field(name="Roster", value=roster[:1024], inline=False)

    embed.add_field(
        name="Reward Rule",
        value="Earn **1 hour** at **00:45**, **2 hours** at **01:45**, etc.\n"
              "**XP/hr:** by level bracket • **GP/hr:** level × 10 • **Capped:** 🗝️/hr",
        inline=False
    )

    return apply_theme(embed)

async def update_tracker_message(message_id: int):
    _, _, _, channel_id, _ = get_session(message_id)
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
    bot.add_view(view)

# =========================
# TIME TICKER
# =========================
def tick_running_sessions():
    """
    Update participants.seconds for sessions that are RUNNING.
    Only participants with last_tick NOT NULL accrue time.
    """
    now = time.time()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT message_id FROM sessions WHERE state=1")
            running_ids = [int(r[0]) for r in cur.fetchall()]

            for mid in running_ids:
                cur.execute("""
                    SELECT user_id, last_tick, seconds
                    FROM participants
                    WHERE message_id=%s AND last_tick IS NOT NULL
                """, (mid,))
                rows = cur.fetchall()

                for uid, last_tick, secs in rows:
                    delta = max(0.0, now - float(last_tick))
                    new_secs = float(secs or 0) + delta
                    cur.execute("""
                        UPDATE participants
                        SET seconds=%s, last_tick=%s
                        WHERE message_id=%s AND user_id=%s
                    """, (new_secs, now, mid, int(uid)))

async def ticker_loop():
    while True:
        try:
            tick_running_sessions()

            with db() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT message_id FROM sessions WHERE state IN (1,2)")
                    mids = [int(r[0]) for r in cur.fetchall()]

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
    capped = discord.ui.TextInput(label="Capped? (yes/no)", required=False, max_length=5, placeholder="no")

    # NEW: DIPs
    xp_dip = discord.ui.TextInput(label="XP DIP? (yes/no)", required=False, max_length=5, placeholder="no")
    gp_dip = discord.ui.TextInput(label="GP DIP? (yes/no)", required=False, max_length=5, placeholder="no")

    def __init__(self, message_id: int):
        super().__init__()
        self.message_id = message_id

    @staticmethod
    def _is_yes(val: Optional[str]) -> int:
        raw = (str(val).strip().lower() if val else "no")
        return 1 if raw in ("y", "yes", "true", "1", "dip") else 0

    async def on_submit(self, interaction: discord.Interaction):
        try:
            lvl = int(str(self.level.value).strip())
            if not (1 <= lvl <= 20):
                raise ValueError
        except ValueError:
            return await interaction.response.send_message("Level must be a number between 1 and 20.", ephemeral=True)

        cname = str(self.name.value).strip()
        if not cname:
            return await interaction.response.send_message("Name can’t be empty.", ephemeral=True)

        is_capped = self._is_yes(self.capped.value)
        has_xp_dip = self._is_yes(self.xp_dip.value)
        has_gp_dip = self._is_yes(self.gp_dip.value)

        now = time.time()
        state, _, _, _, _ = get_session(self.message_id)

        with db() as conn:
            with conn.cursor() as cur:
                # preserve accumulated seconds if editing
                cur.execute("""
                    SELECT COALESCE(seconds, 0) FROM participants
                    WHERE message_id=%s AND user_id=%s
                """, (self.message_id, interaction.user.id))
                row = cur.fetchone()
                prev_secs = float(row[0]) if row else 0.0

                # Upsert
                cur.execute("""
                    INSERT INTO participants
                        (message_id, user_id, character, level, seconds, last_tick, capped, xp_dip, gp_dip)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (message_id, user_id)
                    DO UPDATE SET
                        character = EXCLUDED.character,
                        level = EXCLUDED.level,
                        seconds = EXCLUDED.seconds,
                        last_tick = EXCLUDED.last_tick,
                        capped = EXCLUDED.capped,
                        xp_dip = EXCLUDED.xp_dip,
                        gp_dip = EXCLUDED.gp_dip
                """, (
                    self.message_id,
                    interaction.user.id,
                    cname,
                    lvl,
                    prev_secs,
                    (now if state == 1 else None),
                    is_capped,
                    has_xp_dip,
                    has_gp_dip
                ))

        tags = []
        if is_capped:
            tags.append("Capped: 🗝️/hr")
        if has_xp_dip:
            tags.append("XP DIP")
        if has_gp_dip:
            tags.append("GP DIP")
        tag_txt = f" *({', '.join(tags)})*" if tags else ""

        await interaction.response.send_message(f"✅ Signed in: **{cname}** (lvl {lvl}){tag_txt}", ephemeral=True)
        await update_tracker_message(self.message_id)

# =========================
# VIEW (buttons)
# =========================
class RPView(discord.ui.View):
    def __init__(self, message_id: int):
        super().__init__(timeout=None)
        self.message_id = message_id

        # Row 0: player actions
        self.join_btn = discord.ui.Button(label="✅ Join", style=discord.ButtonStyle.success, custom_id=f"rp_join:{message_id}", row=0)
        self.leave_btn = discord.ui.Button(label="⏹ Leave", style=discord.ButtonStyle.secondary, custom_id=f"rp_leave:{message_id}", row=0)
        self.rejoin_btn = discord.ui.Button(label="🔁 Rejoin", style=discord.ButtonStyle.secondary, custom_id=f"rp_rejoin:{message_id}", row=0)

        # Row 1: actions
        self.start_btn = discord.ui.Button(label="▶️ Start", style=discord.ButtonStyle.primary, custom_id=f"rp_start:{message_id}", row=1)
        self.pause_btn = discord.ui.Button(label="⏸ Pause", style=discord.ButtonStyle.secondary, custom_id=f"rp_pause:{message_id}", row=1)
        self.resume_btn = discord.ui.Button(label="⏵ Resume", style=discord.ButtonStyle.success, custom_id=f"rp_resume:{message_id}", row=1)

        # Row 2: end
        self.end_btn = discord.ui.Button(label="🏁 End", style=discord.ButtonStyle.danger, custom_id=f"rp_end:{message_id}", row=2)

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

    async def join_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(JoinModal(self.message_id))

    async def leave_cb(self, interaction: discord.Interaction):
        """Stop time tracking ONLY for the clicker."""
        now = time.time()
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT last_tick, seconds FROM participants
                    WHERE message_id=%s AND user_id=%s
                """, (self.message_id, interaction.user.id))
                row = cur.fetchone()

                if not row:
                    await interaction.response.send_message("You haven’t joined this RP yet.", ephemeral=True)
                    return

                last_tick, secs = row[0], float(row[1] or 0.0)
                if last_tick is not None:
                    secs += max(0.0, now - float(last_tick))

                cur.execute("""
                    UPDATE participants
                    SET seconds=%s, last_tick=NULL
                    WHERE message_id=%s AND user_id=%s
                """, (secs, self.message_id, interaction.user.id))

        await interaction.response.send_message("⏹ You’ve left (timer paused for you only).", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def rejoin_cb(self, interaction: discord.Interaction):
        """Restart time tracking ONLY for the clicker if session is running."""
        state, _, _, _, _ = get_session(self.message_id)

        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM participants WHERE message_id=%s AND user_id=%s", (self.message_id, interaction.user.id))
                exists = cur.fetchone() is not None

                if not exists:
                    await interaction.response.send_message("You haven’t joined yet. Click **✅ Join** first.", ephemeral=True)
                    return

                if state != 1:
                    cur.execute("UPDATE participants SET last_tick=NULL WHERE message_id=%s AND user_id=%s", (self.message_id, interaction.user.id))
                    await interaction.response.send_message("You’re marked present, but the session isn’t running.", ephemeral=True)
                    await update_tracker_message(self.message_id)
                    return

                now = time.time()
                cur.execute("UPDATE participants SET last_tick=%s WHERE message_id=%s AND user_id=%s", (now, self.message_id, interaction.user.id))

        await interaction.response.send_message("🔁 You’re back in. Your timer is running again.", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def start_cb(self, interaction: discord.Interaction):
        state, _, run_seconds, _, _ = get_session(self.message_id)
        if state == 1:
            await interaction.response.send_message("Already running.", ephemeral=True)
            return

        now = time.time()
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO sessions (message_id, state, started_at, run_seconds, channel_id, guild_id)
                    VALUES (%s, 0, NULL, 0, %s, %s)
                    ON CONFLICT (message_id) DO NOTHING
                """, (self.message_id, interaction.channel_id, interaction.guild_id))

                cur.execute("""
                    UPDATE sessions
                    SET state=1, started_at=%s, channel_id=%s, guild_id=%s, run_seconds=%s
                    WHERE message_id=%s
                """, (now, interaction.channel_id, interaction.guild_id, float(run_seconds or 0.0), self.message_id))

                # start accruing for everyone currently participating
                cur.execute("UPDATE participants SET last_tick=%s WHERE message_id=%s", (now, self.message_id))

        await interaction.response.send_message("▶️ Session started. The guild clock is running.", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def pause_cb(self, interaction: discord.Interaction):
        state, started_at, run_seconds, _, _ = get_session(self.message_id)
        if state != 1 or started_at is None:
            await interaction.response.send_message("Not currently running.", ephemeral=True)
            return

        tick_running_sessions()

        now = time.time()
        new_run = float(run_seconds or 0.0) + max(0.0, now - started_at)

        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE sessions SET state=2, started_at=NULL, run_seconds=%s WHERE message_id=%s", (new_run, self.message_id))
                cur.execute("UPDATE participants SET last_tick=NULL WHERE message_id=%s", (self.message_id,))

        await interaction.response.send_message("⏸ Session paused. Quills down.", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def resume_cb(self, interaction: discord.Interaction):
        state, _, run_seconds, _, _ = get_session(self.message_id)
        if state != 2:
            await interaction.response.send_message("Not currently paused.", ephemeral=True)
            return

        now = time.time()
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE sessions SET state=1, started_at=%s, run_seconds=%s WHERE message_id=%s
                """, (now, float(run_seconds or 0.0), self.message_id))
                # resume for everyone; individual players can opt out via Leave
                cur.execute("UPDATE participants SET last_tick=%s WHERE message_id=%s", (now, self.message_id))

        await interaction.response.send_message("⏵ Session resumed. The guild clock continues.", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def end_cb(self, interaction: discord.Interaction):
        await end_session_and_post_rewards(interaction, self.message_id)

# =========================
# END SESSION CORE
# =========================
async def end_session_and_post_rewards(interaction: discord.Interaction, message_id: int):
    if not interaction.response.is_done():
        await interaction.response.defer(thinking=False)

    state, started_at, run_seconds, channel_id, guild_id = get_session(message_id)
    if not channel_id or not guild_id:
        try:
            await interaction.followup.send("❌ Could not locate this session in the ledger.", ephemeral=True)
        except Exception:
            pass
        return

    # Final tick for anyone still accruing
    if state == 1 and started_at is not None:
        tick_running_sessions()
        now = time.time()
        run_seconds = float(run_seconds or 0.0) + max(0.0, now - started_at)

    # Stop session + stop all personal timers
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE sessions SET state=0, started_at=NULL, run_seconds=%s WHERE message_id=%s",
                (float(run_seconds or 0.0), message_id)
            )
            cur.execute("UPDATE participants SET last_tick=NULL WHERE message_id=%s", (message_id,))

    parts = list_participants(message_id)
    start_link = tracker_url(guild_id, channel_id, message_id)

    header = "🏁 **Guild Ledger Closed — Rewards Issued**\nThe registrar tallies the earnings and stamps the record.\n"
    lines = []

    for uid, char, lvl, secs, cap, xp_dip, gp_dip in parts:
        hrs = reward_hours(secs)

        # Base rewards
        gp = gp_per_hour_for_level(lvl) * hrs
        if gp_dip:
            gp *= 2

        dip_tags = []
        if xp_dip:
            dip_tags.append("XP×2")
        if gp_dip:
            dip_tags.append("GP×2")
        dip_txt = f" *({', '.join(dip_tags)})*" if dip_tags else ""

        if cap:
            keys = hrs
            keys_add(guild_id, uid, keys)
            lines.append(f"<@{uid}> — **{char}** (lvl {lvl}) — **{hrs}h** — **{keys}** 🗝️, **{gp}** gp{dip_txt}")
        else:
            xp = xp_per_hour_for_level(lvl) * hrs
            if xp_dip:
                xp *= 2
            lines.append(f"<@{uid}> — **{char}** (lvl {lvl}) — **{hrs}h** — **{xp}** xp, **{gp}** gp{dip_txt}")

    if not lines:
        lines = ["*(no participants)*"]

    content = header + "\n".join(lines)

    rewards_msg = await interaction.followup.send(content, wait=True)
    try:
        end_link = rewards_msg.jump_url
        links_bottom = f"\n\n🔗 **Start:** {start_link}\n🔗 **End:** {end_link}"
        await rewards_msg.edit(content=rewards_msg.content + links_bottom)
    except Exception:
        pass

    await update_tracker_message(message_id)

# =========================
# SLASH COMMANDS
# =========================
@bot.tree.command(name="rpbegin", description="Post and pin the Adventurer’s Guild RP tracker.")
async def rpbegin(interaction: discord.Interaction):
    await interaction.response.defer(thinking=False)

    temp = apply_theme(discord.Embed(
        title="📜 Opening a new Guild Ledger…",
        description="Preparing the session log."
    ))
    msg = await interaction.followup.send(embed=temp, wait=True)

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sessions (message_id, state, started_at, run_seconds, channel_id, guild_id)
                VALUES (%s, 0, NULL, 0, %s, %s)
                ON CONFLICT (message_id) DO NOTHING
            """, (msg.id, msg.channel.id, interaction.guild_id))
            cur.execute("UPDATE sessions SET channel_id=%s, guild_id=%s WHERE message_id=%s",
                        (msg.channel.id, interaction.guild_id, msg.id))

    view = RPView(msg.id)
    await msg.edit(embed=build_embed(msg.id), view=view)
    bot.add_view(view)

    try:
        await msg.pin(reason="Adventurer’s Guild RP Tracker")
    except Exception:
        pass

@bot.tree.command(name="rpend", description="End the active RP session in this channel/thread.")
async def rpend(interaction: discord.Interaction):
    await interaction.response.defer(thinking=False)

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT message_id
                FROM sessions
                WHERE channel_id=%s AND guild_id=%s AND state IN (1,2)
                ORDER BY message_id DESC
                LIMIT 1
            """, (interaction.channel_id, interaction.guild_id))
            row = cur.fetchone()

    if not row:
        await interaction.followup.send("❌ No active tracker found in this channel.", ephemeral=True)
        return

    message_id = int(row[0])
    await end_session_and_post_rewards(interaction, message_id)

# =========================
# PREFIX COMMAND: !key
# =========================
@bot.command(name="key")
async def key_cmd(ctx: commands.Context, amount: Optional[str] = None, *, reason: Optional[str] = None):
    if amount is None:
        current, lifetime = keys_get(ctx.guild.id, ctx.author.id)
        await ctx.send(embed=build_key_embed(ctx.author, current, lifetime))
        return

    try:
        delta = int(amount.strip())
    except Exception:
        await ctx.send('Usage: `!key` or `!key +3 "Reason"` or `!key -2 "Reason"`')
        return

    if delta == 0:
        await ctx.send("That would change nothing. 🙂")
        return

    target = ctx.author

    if delta > 0:
        keys_add(ctx.guild.id, target.id, delta)
    else:
        keys_sub(ctx.guild.id, target.id, abs(delta))

    current, lifetime = keys_get(ctx.guild.id, target.id)
    reason_text = reason if reason else "*No reason provided.*"

    ledger_text = (
        f"Name: {target.mention}\n"
        f"Keys Earned: {delta:+d} 🗝️\n"
        f"For: {reason_text}"
    )

    await ctx.send(ledger_text)
    await ctx.send(embed=build_key_embed(target, current, lifetime))

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
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT message_id FROM sessions")
            msg_ids = [int(r[0]) for r in cur.fetchall()]

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
