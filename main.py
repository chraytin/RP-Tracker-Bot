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
# DATABASE + SCHEMA
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
            channel_id INTEGER,
            guild_id INTEGER
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

    # keys ledger:
    # current = spendable current keys
    # lifetime = lifetime keys earned (never decreases)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS keys (
            guild_id INTEGER,
            user_id INTEGER,
            current INTEGER DEFAULT 0,
            lifetime INTEGER DEFAULT 0,
            PRIMARY KEY (guild_id, user_id)
        )
    """)

    # Migrations (safe add columns)
    cur.execute("PRAGMA table_info(sessions)")
    scols = {row[1] for row in cur.fetchall()}
    if "guild_id" not in scols:
        cur.execute("ALTER TABLE sessions ADD COLUMN guild_id INTEGER")

    cur.execute("PRAGMA table_info(participants)")
    pcols = {row[1] for row in cur.fetchall()}
    if "capped" not in pcols:
        cur.execute("ALTER TABLE participants ADD COLUMN capped INTEGER")
        cur.execute("UPDATE participants SET capped = COALESCE(capped, 0)")
    if "last_tick" not in pcols:
        cur.execute("ALTER TABLE participants ADD COLUMN last_tick REAL")

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

# Required for prefix command !key (Message Content Intent must be enabled in Dev Portal)
intents.message_content = True

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
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT current, lifetime FROM keys WHERE guild_id=? AND user_id=?", (guild_id, user_id))
    row = cur.fetchone()
    conn.close()
    if not row:
        return (0, 0)
    return (int(row[0] or 0), int(row[1] or 0))

def keys_add(guild_id: int, user_id: int, amount: int):
    amount = int(amount)
    if amount <= 0:
        return
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO keys (guild_id, user_id, current, lifetime)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id)
        DO UPDATE SET
            current = current + excluded.current,
            lifetime = lifetime + excluded.lifetime
    """, (guild_id, user_id, amount, amount))
    conn.commit()
    conn.close()

def keys_sub(guild_id: int, user_id: int, amount: int):
    amount = int(amount)
    if amount <= 0:
        return
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO keys (guild_id, user_id, current, lifetime)
        VALUES (?, ?, 0, 0)
        ON CONFLICT(guild_id, user_id) DO NOTHING
    """, (guild_id, user_id))
    cur.execute("""
        UPDATE keys
        SET current = CASE WHEN current - ? < 0 THEN 0 ELSE current - ? END
        WHERE guild_id=? AND user_id=?
    """, (amount, amount, guild_id, user_id))
    conn.commit()
    conn.close()

def build_key_embed(member: discord.Member, current: int, lifetime: int) -> discord.Embed:
    title = f"🗝️ {member.display_name}'s Keyring"
    embed = discord.Embed(title=title, description="", color=theme_color())
    embed.add_field(name="Current Keys", value=str(current), inline=False)
    embed.add_field(name="Lifetime Keys", value=str(lifetime), inline=False)

    key_thumb = os.getenv("KEY_THUMBNAIL_URL") or os.getenv("THEME_THUMBNAIL_URL")
    if key_thumb:
        embed.set_thumbnail(url=key_thumb)

    # CHANGE #2: Keyring footer should NOT include the 45-minute rewards clause
    return apply_theme(embed, footer_text_override="Stamped & filed by the Guild Registrar")

# =========================
# HELPERS (SESSIONS)
# =========================
def get_session(message_id: int) -> Tuple[int, Optional[float], float, Optional[int], Optional[int]]:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "SELECT state, started_at, COALESCE(run_seconds,0), channel_id, guild_id FROM sessions WHERE message_id=?",
        (message_id,)
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return 0, None, 0.0, None, None
    state = int(row[0] or 0)
    started_at = float(row[1]) if row[1] is not None else None
    run_seconds = float(row[2] or 0.0)
    channel_id = int(row[3]) if row[3] is not None else None
    guild_id = int(row[4]) if row[4] is not None else None
    return state, started_at, run_seconds, channel_id, guild_id

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
        for uid, char, lvl, _secs, cap in parts:
            suffix = " *(Capped)*" if cap else ""
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
            cur.execute("SELECT message_id FROM sessions WHERE state IN (1,2)")
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

        # preserve accumulated seconds if editing
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

        # if session is running, auto-start their personal timer
        state, _, _, _, _ = get_session(self.message_id)
        if state == 1:
            now = time.time()
            cur.execute("""
                UPDATE participants SET last_tick=?
                WHERE message_id=? AND user_id=?
            """, (now, self.message_id, interaction.user.id))

        conn.commit()
        conn.close()

        cap_txt = " *(Capped: 🗝️/hr)*" if is_capped else ""
        await interaction.response.send_message(
            f"✅ Signed in: **{cname}** (lvl {lvl}){cap_txt}",
            ephemeral=True
        )
        await update_tracker_message(self.message_id)

# =========================
# VIEW (buttons)
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

        # Row 1: staff actions
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

        # Row 2: staff only
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

    def _require_staff(self, interaction: discord.Interaction) -> bool:
        return interaction.user.guild_permissions.manage_guild

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
            secs += max(0.0, now - float(last_tick))

        cur.execute("""
            UPDATE participants
            SET seconds=?, last_tick=NULL
            WHERE message_id=? AND user_id=?
        """, (secs, self.message_id, interaction.user.id))

        conn.commit()
        conn.close()

        await interaction.response.send_message("⏹ You’ve left (timer paused for you only).", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def rejoin_cb(self, interaction: discord.Interaction):
        """Restart time tracking ONLY for the clicker if session is running."""
        state, _, _, _, _ = get_session(self.message_id)

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

    async def start_cb(self, interaction: discord.Interaction):
        if not self._require_staff(interaction):
            await interaction.response.send_message("Staff only.", ephemeral=True)
            return

        state, _, run_seconds, _, _ = get_session(self.message_id)
        if state == 1:
            await interaction.response.send_message("Already running.", ephemeral=True)
            return

        now = time.time()
        conn = db()
        cur = conn.cursor()

        cur.execute(
            "INSERT OR IGNORE INTO sessions (message_id, state, started_at, run_seconds, channel_id, guild_id) VALUES (?, 0, NULL, 0, ?, ?)",
            (self.message_id, interaction.channel_id, interaction.guild_id)
        )

        cur.execute(
            "UPDATE sessions SET state=1, started_at=?, channel_id=?, guild_id=?, run_seconds=? WHERE message_id=?",
            (now, interaction.channel_id, interaction.guild_id, float(run_seconds or 0.0), self.message_id)
        )

        # start accruing for everyone currently participating
        cur.execute("UPDATE participants SET last_tick=? WHERE message_id=?", (now, self.message_id))

        conn.commit()
        conn.close()

        await interaction.response.send_message("▶️ Session started. The guild clock is running.", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def pause_cb(self, interaction: discord.Interaction):
        if not self._require_staff(interaction):
            await interaction.response.send_message("Staff only.", ephemeral=True)
            return

        state, started_at, run_seconds, _, _ = get_session(self.message_id)
        if state != 1 or started_at is None:
            await interaction.response.send_message("Not currently running.", ephemeral=True)
            return

        tick_running_sessions()

        now = time.time()
        new_run = float(run_seconds or 0.0) + max(0.0, now - started_at)

        conn = db()
        cur = conn.cursor()
        cur.execute("UPDATE sessions SET state=2, started_at=NULL, run_seconds=? WHERE message_id=?", (new_run, self.message_id))
        cur.execute("UPDATE participants SET last_tick=NULL WHERE message_id=?", (self.message_id,))
        conn.commit()
        conn.close()

        await interaction.response.send_message("⏸ Session paused. Quills down.", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def resume_cb(self, interaction: discord.Interaction):
        if not self._require_staff(interaction):
            await interaction.response.send_message("Staff only.", ephemeral=True)
            return

        state, _, run_seconds, _, _ = get_session(self.message_id)
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
        # resume for everyone; individual players can opt out via Leave
        cur.execute("UPDATE participants SET last_tick=? WHERE message_id=?", (now, self.message_id))

        conn.commit()
        conn.close()

        await interaction.response.send_message("⏵ Session resumed. The guild clock continues.", ephemeral=True)
        await update_tracker_message(self.message_id)

    async def end_cb(self, interaction: discord.Interaction):
        if not self._require_staff(interaction):
            await interaction.response.send_message("Staff only.", ephemeral=True)
            return
        await end_session_and_post_rewards(interaction, self.message_id)

# =========================
# END SESSION CORE (used by button + /rpend)
# Rewards output is NOT an embed.
# =========================
async def end_session_and_post_rewards(interaction: discord.Interaction, message_id: int):
    # Defer so Discord never times out
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
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE sessions SET state=0, started_at=NULL, run_seconds=? WHERE message_id=?",
        (float(run_seconds or 0.0), message_id)
    )
    cur.execute("UPDATE participants SET last_tick=NULL WHERE message_id=?", (message_id,))
    conn.commit()
    conn.close()

    parts = list_participants(message_id)
    start_link = tracker_url(guild_id, channel_id, message_id)

    header = "🏁 **Guild Ledger Closed — Rewards Issued**\nThe registrar tallies the earnings and stamps the record.\n"
    lines = []

    for uid, char, lvl, secs, cap in parts:
        hrs = reward_hours(secs)
        gp = gp_per_hour_for_level(lvl) * hrs

        if cap:
            keys = hrs
            keys_add(guild_id, uid, keys)
            lines.append(f"<@{uid}> — **{char}** (lvl {lvl}) — **{keys}** 🗝️, **{gp}** gp")
        else:
            xp = xp_per_hour_for_level(lvl) * hrs
            lines.append(f"<@{uid}> — **{char}** (lvl {lvl}) — **{xp}** xp, **{gp}** gp")

    if not lines:
        lines = ["*(no participants)*"]

    # CHANGE #1: Links go at the bottom (Start/End)
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

    # Store session
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO sessions (message_id, state, started_at, run_seconds, channel_id, guild_id) VALUES (?, 0, NULL, 0, ?, ?)",
        (msg.id, msg.channel.id, interaction.guild_id)
    )
    cur.execute("UPDATE sessions SET channel_id=?, guild_id=? WHERE message_id=?",
                (msg.channel.id, interaction.guild_id, msg.id))
    conn.commit()
    conn.close()

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

    conn = db()
    cur = conn.cursor()
    cur.execute("""
        SELECT message_id
        FROM sessions
        WHERE channel_id=? AND guild_id=? AND state IN (1,2)
        ORDER BY message_id DESC
        LIMIT 1
    """, (interaction.channel_id, interaction.guild_id))
    row = cur.fetchone()
    conn.close()

    if not row:
        await interaction.followup.send("❌ No active tracker found in this channel.", ephemeral=True)
        return

    message_id = int(row[0])
    await end_session_and_post_rewards(interaction, message_id)

# =========================
# PREFIX COMMAND: !key
# - !key -> show keyring embed
# - !key +# "Reason" / !key -# "Reason" -> staff only, modifies own keys and posts:
#    (1) plain text ledger entry (your format)
#    (2) updated keyring embed
# =========================
@bot.command(name="key")
async def key_cmd(ctx: commands.Context, amount: Optional[str] = None, *, reason: Optional[str] = None):
    # Display: !key
    if amount is None:
        current, lifetime = keys_get(ctx.guild.id, ctx.author.id)
        embed = build_key_embed(ctx.author, current, lifetime)
        await ctx.send(embed=embed)
        return

    # Modify: staff only
    if not ctx.author.guild_permissions.manage_guild:
        await ctx.send("❌ Staff only (Manage Guild required) to modify keys.")
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

    # CHANGE #3: Ledger entry is plain text (not an embed), in your exact format
    reason_text = reason if reason else "*No reason provided.*"
    ledger_text = (
        f"Name: {target.mention}\n"
        f"Keys Earned: {delta:+d} 🗝️\n"
        f"For: {reason_text}"
    )

    keyring_embed = build_key_embed(target, current, lifetime)

    await ctx.send(ledger_text)
    await ctx.send(embed=keyring_embed)

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
