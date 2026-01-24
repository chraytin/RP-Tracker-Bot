import os
import time
import sqlite3
import traceback

import discord
from discord.ext import commands

# =========================
# CONFIG
# =========================
TOKEN = os.getenv("DISCORD_TOKEN")

XP_PER_HOUR = int(os.getenv("XP_PER_HOUR", "100"))
GP_PER_HOUR = int(os.getenv("GP_PER_HOUR", "25"))

DB_FILE = "rp_tracker.db"

# Force immediate logs on Railway
print("Booting RP Tracker…", flush=True)

if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is not set. Add it in Railway → Variables.")

# =========================
# DATABASE
# =========================
def db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            message_id INTEGER PRIMARY KEY,
            active INTEGER,
            started_at REAL
        )
    """)
    conn.execute("""
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
    conn.commit()
    return conn

# =========================
# BOT SETUP
# =========================
intents = discord.Intents.default()
intents.guilds = True
# DO NOT enable members intent unless you also enable it in Dev Portal.
# intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

# =========================
# MODAL
# =========================
class JoinModal(discord.ui.Modal, title="Join RP"):
    name = discord.ui.TextInput(label="Character Name", max_length=64)
    level = discord.ui.TextInput(label="Level (1-30)", max_length=3)

    def __init__(self, message_id: int):
        super().__init__()
        self.message_id = message_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            lvl = int(str(self.level.value).strip())
            if lvl < 1 or lvl > 30:
                raise ValueError
        except ValueError:
            return await interaction.response.send_message(
                "Level must be a number between 1 and 30.",
                ephemeral=True,
            )

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
            str(self.name.value).strip(),
            lvl,
            self.message_id,
            interaction.user.id
        ))
        conn.commit()
        conn.close()

        await interaction.response.send_message(
            f"✅ Joined as **{self.name.value}** (Level {lvl})",
            ephemeral=True
        )

# =========================
# VIEW
# =========================
class RPView(discord.ui.View):
    def __init__(self, message_id: int):
        super().__init__(timeout=None)
        self.message_id = message_id

    def tick(self):
        conn = db()
        cur = conn.cursor()

        cur.execute("SELECT active FROM sessions WHERE message_id=?", (self.message_id,))
        row = cur.fetchone()
        if not row or not row[0]:
            conn.close()
            return

        now = time.time()
        cur.execute("""
            SELECT user_id, last_tick, seconds FROM participants
            WHERE message_id=? AND last_tick IS NOT NULL
        """, (self.message_id,))

        for uid, last, secs in cur.fetchall():
            delta = max(0.0, now - float(last))
            cur.execute("""
                UPDATE participants
                SET seconds=?, last_tick=?
                WHERE message_id=? AND user_id=?
            """, (float(secs) + delta, now, self.message_id, uid))

        conn.commit()
        conn.close()

    @discord.ui.button(label="Join RP", style=discord.ButtonStyle.success)
    async def join(self, interaction: discord.Interaction, _):
        await interaction.response.send_modal(JoinModal(self.message_id))

    @discord.ui.button(label="Start RP", style=discord.ButtonStyle.primary)
    async def start(self, interaction: discord.Interaction, _):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return

        conn = db()
        cur = conn.cursor()
        now = time.time()

        cur.execute("INSERT OR IGNORE INTO sessions VALUES (?, 0, NULL)", (self.message_id,))
        cur.execute("""
            UPDATE sessions SET active=1, started_at=? WHERE message_id=?
        """, (now, self.message_id))

        cur.execute("""
            UPDATE participants SET last_tick=?
            WHERE message_id=?
        """, (now, self.message_id))

        conn.commit()
        conn.close()

        await interaction.response.send_message("▶️ RP Started", ephemeral=True)

    @discord.ui.button(label="End RP", style=discord.ButtonStyle.danger)
    async def end(self, interaction: discord.Interaction, _):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return

        self.tick()

        conn = db()
        cur = conn.cursor()

        cur.execute("UPDATE sessions SET active=0, started_at=NULL WHERE message_id=?", (self.message_id,))
        cur.execute("""
            UPDATE participants SET last_tick=NULL WHERE message_id=?
        """, (self.message_id,))

        cur.execute("""
            SELECT user_id, character, level, seconds
            FROM participants WHERE message_id=?
        """, (self.message_id,))

        lines = []
        for uid, char, lvl, secs in cur.fetchall():
            hours = float(secs) / 3600.0
            xp = XP_PER_HOUR * hours
            gp = GP_PER_HOUR * hours
            lines.append(f"<@{uid}> — **{char}** (Lv {lvl}) → {xp:.1f} XP | {gp:.1f} GP")

        conn.commit()
        conn.close()

        await interaction.response.send_message("**🏁 RP Session Ended**\n" + ("\n".join(lines) if lines else "(no participants)"))

# =========================
# SLASH COMMAND
# =========================
@bot.tree.command(name="post_rp_tracker", description="Post an RP tracker with Join/Start/End buttons.")
async def post_tracker(interaction: discord.Interaction):
    embed = discord.Embed(title="🎭 RP Tracker", description="Join the RP session below.")
    await interaction.response.send_message(embed=embed)
    msg = await interaction.original_response()

    conn = db()
    conn.execute("INSERT OR IGNORE INTO sessions VALUES (?, 0, NULL)", (msg.id,))
    conn.commit()
    conn.close()

    view = RPView(msg.id)
    bot.add_view(view)
    await msg.edit(view=view)

# =========================
# ERROR HANDLERS (so it doesn’t “do nothing”)
# =========================
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: Exception):
    print("Slash command error:", repr(error), flush=True)
    traceback.print_exc()
    try:
        if interaction.response.is_done():
            await interaction.followup.send(f"❌ Error: `{type(error).__name__}`", ephemeral=True)
        else:
            await interaction.response.send_message(f"❌ Error: `{type(error).__name__}`", ephemeral=True)
    except Exception:
        pass

@bot.event
async def on_error(event, *args, **kwargs):
    print(f"Event error in {event}", flush=True)
    traceback.print_exc()

# =========================
# READY
# =========================
@bot.event
async def on_ready():
    # Re-register persistent views
    conn = db()
    for (msg_id,) in conn.execute("SELECT message_id FROM sessions"):
        bot.add_view(RPView(int(msg_id)))
    conn.close()

    # Sync commands
    try:
        await bot.tree.sync()
        print("Slash commands synced.", flush=True)
    except Exception as e:
        print("Command sync failed:", repr(e), flush=True)
        traceback.print_exc()

    print(f"Logged in as {bot.user} (guilds={len(bot.guilds)})", flush=True)

# =========================
# RUN
# =========================
print("Starting bot.run()…", flush=True)
bot.run(TOKEN)
