import discord
from discord.ext import commands
from discord import app_commands
import sqlite3
import time
import os

# =========================
# CONFIG
# =========================
TOKEN = os.getenv("DISCORD_TOKEN")

XP_PER_HOUR = 100
GP_PER_HOUR = 25

DB_FILE = "rp_tracker.db"

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
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

# =========================
# MODAL
# =========================
class JoinModal(discord.ui.Modal, title="Join RP"):
    name = discord.ui.TextInput(label="Character Name")
    level = discord.ui.TextInput(label="Level")

    def __init__(self, message_id):
        super().__init__()
        self.message_id = message_id

    async def on_submit(self, interaction: discord.Interaction):
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
            self.name.value,
            int(self.level.value),
            self.message_id,
            interaction.user.id
        ))

        conn.commit()
        conn.close()

        await interaction.response.send_message(
            f"✅ Joined as **{self.name.value}** (Level {self.level.value})",
            ephemeral=True
        )

# =========================
# VIEW
# =========================
class RPView(discord.ui.View):
    def __init__(self, message_id):
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
            delta = now - last
            cur.execute("""
                UPDATE participants
                SET seconds=?, last_tick=?
                WHERE message_id=? AND user_id=?
            """, (secs + delta, now, self.message_id, uid))

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

        cur.execute("""
            INSERT OR REPLACE INTO sessions (message_id, active, started_at)
            VALUES (?, 1, ?)
        """, (self.message_id, now))

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

        cur.execute("UPDATE sessions SET active=0 WHERE message_id=?", (self.message_id,))
        cur.execute("""
            SELECT user_id, character, level, seconds
            FROM participants WHERE message_id=?
        """, (self.message_id,))

        lines = []
        for uid, char, lvl, secs in cur.fetchall():
            hours = secs / 3600
            xp = XP_PER_HOUR * hours
            gp = GP_PER_HOUR * hours
            lines.append(
                f"<@{uid}> — **{char}** (Lv {lvl}) → "
                f"{xp:.1f} XP | {gp:.1f} GP"
            )

        conn.commit()
        conn.close()

        await interaction.response.send_message(
            "**🏁 RP Session Ended**\n" + "\n".join(lines)
        )

# =========================
# COMMAND
# =========================
@bot.tree.command(name="post_rp_tracker")
async def post_tracker(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🎭 RP Tracker",
        description="Join the RP session below."
    )
    await interaction.response.send_message(embed=embed)
    msg = await interaction.original_response()

    conn = db()
    conn.execute(
        "INSERT OR IGNORE INTO sessions VALUES (?, 0, NULL)",
        (msg.id,)
    )
    conn.commit()
    conn.close()

    view = RPView(msg.id)
    bot.add_view(view)
    await msg.edit(view=view)

# =========================
# READY
# =========================
@bot.event
async def on_ready():
    conn = db()
    for (msg_id,) in conn.execute("SELECT message_id FROM sessions"):
        bot.add_view(RPView(msg_id))
    conn.close()

    await bot.tree.sync()
    print(f"Logged in as {bot.user}")

# =========================
# RUN
# =========================
bot.run(TOKEN)
