import os
import time
import asyncio
import traceback
import random
import csv
import shlex
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, List, Tuple, Dict

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
    raise RuntimeError(
        "DISCORD_TOKEN is not set. Add it in Railway → Variables."
    )

DOUBLE_RP_EVENT_ACTIVE = False
DOUBLE_QUEST_EVENT_ACTIVE = False

SPECIALIST_RP_COINS_PER_HOUR = 2
SPECIALIST_RP_GP_PER_HOUR = 200

SPECIALIST_QUEST_COINS_MIN = 4
SPECIALIST_QUEST_COINS_MAX = 8
SPECIALIST_QUEST_GP_MIN = 1000
SPECIALIST_QUEST_GP_MAX = 2000


def db():
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set. In Railway, add a Variable Reference "
            "from the Postgres service into the RP-Tracker-Bot service."
        )

    return psycopg.connect(database_url)


# =========================
# LOOT LOADING
# =========================
RARITY_ORDER = [
    "Common",
    "Uncommon",
    "Rare",
    "Very Rare",
    "Legendary",
    "Artifact",
]


def _repo_path(filename: str) -> str:
    base = os.getenv(
        "RAILWAY_WORKDIR",
        "/app",
    )

    return os.path.join(
        base,
        filename,
    )


def load_loot_csv(
    path: str,
) -> List[Tuple[str, str]]:

    items: List[Tuple[str, str]] = []

    if not os.path.exists(path):
        return items

    with open(
        path,
        "r",
        encoding="utf-8",
        errors="ignore",
        newline="",
    ) as f:

        reader = csv.reader(f)

        for row in reader:
            if not row:
                continue

            name = (
                row[0]
                or ""
            ).strip()

            if not name:
                continue

            low = name.lower()

            if low in (
                "item",
                "items",
                "name",
            ):
                continue

            if low.startswith("name"):
                continue

            if "minimum trade value" in low:
                continue

            shop_price = ""

            if len(row) > 1:
                shop_price = (
                    row[1]
                    or ""
                ).strip()

            if not shop_price:
                shop_price = "—"

            items.append(
                (
                    name,
                    shop_price,
                )
            )

    seen = set()

    out: List[
        Tuple[str, str]
    ] = []

    for (
        name,
        price,
    ) in items:

        key = name.lower()

        if key not in seen:
            seen.add(key)

            out.append(
                (
                    name,
                    price,
                )
            )

    return out


# =========================
# DETAILED LOOT LOOKUP
# =========================
def _normalize_header(
    value: str,
) -> str:

    return "".join(
        ch
        for ch in str(
            value
        ).strip().lower()
        if ch.isalnum()
    )


def _csv_value(
    row: dict,
    *possible_headers: str,
) -> str:

    normalized_row = {}

    for (
        key,
        value,
    ) in row.items():

        if key is None:
            continue

        normalized_row[
            _normalize_header(
                key
            )
        ] = (
            str(
                value
            ).strip()
            if value is not None
            else ""
        )

    for header in possible_headers:
        value = normalized_row.get(
            _normalize_header(
                header
            )
        )

        if value:
            return value

    return ""


def load_loot_details_csv(
    path: str,
    rarity: str,
) -> List[Dict[str, str]]:

    items: List[
        Dict[str, str]
    ] = []

    if not os.path.exists(path):
        return items

    with open(
        path,
        "r",
        encoding="utf-8-sig",
        errors="ignore",
        newline="",
    ) as f:

        reader = csv.DictReader(f)

        for row in reader:
            name = _csv_value(
                row,
                "Name",
                "Item",
                "Items",
                "Item Name",
            )

            if not name:
                continue

            shop_value = _csv_value(
                row,
                "Shop Price",
                "Shop Value",
                "Shop",
                "Price",
            )

            min_trade = _csv_value(
                row,
                "Minimum Trade Value",
                "Min Trade Value",
                "Minimum Trade",
                "Min Trade",
                "Trade Value",
            )

            items.append({
                "name": name,
                "rarity": rarity,
                "shop_value": (
                    shop_value
                    or "—"
                ),
                "min_trade": (
                    min_trade
                    or "—"
                ),
            })

    return items


LOOT_TABLE: Dict[
    str,
    List[
        Tuple[str, str]
    ]
] = {}


for rarity in RARITY_ORDER:
    fn = (
        f"Guild Loot List - "
        f"{rarity}.csv"
    )

    LOOT_TABLE[
        rarity
    ] = load_loot_csv(
        _repo_path(
            fn
        )
    )


print(
    "Loot loaded:",
    {
        k: len(v)
        for k, v
        in LOOT_TABLE.items()
    },
    flush=True,
)


LOOT_DETAILS: List[
    Dict[str, str]
] = []


for rarity in RARITY_ORDER:
    fn = (
        f"Guild Loot List - "
        f"{rarity}.csv"
    )

    LOOT_DETAILS.extend(
        load_loot_details_csv(
            _repo_path(
                fn
            ),
            rarity,
        )
    )


print(
    f"Loot details loaded: "
    f"{len(LOOT_DETAILS)} items",
    flush=True,
)


def rarity_for_level(
    level: int,
) -> str:

    if 2 <= level <= 4:
        return "Common"

    if 5 <= level <= 8:
        return "Uncommon"

    if 9 <= level <= 12:
        return "Rare"

    if 13 <= level <= 16:
        return "Very Rare"

    return "Legendary"


def rarity_shift(
    base: str,
    roll: int,
    level: int,
) -> str:

    # Grandmaster quest rolls can NEVER
    # automatically produce Artifact loot.
    # Legendary is the maximum.
    idx = RARITY_ORDER.index(
        base
    )

    if roll == 1:
        idx = max(
            0,
            idx - 1,
        )

    elif roll == 100:
        idx = min(
            RARITY_ORDER.index(
                "Legendary"
            ),
            idx + 1,
        )

    return RARITY_ORDER[
        idx
    ]


def random_loot(
    rarity: str,
) -> Optional[Tuple[str, str]]:

    pool = (
        LOOT_TABLE.get(
            rarity
        )
        or []
    )

    if not pool:
        return None

    return random.choice(
        pool
    )


# =========================
# DATABASE + SCHEMA
# =========================
def ensure_schema():
    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    message_id BIGINT PRIMARY KEY,
                    state INT,
                    started_at DOUBLE PRECISION,
                    run_seconds DOUBLE PRECISION,
                    ended_at DOUBLE PRECISION,
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
                    specialist INT DEFAULT 0,
                    present INT DEFAULT 1,
                    PRIMARY KEY (
                        message_id,
                        user_id
                    )
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS keys (
                    guild_id BIGINT,
                    user_id BIGINT,
                    current INT DEFAULT 0,
                    lifetime INT DEFAULT 0,
                    PRIMARY KEY (
                        guild_id,
                        user_id
                    )
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS specialist_coins (
                    guild_id BIGINT,
                    user_id BIGINT,
                    current INT DEFAULT 0,
                    lifetime INT DEFAULT 0,
                    PRIMARY KEY (
                        guild_id,
                        user_id
                    )
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS session_events (
                    id BIGSERIAL PRIMARY KEY,
                    session_message_id BIGINT,
                    event_type TEXT,
                    event_message_id BIGINT,
                    channel_id BIGINT,
                    guild_id BIGINT,
                    created_at DOUBLE PRECISION
                )
            """)

            cur.execute(
                "ALTER TABLE sessions "
                "ADD COLUMN IF NOT EXISTS guild_id BIGINT"
            )

            cur.execute(
                "ALTER TABLE sessions "
                "ADD COLUMN IF NOT EXISTS run_seconds DOUBLE PRECISION"
            )

            cur.execute(
                "ALTER TABLE sessions "
                "ADD COLUMN IF NOT EXISTS ended_at DOUBLE PRECISION"
            )

            cur.execute(
                "ALTER TABLE participants "
                "ADD COLUMN IF NOT EXISTS capped INT DEFAULT 0"
            )

            cur.execute(
                "ALTER TABLE participants "
                "ADD COLUMN IF NOT EXISTS last_tick DOUBLE PRECISION"
            )

            cur.execute(
                "ALTER TABLE participants "
                "ADD COLUMN IF NOT EXISTS xp_dip INT DEFAULT 0"
            )

            cur.execute(
                "ALTER TABLE participants "
                "ADD COLUMN IF NOT EXISTS gp_dip INT DEFAULT 0"
            )

            cur.execute(
                "ALTER TABLE participants "
                "ADD COLUMN IF NOT EXISTS specialist INT DEFAULT 0"
            )

            cur.execute(
                "ALTER TABLE participants "
                "ADD COLUMN IF NOT EXISTS present INT DEFAULT 1"
            )

            cur.execute("""
                UPDATE sessions
                SET run_seconds = COALESCE(
                    run_seconds,
                    0
                )
                WHERE run_seconds IS NULL
            """)

            cur.execute("""
                UPDATE participants
                SET seconds = COALESCE(
                    seconds,
                    0
                )
                WHERE seconds IS NULL
            """)

            cur.execute("""
                UPDATE participants
                SET capped = COALESCE(
                    capped,
                    0
                )
                WHERE capped IS NULL
            """)

            cur.execute("""
                UPDATE participants
                SET xp_dip = COALESCE(
                    xp_dip,
                    0
                )
                WHERE xp_dip IS NULL
            """)

            cur.execute("""
                UPDATE participants
                SET gp_dip = COALESCE(
                    gp_dip,
                    0
                )
                WHERE gp_dip IS NULL
            """)

            cur.execute("""
                UPDATE participants
                SET specialist = COALESCE(
                    specialist,
                    0
                )
                WHERE specialist IS NULL
            """)

            cur.execute("""
                UPDATE participants
                SET present = COALESCE(
                    present,
                    1
                )
                WHERE present IS NULL
            """)

            cur.execute("""
                UPDATE sessions AS s
                SET ended_at = e.ended_at
                FROM (
                    SELECT
                        session_message_id,
                        MAX(created_at) AS ended_at
                    FROM session_events
                    WHERE event_type='end'
                    GROUP BY session_message_id
                ) AS e
                WHERE
                    s.message_id = e.session_message_id
                    AND s.ended_at IS NULL
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS
                    idx_sessions_guild_ended
                ON sessions (
                    guild_id,
                    ended_at
                )
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS
                    idx_participants_user_message
                ON participants (
                    user_id,
                    message_id
                )
            """)


ensure_schema()


# =========================
# RP REWARD RULES
# =========================
def reward_hours(
    seconds: float,
) -> int:

    return int(
        (
            max(
                0.0,
                seconds,
            )
            + 900
        )
        // 3600
    )


def xp_per_hour_for_level(
    level: int,
) -> int:

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


def gp_per_hour_for_level(
    level: int,
) -> int:

    return max(
        0,
        int(
            level
        ),
    ) * 10


# =========================
# QUEST REWARD TABLE
# =========================
QUEST_XP: Dict[
    int,
    Tuple[int, int]
] = {
    2: (600, 1200),
    3: (600, 1200),
    4: (600, 1200),

    5: (1200, 2400),
    6: (1200, 2400),
    7: (1200, 2400),
    8: (1200, 2400),

    9: (1600, 3200),
    10: (1600, 3200),
    11: (1600, 3200),
    12: (1600, 3200),

    13: (2000, 4000),
    14: (2000, 4000),
    15: (2000, 4000),
    16: (2000, 4000),

    17: (2400, 4800),
    18: (2400, 4800),
    19: (2400, 4800),
}


QUEST_GP: Dict[
    int,
    Tuple[int, int]
] = {
    2: (100, 200),
    3: (150, 300),
    4: (200, 400),

    5: (250, 500),
    6: (300, 600),
    7: (350, 700),
    8: (400, 800),

    9: (450, 900),
    10: (500, 1000),
    11: (550, 1100),
    12: (600, 1200),

    13: (650, 1300),
    14: (700, 1400),
    15: (750, 1500),
    16: (800, 1600),

    17: (850, 1700),
    18: (900, 1800),
    19: (950, 1900),

    20: (1000, 2000),
}


# =========================
# THEME
# =========================
def theme_color() -> discord.Color:

    raw = os.getenv(
        "THEME_COLOR",
        "#C9A227",
    ).lstrip("#")

    try:
        return discord.Color(
            int(
                raw,
                16,
            )
        )

    except Exception:
        return discord.Color.gold()


def apply_theme(
    embed: discord.Embed,
    *,
    footer_text_override: Optional[str] = None,
) -> discord.Embed:

    embed.color = theme_color()

    thumb = os.getenv(
        "THEME_THUMBNAIL_URL"
    )

    if thumb:
        embed.set_thumbnail(
            url=thumb
        )

    banner = os.getenv(
        "THEME_BANNER_URL"
    )

    if banner:
        embed.set_image(
            url=banner
        )

    guild_name = os.getenv(
        "THEME_NAME",
        "Adventurer’s Guild Ledger",
    )

    embed.set_author(
        name=guild_name
    )

    footer_text = (
        footer_text_override
        if footer_text_override is not None
        else os.getenv(
            "THEME_FOOTER_TEXT",
            "Stamped & filed by the Guild Registrar • Rewards granted on 45-minute marks",
        )
    )

    footer_icon = os.getenv(
        "THEME_FOOTER_ICON_URL"
    )

    if footer_icon:
        embed.set_footer(
            text=footer_text,
            icon_url=footer_icon,
        )

    else:
        embed.set_footer(
            text=footer_text
        )

    return embed


# =========================
# BOT SETUP
# =========================
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True

bot = commands.Bot(
    command_prefix="!",
    intents=intents,
)


# =========================
# KEEPALIVE WEB SERVER
# =========================
async def handle_root(
    _: web.Request,
) -> web.Response:

    return web.Response(
        text="RP Tracker is running."
    )


async def handle_health(
    _: web.Request,
) -> web.Response:

    return web.Response(
        text="ok"
    )


async def start_web_server():
    port = int(
        os.getenv(
            "PORT",
            "8080",
        )
    )

    app = web.Application()

    app.router.add_get(
        "/",
        handle_root,
    )

    app.router.add_get(
        "/health",
        handle_health,
    )

    runner = web.AppRunner(
        app
    )

    await runner.setup()

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        port,
    )

    await site.start()

    print(
        f"Web server listening on "
        f"0.0.0.0:{port}",
        flush=True,
    )


# =========================
# KEY HELPERS
# =========================
def keys_get(
    guild_id: int,
    user_id: int,
) -> Tuple[int, int]:

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    current,
                    lifetime
                FROM keys
                WHERE
                    guild_id=%s
                    AND user_id=%s
            """, (
                guild_id,
                user_id,
            ))

            row = cur.fetchone()

    if not row:
        return 0, 0

    return (
        int(
            row[0]
            or 0
        ),
        int(
            row[1]
            or 0
        ),
    )


def keys_add(
    guild_id: int,
    user_id: int,
    amount: int,
):
    amount = int(
        amount
    )

    if amount <= 0:
        return

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO keys (
                    guild_id,
                    user_id,
                    current,
                    lifetime
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (
                    guild_id,
                    user_id
                )
                DO UPDATE SET
                    current =
                        keys.current
                        + EXCLUDED.current,
                    lifetime =
                        keys.lifetime
                        + EXCLUDED.lifetime
            """, (
                guild_id,
                user_id,
                amount,
                amount,
            ))


def keys_sub(
    guild_id: int,
    user_id: int,
    amount: int,
):
    amount = int(
        amount
    )

    if amount <= 0:
        return

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO keys (
                    guild_id,
                    user_id,
                    current,
                    lifetime
                )
                VALUES (
                    %s,
                    %s,
                    0,
                    0
                )
                ON CONFLICT (
                    guild_id,
                    user_id
                )
                DO NOTHING
            """, (
                guild_id,
                user_id,
            ))

            cur.execute("""
                UPDATE keys
                SET current = GREATEST(
                    current - %s,
                    0
                )
                WHERE
                    guild_id=%s
                    AND user_id=%s
            """, (
                amount,
                guild_id,
                user_id,
            ))


def build_key_embed(
    member: discord.Member,
    current: int,
    lifetime: int,
) -> discord.Embed:

    embed = discord.Embed(
        title=(
            f"🗝️ "
            f"{member.display_name}'s Keyring"
        ),
        description="",
        color=theme_color(),
    )

    embed.add_field(
        name="Current Keys",
        value=str(
            current
        ),
        inline=False,
    )

    embed.add_field(
        name="Lifetime Keys",
        value=str(
            lifetime
        ),
        inline=False,
    )

    key_thumb = (
        os.getenv(
            "KEY_THUMBNAIL_URL"
        )
        or os.getenv(
            "THEME_THUMBNAIL_URL"
        )
    )

    if key_thumb:
        embed.set_thumbnail(
            url=key_thumb
        )

    return apply_theme(
        embed,
        footer_text_override=(
            "Stamped & filed by "
            "the Guild Registrar"
        ),
    )


# =========================
# SPECIALIST COIN HELPERS
# =========================
def special_get(
    guild_id: int,
    user_id: int,
) -> Tuple[int, int]:

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    current,
                    lifetime
                FROM specialist_coins
                WHERE
                    guild_id=%s
                    AND user_id=%s
            """, (
                guild_id,
                user_id,
            ))

            row = cur.fetchone()

    if not row:
        return 0, 0

    return (
        int(
            row[0]
            or 0
        ),
        int(
            row[1]
            or 0
        ),
    )


def special_add(
    guild_id: int,
    user_id: int,
    amount: int,
):
    amount = int(
        amount
    )

    if amount <= 0:
        return

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO specialist_coins (
                    guild_id,
                    user_id,
                    current,
                    lifetime
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (
                    guild_id,
                    user_id
                )
                DO UPDATE SET
                    current =
                        specialist_coins.current
                        + EXCLUDED.current,
                    lifetime =
                        specialist_coins.lifetime
                        + EXCLUDED.lifetime
            """, (
                guild_id,
                user_id,
                amount,
                amount,
            ))


def special_sub(
    guild_id: int,
    user_id: int,
    amount: int,
):
    amount = int(
        amount
    )

    if amount <= 0:
        return

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO specialist_coins (
                    guild_id,
                    user_id,
                    current,
                    lifetime
                )
                VALUES (
                    %s,
                    %s,
                    0,
                    0
                )
                ON CONFLICT (
                    guild_id,
                    user_id
                )
                DO NOTHING
            """, (
                guild_id,
                user_id,
            ))

            cur.execute("""
                UPDATE specialist_coins
                SET current = GREATEST(
                    current - %s,
                    0
                )
                WHERE
                    guild_id=%s
                    AND user_id=%s
            """, (
                amount,
                guild_id,
                user_id,
            ))


def build_special_embed(
    member: discord.Member,
    current: int,
    lifetime: int,
) -> discord.Embed:

    embed = discord.Embed(
        title=(
            f"🪙 "
            f"{member.display_name}'s "
            f"Specialist Purse"
        ),
        description="",
        color=theme_color(),
    )

    embed.add_field(
        name="Current Coins",
        value=str(
            current
        ),
        inline=False,
    )

    embed.add_field(
        name="Lifetime Coins",
        value=str(
            lifetime
        ),
        inline=False,
    )

    special_thumb = (
        os.getenv(
            "SPECIAL_THUMBNAIL_URL"
        )
        or os.getenv(
            "THEME_THUMBNAIL_URL"
        )
    )

    if special_thumb:
        embed.set_thumbnail(
            url=special_thumb
        )

    return apply_theme(
        embed,
        footer_text_override=(
            "Stamped & filed in "
            "the Specialist Ledger"
        ),
    )


# =========================
# SESSION EVENT HELPERS
# =========================
def log_session_event(
    session_message_id: int,
    event_type: str,
    event_message_id: int,
    channel_id: int,
    guild_id: int,
):
    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO session_events (
                    session_message_id,
                    event_type,
                    event_message_id,
                    channel_id,
                    guild_id,
                    created_at
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
            """, (
                session_message_id,
                event_type,
                event_message_id,
                channel_id,
                guild_id,
                time.time(),
            ))


def get_session_events(
    session_message_id: int,
) -> List[
    Tuple[
        str,
        int,
        int,
        int,
    ]
]:

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    event_type,
                    event_message_id,
                    channel_id,
                    guild_id
                FROM session_events
                WHERE session_message_id=%s
                ORDER BY
                    created_at ASC,
                    id ASC
            """, (
                session_message_id,
            ))

            rows = cur.fetchall()

    return [
        (
            str(
                event_type
            ),
            int(
                event_message_id
            ),
            int(
                channel_id
            ),
            int(
                guild_id
            ),
        )
        for (
            event_type,
            event_message_id,
            channel_id,
            guild_id,
        ) in rows
    ]


def build_jump_link(
    guild_id: int,
    channel_id: int,
    message_id: int,
) -> str:

    return (
        f"https://discord.com/channels/"
        f"{guild_id}/"
        f"{channel_id}/"
        f"{message_id}"
    )


def split_discord_lines(
    header: str,
    lines: List[str],
    limit: int = 1900,
) -> List[str]:

    chunks: List[str] = []
    current = header

    for line in lines:
        addition = (
            "\n" + line
            if current
            else line
        )

        if (
            len(
                current
            )
            + len(
                addition
            )
            > limit
        ):
            if current:
                chunks.append(
                    current
                )

            current = line

        else:
            current += addition

    if current:
        chunks.append(
            current
        )

    return chunks


# =========================
# SESSION HELPERS
# =========================
def get_session(
    message_id: int,
) -> Tuple[
    int,
    Optional[float],
    float,
    Optional[int],
    Optional[int],
]:

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    state,
                    started_at,
                    COALESCE(
                        run_seconds,
                        0
                    ),
                    channel_id,
                    guild_id
                FROM sessions
                WHERE message_id=%s
            """, (
                message_id,
            ))

            row = cur.fetchone()

    if not row:
        return (
            0,
            None,
            0.0,
            None,
            None,
        )

    state = int(
        row[0]
        or 0
    )

    started_at = (
        float(
            row[1]
        )
        if row[1] is not None
        else None
    )

    run_seconds = float(
        row[2]
        or 0.0
    )

    channel_id = (
        int(
            row[3]
        )
        if row[3] is not None
        else None
    )

    guild_id = (
        int(
            row[4]
        )
        if row[4] is not None
        else None
    )

    return (
        state,
        started_at,
        run_seconds,
        channel_id,
        guild_id,
    )


def list_participants(
    message_id: int,
) -> List[
    Tuple[
        int,
        str,
        int,
        float,
        int,
        int,
        int,
        int,
    ]
]:

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    user_id,
                    character,
                    level,
                    COALESCE(
                        seconds,
                        0
                    ),
                    COALESCE(
                        capped,
                        0
                    ),
                    COALESCE(
                        xp_dip,
                        0
                    ),
                    COALESCE(
                        gp_dip,
                        0
                    ),
                    COALESCE(
                        specialist,
                        0
                    )
                FROM participants
                WHERE message_id=%s
                ORDER BY user_id
            """, (
                message_id,
            ))

            rows = cur.fetchall()

    return [
        (
            int(
                uid
            ),
            str(
                ch
            ),
            int(
                lvl
            ),
            float(
                secs
            ),
            int(
                cap
            ),
            int(
                xd
            ),
            int(
                gd
            ),
            int(
                spec
            ),
        )
        for (
            uid,
            ch,
            lvl,
            secs,
            cap,
            xd,
            gd,
            spec,
        ) in rows
    ]


def session_elapsed_seconds(
    message_id: int,
) -> float:

    (
        state,
        started_at,
        run_seconds,
        _,
        _,
    ) = get_session(
        message_id
    )

    if (
        state == 1
        and started_at is not None
    ):
        return (
            run_seconds
            + max(
                0.0,
                time.time()
                - started_at,
            )
        )

    return run_seconds


def fmt_hm(
    seconds: float,
) -> str:

    seconds = max(
        0.0,
        seconds,
    )

    h = int(
        seconds
        // 3600
    )

    m = int(
        (
            seconds
            % 3600
        )
        // 60
    )

    return (
        f"{h}h "
        f"{m}m"
    )


def state_label(
    state: int,
) -> str:

    if state == 1:
        return "🟢 Active"

    if state == 2:
        return "🟡 Paused"

    return "⚪ Not Started"


def tracker_url(
    guild_id: int,
    channel_id: int,
    message_id: int,
) -> str:

    return (
        f"https://discord.com/channels/"
        f"{guild_id}/"
        f"{channel_id}/"
        f"{message_id}"
    )


def build_embed(
    message_id: int,
) -> discord.Embed:

    (
        state,
        _,
        _,
        _,
        _,
    ) = get_session(
        message_id
    )

    elapsed = session_elapsed_seconds(
        message_id
    )

    parts = list_participants(
        message_id
    )

    embed = discord.Embed(
        title=(
            "📜 The Grandmaster's Guild "
            "— RP Session Log"
        ),
        description=(
            "The registrar’s record of "
            "attendance and session time."
        ),
    )

    embed.add_field(
        name="Status",
        value=state_label(
            state
        ),
        inline=True,
    )

    embed.add_field(
        name="Session Time",
        value=(
            f"⏳ **"
            f"{fmt_hm(elapsed)}"
            f"**"
        ),
        inline=True,
    )

    if parts:
        roster_lines = []

        for (
            uid,
            char,
            lvl,
            _secs,
            cap,
            reward_dip,
            gp_dip,
            specialist,
        ) in parts:

            tags = []

            if specialist:
                tags.append(
                    "Specialist"
                )

                if reward_dip == 1:
                    tags.append(
                        "🪙 DIP"
                    )

                elif reward_dip == 2:
                    tags.append(
                        "🪙 DOUBLE DIP"
                    )

                level_text = (
                    "Specialist"
                )

            else:
                if cap:
                    tags.append(
                        "Capped"
                    )

                if (
                    not DOUBLE_RP_EVENT_ACTIVE
                    and reward_dip
                ):
                    tags.append(
                        "XP DIP"
                    )

                level_text = (
                    f"lvl {lvl}"
                )

            if (
                not DOUBLE_RP_EVENT_ACTIVE
                and gp_dip
            ):
                tags.append(
                    "GP DIP"
                )

            suffix = (
                f" *("
                f"{', '.join(tags)}"
                f")*"
                if tags
                else ""
            )

            roster_lines.append(
                f"<@{uid}> — "
                f"**{char}** "
                f"({level_text})"
                f"{suffix}"
            )

        roster = "\n".join(
            roster_lines
        )

    else:
        roster = (
            "*No adventurers "
            "signed in yet.*"
        )

    embed.add_field(
        name="Roster",
        value=roster[:1024],
        inline=False,
    )

    reward_rule = (
        "Earn **1 hour** at **00:45**, "
        "**2 hours** at **01:45**, etc.\n"
        "**Standard:** XP/hr by level • "
        "GP/hr: level × 10\n"
        "**Capped / Level 20:** 🗝️/hr\n"
        "**Specialist:** 2🪙 + 200 gp/hr • "
        "DIP: 4🪙/hr • "
        "Double DIP: 6🪙/hr"
    )

    if DOUBLE_RP_EVENT_ACTIVE:
        reward_rule += (
            "\n\n🎉 **Vamp's B'Day Bash Active:** "
            "RP **XP and GP are automatically doubled**. "
            "Reward and GP dips are disabled."
        )

    embed.add_field(
        name="Reward Rule",
        value=reward_rule,
        inline=False,
    )

    return apply_theme(
        embed
    )


def build_rp_status_announcement(
    action: str,
    actor: discord.abc.User,
    message_id: int,
) -> discord.Embed:

    (
        state,
        _,
        _,
        _,
        _,
    ) = get_session(
        message_id
    )

    elapsed = session_elapsed_seconds(
        message_id
    )

    titles = {
        "start": "▶️ RP Session Started",
        "pause": "⏸ RP Session Paused",
        "resume": "⏵ RP Session Resumed",
        "end": "🏁 RP Session Ended",
    }

    descriptions = {
        "start": (
            "The Grandmaster has marked "
            "this RP as officially underway."
        ),
        "pause": (
            "The Grandmaster has temporarily "
            "halted this RP."
        ),
        "resume": (
            "The Grandmaster has declared "
            "this RP active once more."
        ),
        "end": (
            "The Grandmaster has formally "
            "closed this RP session."
        ),
    }

    embed = discord.Embed(
        title=titles.get(
            action,
            "📜 RP Session Update",
        ),
        description=descriptions.get(
            action,
            "The status of this RP has changed.",
        ),
        color=theme_color(),
    )

    embed.add_field(
        name="Status",
        value=state_label(
            state
        ),
        inline=True,
    )

    embed.add_field(
        name="Session Time",
        value=(
            f"⏳ **"
            f"{fmt_hm(elapsed)}"
            f"**"
        ),
        inline=True,
    )

    embed.add_field(
        name="Filed By",
        value=actor.mention,
        inline=False,
    )

    return apply_theme(
        embed,
        footer_text_override=(
            "Filed in the Guild Ledger"
        ),
    )


async def post_rp_status_announcement(
    channel: discord.abc.Messageable,
    action: str,
    actor: discord.abc.User,
    session_message_id: int,
):
    try:
        embed = build_rp_status_announcement(
            action,
            actor,
            session_message_id,
        )

        msg = await channel.send(
            embed=embed
        )

        guild = getattr(
            channel,
            "guild",
            None,
        )

        channel_id = getattr(
            channel,
            "id",
            None,
        )

        guild_id = getattr(
            guild,
            "id",
            None,
        )

        if (
            channel_id is not None
            and guild_id is not None
        ):
            log_session_event(
                session_message_id=(
                    session_message_id
                ),
                event_type=action,
                event_message_id=msg.id,
                channel_id=channel_id,
                guild_id=guild_id,
            )

        return msg

    except Exception:
        print(
            "Failed to post RP status announcement",
            flush=True,
        )

        traceback.print_exc()

        return None


async def update_tracker_message(
    message_id: int,
):
    (
        _,
        _,
        _,
        channel_id,
        _,
    ) = get_session(
        message_id
    )

    if not channel_id:
        return

    channel = bot.get_channel(
        channel_id
    )

    if channel is None:
        try:
            channel = await bot.fetch_channel(
                channel_id
            )

        except Exception:
            return

    if (
        isinstance(
            channel,
            discord.Thread,
        )
        and channel.archived
    ):
        return

    try:
        msg = await channel.fetch_message(
            message_id
        )

    except Exception:
        return

    view = RPView(
        message_id
    )

    try:
        await msg.edit(
            embed=build_embed(
                message_id
            ),
            view=view,
        )

        bot.add_view(
            view
        )

    except discord.HTTPException as e:
        if getattr(
            e,
            "code",
            None,
        ) == 50083:
            return

        if getattr(
            e,
            "status",
            None,
        ) in (
            502,
            503,
            504,
        ):
            print(
                (
                    "Skipping temporary Discord API "
                    f"error while updating tracker "
                    f"{message_id}: {e}"
                ),
                flush=True,
            )
            return

        raise

    except discord.DiscordServerError as e:
        print(
            (
                "Skipping temporary Discord "
                f"server error while updating "
                f"tracker {message_id}: {e}"
            ),
            flush=True,
        )
        return

    except Exception as e:
        err_text = str(
            e
        ).lower()

        transient_markers = (
            "connection reset by peer",
            "service unavailable",
            "upstream connect error",
            "disconnect/reset before headers",
            "remote connection failure",
            "overflow",
        )

        if any(
            marker in err_text
            for marker in transient_markers
        ):
            print(
                (
                    "Skipping temporary network "
                    f"error while updating tracker "
                    f"{message_id}: {e}"
                ),
                flush=True,
            )
            return

        raise


# =========================
# TIME TICKER
# =========================
def tick_running_sessions():
    now = time.time()

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT message_id
                FROM sessions
                WHERE state=1
            """)

            running_ids = [
                int(
                    r[0]
                )
                for r in cur.fetchall()
            ]

            for mid in running_ids:

                cur.execute("""
                    SELECT
                        user_id,
                        last_tick,
                        seconds
                    FROM participants
                    WHERE
                        message_id=%s
                        AND present=1
                        AND last_tick IS NOT NULL
                        AND last_tick >= 0
                """, (
                    mid,
                ))

                rows = cur.fetchall()

                for (
                    uid,
                    last_tick,
                    secs,
                ) in rows:

                    delta = max(
                        0.0,
                        now
                        - float(
                            last_tick
                        ),
                    )

                    new_secs = (
                        float(
                            secs
                            or 0
                        )
                        + delta
                    )

                    cur.execute("""
                        UPDATE participants
                        SET
                            seconds=%s,
                            last_tick=%s
                        WHERE
                            message_id=%s
                            AND user_id=%s
                            AND present=1
                    """, (
                        new_secs,
                        now,
                        mid,
                        int(
                            uid
                        ),
                    ))


async def ticker_loop():
    while True:
        try:
            tick_running_sessions()

            with db() as conn:
                with conn.cursor() as cur:

                    cur.execute("""
                        SELECT message_id
                        FROM sessions
                        WHERE state=1
                    """)

                    mids = [
                        int(
                            r[0]
                        )
                        for r in cur.fetchall()
                    ]

            for mid in mids:
                try:
                    await update_tracker_message(
                        mid
                    )

                except Exception:
                    print(
                        (
                            "Ticker update failed "
                            f"for message_id={mid}"
                        ),
                        flush=True,
                    )

                    traceback.print_exc()

        except Exception:
            print(
                "Ticker loop error:",
                flush=True,
            )

            traceback.print_exc()

        await asyncio.sleep(
            15
        )


# =========================
# JOIN MODAL
# =========================
class JoinModal(
    discord.ui.Modal,
    title="Adventurer Sign-In",
):
    name = discord.ui.TextInput(
        label="Character Name",
        max_length=64,
    )

    level = discord.ui.TextInput(
        label="Level (1-21)",
        max_length=2,
        placeholder="21 = Specialist",
    )

    capped = discord.ui.TextInput(
        label="Capped? (yes/no)",
        required=False,
        max_length=5,
        placeholder="no",
    )

    xp_dip = discord.ui.TextInput(
        label=(
            "Reward DIP? "
            "(no/dip/double dip)"
        ),
        required=False,
        max_length=10,
        placeholder="no",
    )

    gp_dip = discord.ui.TextInput(
        label="GP DIP? (yes/no)",
        required=False,
        max_length=5,
        placeholder="no",
    )

    def __init__(
        self,
        message_id: int,
    ):
        super().__init__()

        self.message_id = (
            message_id
        )

    @staticmethod
    def _is_yes(
        val: Optional[str],
    ) -> int:

        raw = (
            str(
                val
            ).strip().lower()
            if val
            else "no"
        )

        return (
            1
            if raw in (
                "y",
                "yes",
                "true",
                "1",
                "dip",
            )
            else 0
        )

    @staticmethod
    def _parse_reward_dip(
        val: Optional[str],
    ) -> Optional[int]:

        raw = (
            str(
                val
            ).strip().lower()
            if val
            else "no"
        )

        if raw in (
            "",
            "n",
            "no",
            "none",
            "false",
            "0",
        ):
            return 0

        if raw in (
            "y",
            "yes",
            "dip",
            "single dip",
            "1",
        ):
            return 1

        if raw in (
            "double",
            "double dip",
            "doubledip",
            "dd",
            "2",
        ):
            return 2

        return None

    async def on_submit(
        self,
        interaction: discord.Interaction,
    ):
        try:
            lvl = int(
                str(
                    self.level.value
                ).strip()
            )

            if not 1 <= lvl <= 21:
                raise ValueError

        except ValueError:
            await interaction.response.send_message(
                (
                    "❌ Level must be a number "
                    "from `1` to `21`.\n"
                    "`21` represents a Specialist character."
                ),
                ephemeral=True,
            )
            return

        is_specialist = (
            1
            if lvl == 21
            else 0
        )

        cname = str(
            self.name.value
        ).strip()

        if not cname:
            await interaction.response.send_message(
                "❌ Character name can’t be empty.",
                ephemeral=True,
            )
            return

        is_capped = self._is_yes(
            self.capped.value
        )

        reward_dip = self._parse_reward_dip(
            self.xp_dip.value
        )

        if reward_dip is None:
            await interaction.response.send_message(
                (
                    "❌ Reward DIP must be "
                    "`no`, `dip`, or `double dip`."
                ),
                ephemeral=True,
            )
            return

        if (
            not is_specialist
            and reward_dip == 2
        ):
            await interaction.response.send_message(
                (
                    "❌ Double DIP is only "
                    "available to Specialist characters."
                ),
                ephemeral=True,
            )
            return

        if is_specialist:
            is_capped = 0

        if is_capped:
            reward_dip = 0

        if DOUBLE_RP_EVENT_ACTIVE:
            has_reward_dip = 0
            has_gp_dip = 0

        else:
            has_reward_dip = (
                reward_dip
            )

            has_gp_dip = self._is_yes(
                self.gp_dip.value
            )

        now = time.time()

        (
            state,
            _,
            _,
            _,
            _,
        ) = get_session(
            self.message_id
        )

        with db() as conn:
            with conn.cursor() as cur:

                cur.execute("""
                    SELECT
                        COALESCE(
                            seconds,
                            0
                        )
                    FROM participants
                    WHERE
                        message_id=%s
                        AND user_id=%s
                """, (
                    self.message_id,
                    interaction.user.id,
                ))

                row = cur.fetchone()

                prev_secs = (
                    float(
                        row[0]
                    )
                    if row
                    else 0.0
                )

                cur.execute("""
                    INSERT INTO participants (
                        message_id,
                        user_id,
                        character,
                        level,
                        seconds,
                        last_tick,
                        capped,
                        xp_dip,
                        gp_dip,
                        specialist,
                        present
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        1
                    )
                    ON CONFLICT (
                        message_id,
                        user_id
                    )
                    DO UPDATE SET
                        character =
                            EXCLUDED.character,
                        level =
                            EXCLUDED.level,
                        seconds =
                            EXCLUDED.seconds,
                        last_tick =
                            EXCLUDED.last_tick,
                        capped =
                            EXCLUDED.capped,
                        xp_dip =
                            EXCLUDED.xp_dip,
                        gp_dip =
                            EXCLUDED.gp_dip,
                        specialist =
                            EXCLUDED.specialist,
                        present = 1
                """, (
                    self.message_id,
                    interaction.user.id,
                    cname,
                    lvl,
                    prev_secs,
                    (
                        now
                        if state == 1
                        else None
                    ),
                    is_capped,
                    has_reward_dip,
                    has_gp_dip,
                    is_specialist,
                ))

        tags = []

        if is_specialist:
            tags.append(
                "Specialist"
            )

            if has_reward_dip == 1:
                tags.append(
                    "🪙 DIP"
                )

            elif has_reward_dip == 2:
                tags.append(
                    "🪙 DOUBLE DIP"
                )

        elif is_capped:
            tags.append(
                "Capped: 🗝️/hr"
            )

        elif has_reward_dip:
            tags.append(
                "XP DIP"
            )

        if has_gp_dip:
            tags.append(
                "GP DIP"
            )

        tag_txt = (
            f" *("
            f"{', '.join(tags)}"
            f")*"
            if tags
            else ""
        )

        display_level = (
            "Specialist"
            if is_specialist
            else f"lvl {lvl}"
        )

        if DOUBLE_RP_EVENT_ACTIVE:
            await interaction.response.send_message(
                (
                    f"✅ Signed in: **{cname}** "
                    f"({display_level}){tag_txt}\n\n"
                    "🎉 **Vamp's B'Day Bash Active:** "
                    "RP **XP and GP are automatically doubled**.\n"
                    "Reward and GP dips are disabled "
                    "during the event."
                ),
                ephemeral=True,
            )

        else:
            await interaction.response.send_message(
                (
                    f"✅ Signed in: **{cname}** "
                    f"({display_level}){tag_txt}"
                ),
                ephemeral=True,
            )

        await update_tracker_message(
            self.message_id
        )


# =========================
# VIEW
# =========================
class EndConfirmModal(
    discord.ui.Modal,
    title="Confirm RP End",
):
    confirm = discord.ui.TextInput(
        label=(
            'Type "yes" to end '
            "the RP for EVERYONE"
        ),
        placeholder="yes",
        max_length=10,
    )

    def __init__(
        self,
        message_id: int,
    ):
        super().__init__()

        self.message_id = (
            message_id
        )

    async def on_submit(
        self,
        interaction: discord.Interaction,
    ):
        if (
            str(
                self.confirm.value
            ).strip().lower()
            != "yes"
        ):
            await interaction.response.send_message(
                (
                    "❌ RP end cancelled. "
                    "You must type `yes` exactly."
                ),
                ephemeral=True,
            )
            return

        await end_session_and_post_rewards(
            interaction,
            self.message_id,
        )


class EndConfirmView(
    discord.ui.View,
):
    def __init__(
        self,
        message_id: int,
    ):
        super().__init__(
            timeout=60
        )

        self.message_id = (
            message_id
        )

    @discord.ui.button(
        label="Confirm End RP",
        style=discord.ButtonStyle.danger,
    )
    async def confirm_end(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        await interaction.response.send_modal(
            EndConfirmModal(
                self.message_id
            )
        )


class RPView(
    discord.ui.View,
):
    def __init__(
        self,
        message_id: int,
    ):
        super().__init__(
            timeout=None
        )

        self.message_id = (
            message_id
        )

        self.join_btn = discord.ui.Button(
            label="✅ Join",
            style=discord.ButtonStyle.success,
            custom_id=(
                f"rp_join:{message_id}"
            ),
            row=0,
        )

        self.leave_btn = discord.ui.Button(
            label="⏹ Leave",
            style=discord.ButtonStyle.secondary,
            custom_id=(
                f"rp_leave:{message_id}"
            ),
            row=0,
        )

        self.rejoin_btn = discord.ui.Button(
            label="🔁 Rejoin",
            style=discord.ButtonStyle.secondary,
            custom_id=(
                f"rp_rejoin:{message_id}"
            ),
            row=0,
        )

        self.start_btn = discord.ui.Button(
            label="▶️ Start",
            style=discord.ButtonStyle.primary,
            custom_id=(
                f"rp_start:{message_id}"
            ),
            row=1,
        )

        self.pause_btn = discord.ui.Button(
            label="⏸ Pause",
            style=discord.ButtonStyle.secondary,
            custom_id=(
                f"rp_pause:{message_id}"
            ),
            row=1,
        )

        self.resume_btn = discord.ui.Button(
            label="⏵ Resume",
            style=discord.ButtonStyle.success,
            custom_id=(
                f"rp_resume:{message_id}"
            ),
            row=1,
        )

        self.end_btn = discord.ui.Button(
            label="🏁 End",
            style=discord.ButtonStyle.danger,
            custom_id=(
                f"rp_end:{message_id}"
            ),
            row=2,
        )

        self.join_btn.callback = (
            self.join_cb
        )

        self.leave_btn.callback = (
            self.leave_cb
        )

        self.rejoin_btn.callback = (
            self.rejoin_cb
        )

        self.start_btn.callback = (
            self.start_cb
        )

        self.pause_btn.callback = (
            self.pause_cb
        )

        self.resume_btn.callback = (
            self.resume_cb
        )

        self.end_btn.callback = (
            self.end_cb
        )

        for button in (
            self.join_btn,
            self.leave_btn,
            self.rejoin_btn,
            self.start_btn,
            self.pause_btn,
            self.resume_btn,
            self.end_btn,
        ):
            self.add_item(
                button
            )

    async def join_cb(
        self,
        interaction: discord.Interaction,
    ):
        await interaction.response.send_modal(
            JoinModal(
                self.message_id
            )
        )

    async def leave_cb(
        self,
        interaction: discord.Interaction,
    ):
        now = time.time()

        with db() as conn:
            with conn.cursor() as cur:

                cur.execute("""
                    SELECT
                        last_tick,
                        seconds
                    FROM participants
                    WHERE
                        message_id=%s
                        AND user_id=%s
                """, (
                    self.message_id,
                    interaction.user.id,
                ))

                row = cur.fetchone()

                if not row:
                    await interaction.response.send_message(
                        "You haven’t joined this RP yet.",
                        ephemeral=True,
                    )
                    return

                last_tick = (
                    row[0]
                )

                secs = float(
                    row[1]
                    or 0.0
                )

                if (
                    last_tick is not None
                    and float(
                        last_tick
                    ) >= 0
                ):
                    secs += max(
                        0.0,
                        now
                        - float(
                            last_tick
                        ),
                    )

                cur.execute("""
                    UPDATE participants
                    SET
                        seconds=%s,
                        last_tick=NULL,
                        present=0
                    WHERE
                        message_id=%s
                        AND user_id=%s
                """, (
                    secs,
                    self.message_id,
                    interaction.user.id,
                ))

        await interaction.response.send_message(
            (
                "⏹ You’ve left. "
                "Your personal RP timer is stopped."
            ),
            ephemeral=True,
        )

        await update_tracker_message(
            self.message_id
        )

    async def rejoin_cb(
        self,
        interaction: discord.Interaction,
    ):
        (
            state,
            _,
            _,
            _,
            _,
        ) = get_session(
            self.message_id
        )

        with db() as conn:
            with conn.cursor() as cur:

                cur.execute("""
                    SELECT 1
                    FROM participants
                    WHERE
                        message_id=%s
                        AND user_id=%s
                """, (
                    self.message_id,
                    interaction.user.id,
                ))

                exists = (
                    cur.fetchone()
                    is not None
                )

                if not exists:
                    await interaction.response.send_message(
                        (
                            "You haven’t joined yet. "
                            "Click **✅ Join** first."
                        ),
                        ephemeral=True,
                    )
                    return

                if state != 1:
                    cur.execute("""
                        UPDATE participants
                        SET
                            last_tick=NULL,
                            present=1
                        WHERE
                            message_id=%s
                            AND user_id=%s
                    """, (
                        self.message_id,
                        interaction.user.id,
                    ))

                    await interaction.response.send_message(
                        (
                            "You’re marked present, "
                            "but the session isn’t running."
                        ),
                        ephemeral=True,
                    )

                    await update_tracker_message(
                        self.message_id
                    )

                    return

                now = time.time()

                cur.execute("""
                    UPDATE participants
                    SET
                        last_tick=%s,
                        present=1
                    WHERE
                        message_id=%s
                        AND user_id=%s
                """, (
                    now,
                    self.message_id,
                    interaction.user.id,
                ))

        await interaction.response.send_message(
            (
                "🔁 You’re back in. "
                "Your timer is running again."
            ),
            ephemeral=True,
        )

        await update_tracker_message(
            self.message_id
        )

    async def start_cb(
        self,
        interaction: discord.Interaction,
    ):
        (
            state,
            _,
            run_seconds,
            _,
            _,
        ) = get_session(
            self.message_id
        )

        if state == 1:
            await interaction.response.send_message(
                "Already running.",
                ephemeral=True,
            )
            return

        now = time.time()

        with db() as conn:
            with conn.cursor() as cur:

                cur.execute("""
                    INSERT INTO sessions (
                        message_id,
                        state,
                        started_at,
                        run_seconds,
                        channel_id,
                        guild_id
                    )
                    VALUES (
                        %s,
                        0,
                        NULL,
                        0,
                        %s,
                        %s
                    )
                    ON CONFLICT (
                        message_id
                    )
                    DO NOTHING
                """, (
                    self.message_id,
                    interaction.channel_id,
                    interaction.guild_id,
                ))

                cur.execute("""
                    UPDATE sessions
                    SET
                        state=1,
                        started_at=%s,
                        channel_id=%s,
                        guild_id=%s,
                        run_seconds=%s,
                        ended_at=NULL
                    WHERE message_id=%s
                """, (
                    now,
                    interaction.channel_id,
                    interaction.guild_id,
                    float(
                        run_seconds
                        or 0.0
                    ),
                    self.message_id,
                ))

                cur.execute("""
                    UPDATE participants
                    SET last_tick=%s
                    WHERE
                        message_id=%s
                        AND present=1
                """, (
                    now,
                    self.message_id,
                ))

        await interaction.response.send_message(
            (
                "▶️ Session started. "
                "The guild clock is running."
            ),
            ephemeral=True,
        )

        if interaction.channel is not None:
            await post_rp_status_announcement(
                interaction.channel,
                "start",
                interaction.user,
                self.message_id,
            )

        await update_tracker_message(
            self.message_id
        )

    async def pause_cb(
        self,
        interaction: discord.Interaction,
    ):
        (
            state,
            started_at,
            run_seconds,
            _,
            _,
        ) = get_session(
            self.message_id
        )

        if (
            state != 1
            or started_at is None
        ):
            await interaction.response.send_message(
                "Not currently running.",
                ephemeral=True,
            )
            return

        tick_running_sessions()

        now = time.time()

        new_run = (
            float(
                run_seconds
                or 0.0
            )
            + max(
                0.0,
                now
                - started_at,
            )
        )

        with db() as conn:
            with conn.cursor() as cur:

                cur.execute("""
                    UPDATE sessions
                    SET
                        state=2,
                        started_at=NULL,
                        run_seconds=%s
                    WHERE message_id=%s
                """, (
                    new_run,
                    self.message_id,
                ))

                cur.execute("""
                    UPDATE participants
                    SET last_tick=-1
                    WHERE
                        message_id=%s
                        AND present=1
                        AND last_tick IS NOT NULL
                """, (
                    self.message_id,
                ))

        await interaction.response.send_message(
            "⏸ Session paused. Quills down.",
            ephemeral=True,
        )

        if interaction.channel is not None:
            await post_rp_status_announcement(
                interaction.channel,
                "pause",
                interaction.user,
                self.message_id,
            )

        await update_tracker_message(
            self.message_id
        )

    async def resume_cb(
        self,
        interaction: discord.Interaction,
    ):
        (
            state,
            _,
            run_seconds,
            _,
            _,
        ) = get_session(
            self.message_id
        )

        if state != 2:
            await interaction.response.send_message(
                "Not currently paused.",
                ephemeral=True,
            )
            return

        now = time.time()

        with db() as conn:
            with conn.cursor() as cur:

                cur.execute("""
                    UPDATE sessions
                    SET
                        state=1,
                        started_at=%s,
                        run_seconds=%s
                    WHERE message_id=%s
                """, (
                    now,
                    float(
                        run_seconds
                        or 0.0
                    ),
                    self.message_id,
                ))

                cur.execute("""
                    UPDATE participants
                    SET last_tick=%s
                    WHERE
                        message_id=%s
                        AND present=1
                        AND last_tick=-1
                """, (
                    now,
                    self.message_id,
                ))

        await interaction.response.send_message(
            (
                "⏵ Session resumed. "
                "The guild clock continues."
            ),
            ephemeral=True,
        )

        if interaction.channel is not None:
            await post_rp_status_announcement(
                interaction.channel,
                "resume",
                interaction.user,
                self.message_id,
            )

        await update_tracker_message(
            self.message_id
        )

    async def end_cb(
        self,
        interaction: discord.Interaction,
    ):
        embed = discord.Embed(
            title="⚠️ End RP Confirmation",
            description=(
                "**Are you sure you want to end "
                "the RP for EVERYONE?**\n\n"
                "This will close the ledger "
                "and generate rewards.\n"
                "Click the button below, then "
                "type `yes` to confirm."
            ),
            color=discord.Color.red(),
        )

        await interaction.response.send_message(
            embed=embed,
            view=EndConfirmView(
                self.message_id
            ),
            ephemeral=True,
        )


# =========================
# END SESSION CORE
# =========================
async def end_session_and_post_rewards(
    interaction: discord.Interaction,
    message_id: int,
):
    if not interaction.response.is_done():
        await interaction.response.defer(
            thinking=False
        )

    (
        state,
        started_at,
        run_seconds,
        channel_id,
        guild_id,
    ) = get_session(
        message_id
    )

    if (
        not channel_id
        or not guild_id
    ):
        try:
            await interaction.followup.send(
                (
                    "❌ Could not locate this "
                    "session in the ledger."
                ),
                ephemeral=True,
            )

        except Exception:
            pass

        return

    if (
        state == 1
        and started_at is not None
    ):
        tick_running_sessions()

        now = time.time()

        run_seconds = (
            float(
                run_seconds
                or 0.0
            )
            + max(
                0.0,
                now
                - started_at,
            )
        )

    ended_at = time.time()

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                UPDATE sessions
                SET
                    state=0,
                    started_at=NULL,
                    run_seconds=%s,
                    ended_at=%s
                WHERE message_id=%s
            """, (
                float(
                    run_seconds
                    or 0.0
                ),
                ended_at,
                message_id,
            ))

            cur.execute("""
                UPDATE participants
                SET last_tick=NULL
                WHERE message_id=%s
            """, (
                message_id,
            ))

    if interaction.channel is not None:
        await post_rp_status_announcement(
            interaction.channel,
            "end",
            interaction.user,
            message_id,
        )

    parts = list_participants(
        message_id
    )

    lines = []

    for (
        uid,
        char,
        lvl,
        secs,
        cap,
        reward_dip,
        gp_dip,
        specialist,
    ) in parts:

        hrs = reward_hours(
            secs
        )

        if specialist:

            coin_rate = {
                0: 2,
                1: 4,
                2: 6,
            }.get(
                reward_dip,
                SPECIALIST_RP_COINS_PER_HOUR,
            )

            coins = (
                coin_rate
                * hrs
            )

            gp = (
                SPECIALIST_RP_GP_PER_HOUR
                * hrs
            )

            if DOUBLE_RP_EVENT_ACTIVE:
                gp *= 2

            elif gp_dip:
                gp *= 2

            special_add(
                guild_id,
                uid,
                coins,
            )

            specialist_tags = []

            if reward_dip == 1:
                specialist_tags.append(
                    "🪙 DIP"
                )

            elif reward_dip == 2:
                specialist_tags.append(
                    "🪙 DOUBLE DIP"
                )

            if (
                not DOUBLE_RP_EVENT_ACTIVE
                and gp_dip
            ):
                specialist_tags.append(
                    "GP×2"
                )

            specialist_tag_text = (
                f" *("
                f"{', '.join(specialist_tags)}"
                f")*"
                if specialist_tags
                else ""
            )

            lines.append(
                f"<@{uid}> — "
                f"**{char}** "
                f"(Specialist) — "
                f"**{hrs}h** — "
                f"**{coins}** 🪙, "
                f"**{gp}** gp"
                f"{specialist_tag_text}"
            )

        else:
            gp = (
                gp_per_hour_for_level(
                    lvl
                )
                * hrs
            )

            if DOUBLE_RP_EVENT_ACTIVE:
                gp *= 2

            elif gp_dip:
                gp *= 2

            dip_tags = []

            if not DOUBLE_RP_EVENT_ACTIVE:
                if reward_dip:
                    dip_tags.append(
                        "XP×2"
                    )

                if gp_dip:
                    dip_tags.append(
                        "GP×2"
                    )

            dip_txt = (
                f" *("
                f"{', '.join(dip_tags)}"
                f")*"
                if dip_tags
                else ""
            )

            if (
                lvl >= 20
                or cap
            ):
                keys = hrs

                keys_add(
                    guild_id,
                    uid,
                    keys,
                )

                lines.append(
                    f"<@{uid}> — "
                    f"**{char}** "
                    f"(lvl {lvl}) — "
                    f"**{hrs}h** — "
                    f"**{keys}** 🗝️, "
                    f"**{gp}** gp"
                    f"{dip_txt}"
                )

            else:
                xp = (
                    xp_per_hour_for_level(
                        lvl
                    )
                    * hrs
                )

                if DOUBLE_RP_EVENT_ACTIVE:
                    xp *= 2

                elif reward_dip:
                    xp *= 2

                lines.append(
                    f"<@{uid}> — "
                    f"**{char}** "
                    f"(lvl {lvl}) — "
                    f"**{hrs}h** — "
                    f"**{xp}** xp, "
                    f"**{gp}** gp"
                    f"{dip_txt}"
                )

        if hrs > 0:
            try:
                guild = (
                    interaction.guild
                )

                if guild is not None:
                    member = guild.get_member(
                        uid
                    )

                    if member is None:
                        member = await guild.fetch_member(
                            uid
                        )

                    fresh_role = discord.utils.get(
                        guild.roles,
                        name=FRESH_ADVENTURER_ROLE_NAME,
                    )

                    if (
                        member is not None
                        and fresh_role is not None
                        and fresh_role in member.roles
                    ):
                        await member.remove_roles(
                            fresh_role,
                            reason=(
                                "Completed first RP session"
                            ),
                        )

            except Exception:
                traceback.print_exc()

    if not lines:
        lines = [
            "*(no participants)*"
        ]

    start_link = tracker_url(
        guild_id,
        channel_id,
        message_id,
    )

    end_link = None

    mid_links: List[
        str
    ] = []

    try:
        events = get_session_events(
            message_id
        )

        for (
            event_type,
            event_message_id,
            ev_channel_id,
            ev_guild_id,
        ) in events:

            jump = build_jump_link(
                ev_guild_id,
                ev_channel_id,
                event_message_id,
            )

            if event_type == "start":
                start_link = jump

            elif event_type == "pause":
                mid_links.append(
                    f"Pause: {jump}"
                )

            elif event_type == "resume":
                mid_links.append(
                    f"Resume: {jump}"
                )

            elif event_type == "end":
                end_link = jump

    except Exception:
        traceback.print_exc()

    if end_link is None:
        end_link = tracker_url(
            guild_id,
            channel_id,
            message_id,
        )

    link_lines = [
        f"Start: {start_link}"
    ]

    link_lines.extend(
        mid_links
    )

    link_lines.append(
        f"End: {end_link}"
    )

    record_links = (
        "\n\n"
        + "\n".join(
            link_lines
        )
    )

    first_header = (
        "🏁 **Guild Ledger Closed — Rewards Issued**\n"
        "The registrar tallies the earnings "
        "and stamps the record.\n"
    )

    continuation_header = (
        "🏁 **Guild Ledger Closed — Rewards Continued**\n"
    )

    reward_chunks: List[
        str
    ] = []

    current = first_header

    for line in lines:
        addition = (
            "\n"
            + line
        )

        if (
            len(
                current
            )
            + len(
                addition
            )
            + len(
                record_links
            )
            > 1900
        ):
            reward_chunks.append(
                current
                + record_links
            )

            current = (
                continuation_header
                + "\n"
                + line
            )

        else:
            current += addition

    if current:
        reward_chunks.append(
            current
            + record_links
        )

    for chunk in reward_chunks:
        await interaction.followup.send(
            chunk,
            wait=True,
        )

    await update_tracker_message(
        message_id
    )


# =========================
# SLASH COMMANDS
# =========================
@bot.tree.command(
    name="rpbegin",
    description=(
        "Post and pin the "
        "Adventurer’s Guild RP tracker."
    ),
)
async def rpbegin(
    interaction: discord.Interaction,
):
    await interaction.response.defer(
        thinking=False
    )

    temp = apply_theme(
        discord.Embed(
            title=(
                "📜 Opening a new "
                "Guild Ledger…"
            ),
            description=(
                "Preparing the session log."
            ),
        )
    )

    msg = await interaction.followup.send(
        embed=temp,
        wait=True,
    )

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO sessions (
                    message_id,
                    state,
                    started_at,
                    run_seconds,
                    channel_id,
                    guild_id
                )
                VALUES (
                    %s,
                    0,
                    NULL,
                    0,
                    %s,
                    %s
                )
                ON CONFLICT (
                    message_id
                )
                DO NOTHING
            """, (
                msg.id,
                msg.channel.id,
                interaction.guild_id,
            ))

            cur.execute("""
                UPDATE sessions
                SET
                    channel_id=%s,
                    guild_id=%s
                WHERE message_id=%s
            """, (
                msg.channel.id,
                interaction.guild_id,
                msg.id,
            ))

    view = RPView(
        msg.id
    )

    await msg.edit(
        embed=build_embed(
            msg.id
        ),
        view=view,
    )

    bot.add_view(
        view
    )

    try:
        await msg.pin(
            reason=(
                "Adventurer’s Guild "
                "RP Tracker"
            )
        )

    except Exception:
        pass


@bot.tree.command(
    name="rpend",
    description=(
        "End the active RP session "
        "in this channel/thread."
    ),
)
async def rpend(
    interaction: discord.Interaction,
):
    await interaction.response.defer(
        thinking=False
    )

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT message_id
                FROM sessions
                WHERE
                    channel_id=%s
                    AND guild_id=%s
                    AND state IN (
                        1,
                        2
                    )
                ORDER BY message_id DESC
                LIMIT 1
            """, (
                interaction.channel_id,
                interaction.guild_id,
            ))

            row = cur.fetchone()

    if not row:
        await interaction.followup.send(
            (
                "❌ No active tracker found "
                "in this channel."
            ),
            ephemeral=True,
        )
        return

    await end_session_and_post_rewards(
        interaction,
        int(
            row[0]
        ),
    )


# =========================
# PREFIX COMMAND: !key
# =========================
@bot.command(
    name="key"
)
async def key_cmd(
    ctx: commands.Context,
    amount: Optional[str] = None,
    *,
    reason: Optional[str] = None,
):
    if ctx.guild is None:
        await ctx.send(
            "❌ This command can only be used in a server."
        )
        return

    if amount is None:
        (
            current,
            lifetime,
        ) = keys_get(
            ctx.guild.id,
            ctx.author.id,
        )

        await ctx.send(
            embed=build_key_embed(
                ctx.author,
                current,
                lifetime,
            )
        )
        return

    try:
        delta = int(
            amount.strip()
        )

    except Exception:
        await ctx.send(
            'Usage: `!key` or `!key +3 "Reason"` or `!key -2 "Reason"`'
        )
        return

    if delta == 0:
        await ctx.send(
            "That would change nothing. 🙂"
        )
        return

    if delta > 0:
        keys_add(
            ctx.guild.id,
            ctx.author.id,
            delta,
        )

    else:
        keys_sub(
            ctx.guild.id,
            ctx.author.id,
            abs(
                delta
            ),
        )

    (
        current,
        lifetime,
    ) = keys_get(
        ctx.guild.id,
        ctx.author.id,
    )

    reason_text = (
        reason
        if reason
        else "*No reason provided.*"
    )

    ledger_text = (
        f"Name: {ctx.author.mention}\n"
        f"Keys Earned: {delta:+d} 🗝️\n"
        f"For: {reason_text}"
    )

    await ctx.send(
        ledger_text
    )

    await ctx.send(
        embed=build_key_embed(
            ctx.author,
            current,
            lifetime,
        )
    )


# =========================
# PREFIX COMMAND: !special
# =========================
@bot.command(
    name="special"
)
async def special_cmd(
    ctx: commands.Context,
    amount: Optional[str] = None,
    *,
    reason: Optional[str] = None,
):
    if ctx.guild is None:
        await ctx.send(
            "❌ This command can only be used in a server."
        )
        return

    if amount is None:
        (
            current,
            lifetime,
        ) = special_get(
            ctx.guild.id,
            ctx.author.id,
        )

        await ctx.send(
            embed=build_special_embed(
                ctx.author,
                current,
                lifetime,
            )
        )
        return

    try:
        delta = int(
            amount.strip()
        )

    except Exception:
        await ctx.send(
            'Usage: `!special` or `!special +3 "Reason"` or `!special -2 "Reason"`'
        )
        return

    if delta == 0:
        await ctx.send(
            "That would change nothing. 🙂"
        )
        return

    if delta > 0:
        special_add(
            ctx.guild.id,
            ctx.author.id,
            delta,
        )

    else:
        special_sub(
            ctx.guild.id,
            ctx.author.id,
            abs(
                delta
            ),
        )

    (
        current,
        lifetime,
    ) = special_get(
        ctx.guild.id,
        ctx.author.id,
    )

    reason_text = (
        reason
        if reason
        else "*No reason provided.*"
    )

    ledger_text = (
        f"Name: {ctx.author.mention}\n"
        f"Specialist Coins: "
        f"{delta:+d} 🪙\n"
        f"For: {reason_text}"
    )

    await ctx.send(
        ledger_text
    )

    await ctx.send(
        embed=build_special_embed(
            ctx.author,
            current,
            lifetime,
        )
    )


# =========================
# PREFIX COMMAND: !loot
# =========================
@bot.command(
    name="loot"
)
async def loot_cmd(
    ctx: commands.Context,
    *,
    item_name: Optional[str] = None,
):
    if ctx.guild is None:
        await ctx.send(
            "❌ This command can only be used in a server."
        )
        return

    if not item_name:
        await ctx.send(
            "❌ Usage: `!loot Item Name`\n"
            "Example: `!loot Flame Tongue`"
        )
        return

    query = " ".join(
        item_name
        .strip()
        .lower()
        .split()
    )

    exact_matches = [
        item
        for item in LOOT_DETAILS
        if " ".join(
            item["name"]
            .lower()
            .split()
        ) == query
    ]

    if len(
        exact_matches
    ) == 1:

        item = (
            exact_matches[0]
        )

    elif len(
        exact_matches
    ) > 1:

        matches = (
            exact_matches
        )

        lines = [
            (
                f"• **{entry['name']}** "
                f"— {entry['rarity']}"
            )
            for entry
            in matches[:15]
        ]

        await ctx.send(
            (
                "🔎 **Multiple exact loot "
                "entries were found:**\n\n"
                + "\n".join(
                    lines
                )
            )
        )
        return

    else:
        matches = [
            item
            for item
            in LOOT_DETAILS
            if query
            in item["name"].lower()
        ]

        if not matches:
            await ctx.send(
                f"❌ No loot item found matching `{item_name}`."
            )
            return

        if len(
            matches
        ) > 1:

            shown = (
                matches[:15]
            )

            lines = [
                (
                    f"• **{entry['name']}** "
                    f"— {entry['rarity']}"
                )
                for entry
                in shown
            ]

            extra = ""

            if len(
                matches
            ) > 15:
                extra = (
                    f"\n\n*And "
                    f"{len(matches) - 15} more. "
                    "Use a more specific search.*"
                )

            await ctx.send(
                (
                    "🔎 **Multiple loot items "
                    f"match `{item_name}`:**\n\n"
                    + "\n".join(
                        lines
                    )
                    + extra
                )
            )
            return

        item = (
            matches[0]
        )

    embed = discord.Embed(
        title=(
            f"📦 {item['name']}"
        ),
        color=theme_color(),
    )

    embed.add_field(
        name="Rarity",
        value=item["rarity"],
        inline=False,
    )

    embed.add_field(
        name="Shop Value",
        value=item["shop_value"],
        inline=True,
    )

    embed.add_field(
        name="Minimum Trade Value",
        value=item["min_trade"],
        inline=True,
    )

    embed = apply_theme(
        embed,
        footer_text_override=(
            "Guild Loot Registry"
        ),
    )

    await ctx.send(
        embed=embed
    )


# =========================
# RANDOM LOOT: !newloot
# =========================
@bot.command(
    name="newloot"
)
async def newloot_cmd(
    ctx: commands.Context,
    *,
    rarity: Optional[str] = None,
):
    if ctx.guild is None:
        await ctx.send(
            "❌ This command can only be used in a server."
        )
        return

    if not rarity:
        await ctx.send(
            "❌ Usage: `!newloot rare`"
        )
        return

    rarity_lookup = {
        "common": "Common",
        "uncommon": "Uncommon",
        "rare": "Rare",
        "very rare": "Very Rare",
        "veryrare": "Very Rare",
        "legendary": "Legendary",
        "artifact": "Artifact",
    }

    rarity_key = (
        rarity
        .strip()
        .lower()
    )

    actual_rarity = rarity_lookup.get(
        rarity_key
    )

    if actual_rarity is None:
        await ctx.send(
            "❌ Invalid rarity. Use: "
            "`common`, `uncommon`, `rare`, "
            "`very rare`, `legendary`, or `artifact`."
        )
        return

    item = random_loot(
        actual_rarity
    )

    if item is None:
        await ctx.send(
            f"❌ No items are loaded for **{actual_rarity}**."
        )
        return

    (
        item_name,
        _shop_price,
    ) = item

    await ctx.send(
        f"🎲 **{actual_rarity} Loot:** {item_name}"
    )


# =========================
# STAFF PERMISSIONS
# =========================
PAYDAY_ALLOWED_ROLES = {
    "The Hearth"
}

PAYDAY_STAFF_ROLES = {
    "Guild Ambassador": 5,
    "Stewards": 5,
    "The Hearth": 5,
}


# =========================
# RELOAD LOOT: !reloadloot
# =========================
@bot.command(
    name="reloadloot"
)
async def reloadloot_cmd(
    ctx: commands.Context,
):
    if ctx.guild is None:
        await ctx.send(
            "❌ This command can only be used in a server."
        )
        return

    author_roles = {
        role.name
        for role
        in ctx.author.roles
    }

    if not (
        author_roles
        & PAYDAY_ALLOWED_ROLES
    ):
        await ctx.send(
            "❌ Only The Hearth may reload the loot registry."
        )
        return

    global LOOT_TABLE
    global LOOT_DETAILS

    new_table: Dict[
        str,
        List[
            Tuple[str, str]
        ]
    ] = {}

    new_details: List[
        Dict[str, str]
    ] = []

    for rarity in RARITY_ORDER:
        fn = (
            f"Guild Loot List - "
            f"{rarity}.csv"
        )

        path = _repo_path(
            fn
        )

        new_table[
            rarity
        ] = load_loot_csv(
            path
        )

        new_details.extend(
            load_loot_details_csv(
                path,
                rarity,
            )
        )

    LOOT_TABLE = (
        new_table
    )

    LOOT_DETAILS = (
        new_details
    )

    await ctx.send(
        (
            "✅ Loot registry reloaded.\n"
            f"**{len(LOOT_DETAILS)}** "
            "detailed items loaded."
        )
    )


# =========================
# STAFF PAYDAY: !payday
# =========================
@bot.command(
    name="payday"
)
async def payday_cmd(
    ctx: commands.Context,
):
    if ctx.guild is None:
        await ctx.send(
            "❌ This command can only be used in a server."
        )
        return

    author_roles = {
        role.name
        for role
        in ctx.author.roles
    }

    if not (
        author_roles
        & PAYDAY_ALLOWED_ROLES
    ):
        await ctx.send(
            "❌ Only The Hearth may run staff payday."
        )
        return

    paid_users = {}

    async for member in (
        ctx.guild.fetch_members(
            limit=None
        )
    ):
        if member.bot:
            continue

        member_role_names = {
            role.name
            for role
            in member.roles
        }

        amount = 0
        matched_roles = []

        for (
            role_name,
            role_amount,
        ) in PAYDAY_STAFF_ROLES.items():

            if (
                role_name
                in member_role_names
            ):
                amount += (
                    role_amount
                )

                matched_roles.append(
                    role_name
                )

        if amount > 0:
            paid_users[
                member.id
            ] = {
                "member": member,
                "amount": amount,
                "roles": matched_roles,
            }

    if not paid_users:
        await ctx.send(
            (
                "❌ No eligible staff found. "
                "Make sure the bot has "
                "**Server Members Intent** enabled "
                "in the Discord Developer Portal."
            )
        )
        return

    for data in paid_users.values():
        keys_add(
            ctx.guild.id,
            data["member"].id,
            data["amount"],
        )

    lines = [
        (
            f"{data['member'].mention} — "
            f"**+{data['amount']}** 🗝️ "
            f"*({', '.join(data['roles'])})*"
        )
        for data
        in paid_users.values()
    ]

    out = (
        "🗝️ **Staff Payday Issued**\n"
        "The Guild Registrar has paid "
        "eligible staff.\n\n"
        + "\n".join(
            lines
        )
    )

    if len(
        out
    ) > 1900:
        await ctx.send(
            (
                "🗝️ **Staff Payday Issued**\n"
                f"Paid **{len(paid_users)}** "
                "eligible staff, but the list "
                "was too long to display."
            )
        )
        return

    await ctx.send(
        out
    )


# =========================
# USER ID PARSER
# =========================
def parse_user_id(
    token: str,
) -> Optional[int]:

    t = token.strip()

    if (
        t.startswith("<@")
        and t.endswith(">")
    ):
        t = t[2:-1]

        if t.startswith("!"):
            t = t[1:]

    if t.isdigit():
        return int(
            t
        )

    return None


# =========================
# RP ACTIVITY TRACKING
# !track
# =========================
TRACK_TIMEZONE = ZoneInfo(
    os.getenv(
        "TRACK_TIMEZONE",
        "America/Chicago",
    )
)


TRACK_DATE_RANGE_RE = re.compile(
    r"(\d{1,2}/\d{1,2}/\d{2,4})"
    r"\s*-\s*"
    r"(\d{1,2}/\d{1,2}/\d{2,4})"
    r"\s*$"
)


def parse_track_date(
    value: str,
):
    value = (
        value.strip()
    )

    for fmt in (
        "%m/%d/%y",
        "%m/%d/%Y",
    ):
        try:
            return datetime.strptime(
                value,
                fmt,
            ).date()

        except ValueError:
            continue

    raise ValueError(
        f"Invalid date: {value}"
    )


def get_track_date_range(
    start_text: str,
    end_text: str,
):
    start_date = parse_track_date(
        start_text
    )

    end_date = parse_track_date(
        end_text
    )

    if end_date < start_date:
        raise ValueError(
            "End date cannot be before start date."
        )

    start_dt = datetime(
        start_date.year,
        start_date.month,
        start_date.day,
        tzinfo=TRACK_TIMEZONE,
    )

    next_day = (
        end_date
        + timedelta(
            days=1
        )
    )

    end_dt = datetime(
        next_day.year,
        next_day.month,
        next_day.day,
        tzinfo=TRACK_TIMEZONE,
    )

    return (
        start_date,
        end_date,
        start_dt.timestamp(),
        end_dt.timestamp(),
    )
