"""Grandmaster Marketplace add-on. Register with setup_marketplace(bot, db, apply_theme, rarity_order)."""
import asyncio
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import discord
from discord.ext import commands, tasks

LOG = logging.getLogger(__name__)
STAFF_ROLES = {'The Hearth', 'Steward Supreme', 'Steward'}
RARITIES = ('Common', 'Uncommon', 'Rare', 'Very Rare', 'Legendary', 'Artifact')
LOCKS = {}


def init_tables(db):
    with db() as conn, conn.cursor() as cur:
        cur.execute('''CREATE TABLE IF NOT EXISTS marketplace_listings (
            id BIGSERIAL PRIMARY KEY, guild_id BIGINT NOT NULL,
            seller_id BIGINT NOT NULL, character TEXT NOT NULL,
            item_name TEXT NOT NULL, rarity TEXT NOT NULL,
            asking_gp NUMERIC(18,2) NOT NULL CHECK (asking_gp > 0),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )''')
        cur.execute('''CREATE INDEX IF NOT EXISTS marketplace_listings_guild
                       ON marketplace_listings(guild_id, rarity, item_name)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS marketplace_catalogs (
            guild_id BIGINT PRIMARY KEY, channel_id BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )''')
        cur.execute('''CREATE TABLE IF NOT EXISTS marketplace_catalog_messages (
            guild_id BIGINT NOT NULL, position INT NOT NULL,
            message_id BIGINT NOT NULL,
            PRIMARY KEY(guild_id, position)
        )''')
        cur.execute('''CREATE TABLE IF NOT EXISTS marketplace_audit (
            id BIGSERIAL PRIMARY KEY, guild_id BIGINT NOT NULL,
            listing_id BIGINT NOT NULL, actor_id BIGINT NOT NULL,
            seller_id BIGINT NOT NULL, action TEXT NOT NULL,
            item_name TEXT NOT NULL, details TEXT,
            occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )''')


def staff(ctx):
    return (ctx.guild is not None and (ctx.author.id == ctx.guild.owner_id or
            any(r.name in STAFF_ROLES for r in getattr(ctx.author, 'roles', ())) or
            ctx.author.guild_permissions.administrator))


def clean(value, limit=100):
    value = re.sub(r'\s+', ' ', str(value)).strip()
    value = discord.utils.escape_markdown(value)
    value = discord.utils.escape_mentions(value)
    return value[:limit]


def parse_gp(raw):
    raw = raw.strip().lower().replace(',', '').replace('gp', '').strip()
    try:
        gp = Decimal(raw)
        if not gp.is_finite() or gp <= 0 or gp > Decimal('9999999999999999.99'):
            return None
        if gp.as_tuple().exponent < -2:
            return None
        return gp.quantize(Decimal('0.01'))
    except (InvalidOperation, ValueError):
        return None


def gp_text(value):
    return f'{value:,.2f}'.rstrip('0').rstrip('.') + ' gp'


def get_listing(db, guild_id, listing_id):
    with db() as conn, conn.cursor() as cur:
        cur.execute('''SELECT id, seller_id, character, item_name, rarity, asking_gp, created_at
                       FROM marketplace_listings WHERE guild_id=%s AND id=%s''',
                    (guild_id, listing_id))
        return cur.fetchone()


def get_listings(db, guild_id):
    with db() as conn, conn.cursor() as cur:
        cur.execute('''SELECT id, seller_id, character, item_name, rarity, asking_gp, created_at
                       FROM marketplace_listings WHERE guild_id=%s''', (guild_id,))
        return cur.fetchall()


def make_pages(rows):
    """One or more short embeds per rarity, within Discord's limits."""
    now = datetime.now(timezone.utc)
    pages = []
    for rarity in RARITIES:
        group = sorted((r for r in rows if r[4] == rarity), key=lambda r: (r[3].casefold(), r[0]))
        if not group:
            continue
        lines = []
        for listing_id, uid, character, item, _, price, created in group:
            created = created if created.tzinfo else created.replace(tzinfo=timezone.utc)
            days = max(0, int((now - created).total_seconds() // 86400))
            line = (f'`#{listing_id}` **{item}** — {character} (<@{uid}>) — '
                    f'**{gp_text(price)}** — {days} day{"s" if days != 1 else ""} listed')
            lines.append(line)
        # Description <=4096, allow ample space for heading and potential markdown.
        chunk = []
        size = 0
        part = 1
        for line in lines:
            if chunk and size + len(line) + 1 > 3400:
                pages.append((rarity, part, '\n'.join(chunk)))
                chunk, size = [], 0
                part += 1
            chunk.append(line)
            size += len(line) + 1
        if chunk:
            pages.append((rarity, part, '\n'.join(chunk)))
    if not pages:
        pages = [('Marketplace', 1, '*No items are currently listed.*')]
    return pages


def setup_marketplace(bot, db, apply_theme, rarity_order=None):
    """Call once after creating `bot`; requires existing db() and apply_theme()."""
    init_tables(db)

    def catalog_lock(guild_id):
        return LOCKS.setdefault(guild_id, asyncio.Lock())

    async def refresh(guild_id):
        async with catalog_lock(guild_id):
            with db() as conn, conn.cursor() as cur:
                cur.execute('SELECT channel_id FROM marketplace_catalogs WHERE guild_id=%s', (guild_id,))
                row = cur.fetchone()
                if not row:
                    return
                channel_id = int(row[0])
                cur.execute('''SELECT position, message_id FROM marketplace_catalog_messages
                               WHERE guild_id=%s ORDER BY position''', (guild_id,))
                existing = [(int(pos), int(mid)) for pos, mid in cur.fetchall()]
            channel = bot.get_channel(channel_id)
            if channel is None:
                channel = await bot.fetch_channel(channel_id)
            if not hasattr(channel, 'send') or not hasattr(channel, 'fetch_message'):
                raise RuntimeError('Marketplace channel does not support messages.')
            pages = make_pages(get_listings(db, guild_id))
            for position, (rarity, part, description) in enumerate(pages):
                title = '⚖️ The Grandmaster’s Marketplace'
                embed = discord.Embed(title=title, description=description)
                embed.add_field(name='Rarity', value=f'{rarity}' + (f' • Page {part}' if part > 1 else ''), inline=False)
                embed = apply_theme(embed, footer_text_override='Listings refresh automatically • Use !market help for commands')
                msg = None
                if position < len(existing):
                    try:
                        msg = await channel.fetch_message(existing[position][1])
                        await msg.edit(embed=embed, allowed_mentions=discord.AllowedMentions.none())
                    except discord.NotFound:
                        msg = None
                if msg is None:
                    msg = await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
                with db() as conn, conn.cursor() as cur:
                    cur.execute('''INSERT INTO marketplace_catalog_messages(guild_id,position,message_id)
                                   VALUES (%s,%s,%s) ON CONFLICT(guild_id,position)
                                   DO UPDATE SET message_id=EXCLUDED.message_id''', (guild_id, position, msg.id))
            for position, message_id in existing[len(pages):]:
                try:
                    msg = await channel.fetch_message(message_id)
                    await msg.delete()
                except discord.NotFound:
                    pass
                with db() as conn, conn.cursor() as cur:
                    cur.execute('DELETE FROM marketplace_catalog_messages WHERE guild_id=%s AND position=%s', (guild_id, position))
            with db() as conn, conn.cursor() as cur:
                cur.execute('UPDATE marketplace_catalogs SET updated_at=NOW() WHERE guild_id=%s', (guild_id,))

    async def update_or_warn(ctx):
        try:
            await refresh(ctx.guild.id)
        except Exception:
            LOG.exception('Marketplace catalog update failed for guild %s', ctx.guild.id)
            await ctx.send('⚠️ Listing saved, but the catalog could not refresh. Staff can retry with `!market refresh`.')

    @bot.group(name='market', invoke_without_command=True)
    async def market(ctx):
        await ctx.send('📜 Use `!market help` for marketplace commands. The catalog updates automatically.')

    @market.command(name='help')
    async def market_help(ctx):
        e = discord.Embed(title='⚖️ Grandmaster Marketplace — Command Guide', description=(
            '**Browse:** View the permanent marketplace catalog.\n\n'
            '**List:** `!sell Rarity | Item Name | Character Name | Price`\n'
            'Example: `!sell Very Rare | Flame Tongue | Rook | 15000`\n\n'
            '**Your listings:** `!mylistings`\n'
            '**Change price:** `!marketprice ID PRICE`\n'
            '**Mark sold:** `!sold ID`\n'
            '**Remove unsold:** `!delist ID`\n\n'
            '**Staff:** `!market setup`, `!market move CHANNEL_ID`, `!market refresh`, `!marketremove ID`\n\n'
            'Listings do not expire. Sales and payments are arranged between players. '
            'Follow server trading and level-bracket rules.'))
        await ctx.send(embed=apply_theme(e, footer_text_override='Grandmaster Marketplace • Player Guide'))

    @market.command(name='setup')
    @commands.guild_only()
    async def market_setup(ctx):
        if not staff(ctx):
            return await ctx.send('❌ Staff only.')
        if not hasattr(ctx.channel, 'fetch_message'):
            return await ctx.send('❌ Use a standard text channel for the catalog.')
        with db() as conn, conn.cursor() as cur:
            cur.execute('SELECT channel_id FROM marketplace_catalogs WHERE guild_id=%s', (ctx.guild.id,))
            old = cur.fetchone()
            if old and int(old[0]) != ctx.channel.id:
                return await ctx.send('❌ A catalog already exists in another channel. Ask for migration support before moving it.')
            cur.execute('''INSERT INTO marketplace_catalogs(guild_id,channel_id)
                           VALUES (%s,%s) ON CONFLICT(guild_id) DO NOTHING''', (ctx.guild.id, ctx.channel.id))
        try:
            await refresh(ctx.guild.id)
            await ctx.send('✅ Marketplace catalog initialized here. It will update automatically.')
        except Exception:
            LOG.exception('Marketplace setup failed')
            await ctx.send('❌ Catalog setup failed. Check Grandmaster’s channel permissions and Railway logs.')

    @market.command(name='move')
    @commands.guild_only()
    async def market_move(ctx, channel_id: int):
        """Move the catalog to a text channel in this server, retaining listings."""
        if not staff(ctx):
            return await ctx.send('❌ Staff only.')
        channel = ctx.guild.get_channel(channel_id)
        if channel is None:
            try:
                channel = await ctx.guild.fetch_channel(channel_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                return await ctx.send('❌ Channel not found in this server or not accessible.')
        if not isinstance(channel, discord.TextChannel):
            return await ctx.send('❌ Choose a standard server text channel.')
        me = ctx.guild.me
        permissions = channel.permissions_for(me)
        if not (permissions.view_channel and permissions.send_messages and
                permissions.embed_links and permissions.read_message_history):
            return await ctx.send('❌ Grandmaster needs View Channel, Send Messages, Embed Links, and Read Message History there.')
        with db() as conn, conn.cursor() as cur:
            cur.execute('SELECT channel_id FROM marketplace_catalogs WHERE guild_id=%s', (ctx.guild.id,))
            old = cur.fetchone()
            if old is None:
                return await ctx.send('❌ No marketplace exists yet. Run `!market setup` first.')
            old_channel_id = int(old[0])
            if old_channel_id == channel_id:
                return await ctx.send('ℹ️ The marketplace already uses that channel.')
            cur.execute('SELECT message_id FROM marketplace_catalog_messages WHERE guild_id=%s', (ctx.guild.id,))
            old_message_ids = [int(row[0]) for row in cur.fetchall()]
            cur.execute('UPDATE marketplace_catalogs SET channel_id=%s WHERE guild_id=%s', (channel_id, ctx.guild.id))
            cur.execute('DELETE FROM marketplace_catalog_messages WHERE guild_id=%s', (ctx.guild.id,))
        try:
            await refresh(ctx.guild.id)
        except Exception:
            LOG.exception('Marketplace move failed for guild %s', ctx.guild.id)
            with db() as conn, conn.cursor() as cur:
                cur.execute('UPDATE marketplace_catalogs SET channel_id=%s WHERE guild_id=%s', (old_channel_id, ctx.guild.id))
                cur.execute('DELETE FROM marketplace_catalog_messages WHERE guild_id=%s', (ctx.guild.id))
                for position, message_id in enumerate(old_message_ids):
                    cur.execute('INSERT INTO marketplace_catalog_messages(guild_id,position,message_id) VALUES (%s,%s,%s)', (ctx.guild.id, position, message_id))
            return await ctx.send('❌ Move failed; the previous catalog remains configured. Check Railway logs.')
        try:
            old_channel = ctx.guild.get_channel(old_channel_id) or await bot.fetch_channel(old_channel_id)
            for message_id in old_message_ids:
                try:
                    old_message = await old_channel.fetch_message(message_id)
                    await old_message.delete()
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    LOG.warning('Could not delete old marketplace message %s', message_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            LOG.warning('Could not access old marketplace channel %s for cleanup', old_channel_id)
        await ctx.send(f'✅ Marketplace moved to {channel.mention}. Existing listings were preserved.')

    @market.command(name='refresh')
    @commands.guild_only()
    async def market_refresh(ctx):
        if not staff(ctx):
            return await ctx.send('❌ Staff only.')
        try:
            await refresh(ctx.guild.id)
            await ctx.send('✅ Marketplace catalog refreshed.')
        except Exception:
            LOG.exception('Marketplace refresh failed')
            await ctx.send('❌ Refresh failed. Check Railway logs and channel permissions.')

    @bot.command(name='sell')
    @commands.guild_only()
    async def sell(ctx, *, details: str = ''):
        parts = [p.strip() for p in details.split('|')]
        if len(parts) != 4:
            return await ctx.send('❌ Usage: `!sell Very Rare | Flame Tongue | Rook | 15000`')
        rarity = next((r for r in RARITIES if r.casefold().replace(' ', '') == parts[0].casefold().replace(' ', '')), None)
        item, character = clean(parts[1], 100), clean(parts[2], 64)
        price = parse_gp(parts[3])
        if not rarity or not item or not character or price is None:
            return await ctx.send('❌ Invalid rarity, item, character, or price. Price must be positive GP (up to 2 decimals).')
        with db() as conn, conn.cursor() as cur:
            cur.execute('''INSERT INTO marketplace_listings(guild_id,seller_id,character,item_name,rarity,asking_gp)
                           VALUES (%s,%s,%s,%s,%s,%s) RETURNING id''',
                        (ctx.guild.id, ctx.author.id, character, item, rarity, price))
            listing_id = cur.fetchone()[0]
        await ctx.send(f'✅ Listed **{item}** for **{gp_text(price)}** as listing `#{listing_id}`.')
        await update_or_warn(ctx)

    @bot.command(name='mylistings')
    @commands.guild_only()
    async def mylistings(ctx):
        with db() as conn, conn.cursor() as cur:
            cur.execute('''SELECT id,item_name,rarity,character,asking_gp FROM marketplace_listings
                           WHERE guild_id=%s AND seller_id=%s ORDER BY id''', (ctx.guild.id, ctx.author.id))
            rows = cur.fetchall()
        if not rows:
            return await ctx.send('📭 You have no active marketplace listings.')
        lines = [f'`#{lid}` **{item}** ({rarity}) — {character} — {gp_text(price)}' for lid,item,rarity,character,price in rows]
        chunk = '📜 **Your Marketplace Listings**\n'
        for line in lines:
            if len(chunk) + len(line) + 1 > 1850:
                await ctx.send(chunk, allowed_mentions=discord.AllowedMentions.none())
                chunk = ''
            chunk += line + '\n'
        if chunk:
            await ctx.send(chunk, allowed_mentions=discord.AllowedMentions.none())

    async def modify(ctx, listing_id, action, new_price=None):
        try:
            lid = int(listing_id)
            if lid <= 0:
                raise ValueError
        except (TypeError, ValueError):
            return await ctx.send('❌ Provide a valid listing ID. See `!mylistings`.')
        # Atomic ownership check and mutation; row lock avoids concurrent duplicate sales/removals.
        with db() as conn, conn.cursor() as cur:
            cur.execute('''SELECT seller_id,item_name,asking_gp FROM marketplace_listings
                           WHERE guild_id=%s AND id=%s FOR UPDATE''', (ctx.guild.id, lid))
            row = cur.fetchone()
            if not row:
                return await ctx.send('❌ Listing not found.')
            seller_id, item, old_price = row
            if action == 'staffremove':
                if not staff(ctx):
                    return await ctx.send('❌ Staff only.')
            elif seller_id != ctx.author.id:
                return await ctx.send('❌ You can only manage your own listings.')
            if action == 'price':
                cur.execute('''UPDATE marketplace_listings SET asking_gp=%s
                               WHERE guild_id=%s AND id=%s''', (new_price, ctx.guild.id, lid))
                details = f'{gp_text(old_price)} -> {gp_text(new_price)}'
            else:
                cur.execute('DELETE FROM marketplace_listings WHERE guild_id=%s AND id=%s', (ctx.guild.id, lid))
                details = None
            cur.execute('''INSERT INTO marketplace_audit(guild_id,listing_id,actor_id,seller_id,action,item_name,details)
                           VALUES (%s,%s,%s,%s,%s,%s,%s)''',
                        (ctx.guild.id, lid, ctx.author.id, seller_id, action, item, details))
        labels = {'price':'Price updated', 'sold':'Marked sold', 'delist':'Listing removed', 'staffremove':'Staff removed listing'}
        await ctx.send(f'✅ {labels[action]}: `#{lid}` — **{item}**.')
        await update_or_warn(ctx)

    @bot.command(name='marketprice')
    @commands.guild_only()
    async def marketprice(ctx, listing_id: str = '', *, price: str = ''):
        amount = parse_gp(price) if price else None
        if amount is None:
            return await ctx.send('❌ Usage: `!marketprice 12 18000`')
        await modify(ctx, listing_id, 'price', amount)

    @bot.command(name='sold')
    @commands.guild_only()
    async def sold(ctx, listing_id: str = ''):
        await modify(ctx, listing_id, 'sold')

    @bot.command(name='delist')
    @commands.guild_only()
    async def delist(ctx, listing_id: str = ''):
        await modify(ctx, listing_id, 'delist')

    @bot.command(name='marketremove')
    @commands.guild_only()
    async def marketremove(ctx, listing_id: str = ''):
        if not staff(ctx):
            return await ctx.send('❌ Staff only.')
        await modify(ctx, listing_id, 'staffremove')

    @tasks.loop(hours=1)
    async def market_daily_refresh():
        with db() as conn, conn.cursor() as cur:
            cur.execute('SELECT guild_id FROM marketplace_catalogs')
            guilds = [int(row[0]) for row in cur.fetchall()]
        for gid in guilds:
            try:
                await refresh(gid)
            except Exception:
                LOG.exception('Scheduled marketplace refresh failed for %s', gid)

    @market_daily_refresh.before_loop
    async def wait_for_bot():
        await bot.wait_until_ready()

    @bot.listen("on_ready")
    async def marketplace_start_refresh():
        if not market_daily_refresh.is_running():
            market_daily_refresh.start()

    return refresh
