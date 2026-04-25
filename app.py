"""
Grow a Garden - Stock Monitoring System
Single-file app: FastAPI web server + Discord bot + WebSocket listener
Deploy on Render.com with environment variables: DISCORD_TOKEN, PUBLIC_URL, BOT_OWNER_ID
"""

import asyncio
import json
import logging
import os
import re
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import discord
from discord import app_commands
from discord.ext import tasks
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("gag-monitor")

# ─────────────────────────────────────────────
# Environment
# ─────────────────────────────────────────────
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID", "0"))
PUBLIC_URL = os.getenv("PUBLIC_URL", "http://localhost:8000")
DB_PATH = os.getenv("DB_PATH", "/tmp/gag_monitor.db")
GAME_WS_URL = "wss://api.jstudio.ai/grow-a-garden/ws"

# ─────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            discord_id TEXT PRIMARY KEY,
            notification_channel TEXT,
            notification_role TEXT
        );

        CREATE TABLE IF NOT EXISTS watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            item_name TEXT NOT NULL,
            category TEXT,
            priority TEXT DEFAULT 'medium',
            wildcard_pattern TEXT,
            enabled INTEGER DEFAULT 1,
            UNIQUE(user_id, item_name)
        );

        CREATE TABLE IF NOT EXISTS stock_cache (
            item_name TEXT PRIMARY KEY,
            category TEXT,
            price TEXT,
            in_stock INTEGER DEFAULT 0,
            quantity INTEGER DEFAULT 0,
            last_seen TEXT,
            last_updated TEXT
        );

        CREATE TABLE IF NOT EXISTS events (
            event_name TEXT PRIMARY KEY,
            event_type TEXT,
            active INTEGER DEFAULT 1,
            data_json TEXT,
            detected_at TEXT
        );

        CREATE TABLE IF NOT EXISTS notification_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            item_name TEXT,
            sent_at TEXT
        );

        CREATE TABLE IF NOT EXISTS future_watches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            pattern TEXT,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS error_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT,
            message TEXT,
            logged_at TEXT
        );
    """)
    conn.commit()

    # Seed known Easter 2026 event data
    events = [
        ("Evil Bunny (Part 2)", "easter", 1, json.dumps({
            "description": "Easter Part 2: Evil Bunny boss encounter",
            "items": [
                {"name": "Easter Sprout", "price": "~$1.1M", "drop_chance": "0.25% from Evil Bunny"},
                {"name": "Blue Candy Lollipop", "price": "$40k-$80k", "note": "Size dependent"},
                {"name": "Easter Eggmelon", "price": "~$195k"},
                {"name": "Stork Pet", "type": "pet"},
                {"name": "Orchid Mantis Pet", "type": "pet"},
            ],
            "mechanic": "Sacrifice items to summon Evil Bunny"
        })),
        ("Candy Packaging (Part 3)", "easter", 1, json.dumps({
            "description": "Easter Part 3: Candy Packaging with Wally Wrapper NPC",
            "npc": "Wally Wrapper",
            "tiers": {"individual": 28, "team": 12},
            "items": [
                {"name": "Peryton Pet", "drop_chance": "0.5%"},
                {"name": "Spring Bee Pet", "drop_chance": "55%"},
                {"name": "Jerboa Pet", "drop_chance": "35%"},
                {"name": "Nyala Pet", "drop_chance": "9%"},
                {"name": "Gilded Chocolate variants"},
            ]
        })),
        ("Egg War (Hourly)", "easter", 1, json.dumps({
            "description": "Hourly Egg War event",
            "mechanic": "10 eggs spawn each hour, steal mechanic active",
            "rewards": {
                "Divine": "5 Springtide + 10 Golden + Marshmallow Root",
                "Legendary": "5k Choc + 2 Sprinklers + 2 Packs + 2 Golden",
                "Mythical": "10k Choc + Springtide + Pack + 3 Golden",
                "Rare": "1.25k Choc + Crate + Sprinkler + Pack + Golden",
            }
        })),
        ("Training Quests (Finale)", "easter", 1, json.dumps({
            "description": "Easter Finale: Training Quests with Commander Carrot",
            "npc": "Commander Carrot",
            "reset_hours": 8,
            "tiers": {"individual": 20, "team_max_players": 4},
            "pets": [{"name": "Lyrebird", "ability": "Song of Lyre - chance for double harvest and larger fruits"}],
        })),
        ("Candy Blossom", "easter", 1, json.dumps({
            "description": "Candy Blossom event extended to April 30, 2026",
            "reward": "50 Golden Eggs + Blossom Shard",
            "note": "Costs increase each time purchased",
            "end_date": "April 30, 2026"
        })),
    ]
    conn = get_db()
    c = conn.cursor()
    for ev in events:
        c.execute("""
            INSERT OR IGNORE INTO events (event_name, event_type, active, data_json, detected_at)
            VALUES (?, ?, ?, ?, ?)
        """, (ev[0], ev[1], ev[2], ev[3], datetime.now(timezone.utc).isoformat()))
    conn.commit()

def log_error(level: str, message: str):
    conn = get_db()
    conn.execute(
        "INSERT INTO error_log (level, message, logged_at) VALUES (?, ?, ?)",
        (level, message, datetime.now(timezone.utc).isoformat())
    )
    conn.commit()
    if level == "ERROR":
        log.error(message)
    else:
        log.warning(message)

# ─────────────────────────────────────────────
# In-memory state for WebSocket broadcast
# ─────────────────────────────────────────────
connected_ws_clients: list[WebSocket] = []

async def broadcast_stock_update(data: dict):
    dead = []
    for ws in connected_ws_clients:
        try:
            await ws.send_json(data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        connected_ws_clients.remove(ws)

# ─────────────────────────────────────────────
# Game WebSocket Listener
# ─────────────────────────────────────────────
KNOWN_CATEGORIES = {"seeds", "gear", "eggs", "event", "easter"}
_game_ws_task: Optional[asyncio.Task] = None

async def game_ws_listener(bot: discord.Client):
    """Connects to game WebSocket, parses stock data, fires Discord notifications."""
    global _game_ws_task
    while True:
        try:
            log.info(f"Connecting to game WebSocket: {GAME_WS_URL}")
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(GAME_WS_URL, heartbeat=30) as ws:
                    log.info("Game WebSocket connected.")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await handle_game_message(msg.data, bot)
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            log.warning("Game WebSocket closed/error. Reconnecting...")
                            break
        except Exception as e:
            log_error("ERROR", f"Game WebSocket error: {e}")
        log.info("Reconnecting to game WebSocket in 5 seconds...")
        await asyncio.sleep(5)

async def handle_game_message(raw: str, bot: discord.Client):
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return

    msg_type = data.get("type", "unknown")

    if msg_type in ("stock_refresh", "stock_update"):
        await process_stock_update(data, bot)
    elif msg_type == "weather_update":
        pass  # Future: weather notifications
    elif msg_type == "event_shop":
        await process_event_shop(data, bot)
    else:
        log_error("WARNING", f"Unknown message type from game WS: {msg_type} | data: {raw[:200]}")
        await notify_owner(bot, f"⚠️ Unknown game WS message type: `{msg_type}`\n```{raw[:500]}```")

async def process_stock_update(data: dict, bot: discord.Client):
    items = data.get("items", data.get("stock", []))
    if not items:
        return

    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()
    newly_stocked: list[dict] = []

    for item in items:
        name = item.get("name", "Unknown")
        category = (item.get("category") or item.get("type", "unknown")).lower()
        price = str(item.get("price", item.get("cost", "")))
        in_stock = int(bool(item.get("inStock", item.get("in_stock", True))))
        quantity = int(item.get("quantity", item.get("qty", 0)))

        # Auto-detect unknown categories
        if category not in KNOWN_CATEGORIES and category != "unknown":
            KNOWN_CATEGORIES.add(category)
            await notify_owner(bot, f"🆕 New category detected: `{category}` from item `{name}`")

        prev = conn.execute(
            "SELECT in_stock FROM stock_cache WHERE item_name=?", (name,)
        ).fetchone()

        was_stocked = bool(prev and prev["in_stock"])
        now_stocked = bool(in_stock)

        conn.execute("""
            INSERT INTO stock_cache (item_name, category, price, in_stock, quantity, last_seen, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(item_name) DO UPDATE SET
                category=excluded.category, price=excluded.price,
                in_stock=excluded.in_stock, quantity=excluded.quantity,
                last_updated=excluded.last_updated,
                last_seen=CASE WHEN excluded.in_stock=1 THEN excluded.last_seen ELSE last_seen END
        """, (name, category, price, in_stock, quantity, now if in_stock else None, now))

        if now_stocked and not was_stocked:
            newly_stocked.append({"name": name, "category": category, "price": price})

    conn.commit()

    if newly_stocked:
        await broadcast_stock_update({"type": "stock_refresh", "items": newly_stocked, "timestamp": now})
        await fire_watchlist_notifications(bot, newly_stocked)

async def process_event_shop(data: dict, bot: discord.Client):
    event_name = data.get("event", data.get("name", "Unknown Event"))
    conn = get_db()
    existing = conn.execute("SELECT event_name FROM events WHERE event_name=?", (event_name,)).fetchone()
    if not existing:
        conn.execute("""
            INSERT INTO events (event_name, event_type, active, data_json, detected_at)
            VALUES (?, 'auto_detected', 1, ?, ?)
        """, (event_name, json.dumps(data), datetime.now(timezone.utc).isoformat()))
        conn.commit()
        await notify_owner(bot, f"🎉 New event shop detected: `{event_name}`")
        await check_future_event_watches(bot, event_name)

async def fire_watchlist_notifications(bot: discord.Client, newly_stocked: list[dict]):
    conn = get_db()
    for item in newly_stocked:
        name_lower = item["name"].lower()
        category = item["category"]

        watchers = conn.execute("""
            SELECT w.user_id, w.priority, w.wildcard_pattern, u.notification_channel, u.notification_role
            FROM watchlist w
            LEFT JOIN users u ON w.user_id = u.discord_id
            WHERE w.enabled=1 AND (
                lower(w.item_name) = ? OR
                lower(w.item_name) = ? OR
                w.category = ?
            )
        """, (name_lower, item["name"].lower(), category)).fetchall()

        # Also check wildcards
        wildcard_watchers = conn.execute("""
            SELECT w.user_id, w.priority, w.wildcard_pattern, u.notification_channel, u.notification_role
            FROM watchlist w
            LEFT JOIN users u ON w.user_id = u.discord_id
            WHERE w.enabled=1 AND w.wildcard_pattern IS NOT NULL
        """).fetchall()

        for ww in wildcard_watchers:
            pattern = ww["wildcard_pattern"].replace("*", ".*").lower()
            if re.search(pattern, name_lower):
                watchers = list(watchers) + [ww]

        notified = set()
        for watcher in watchers:
            uid = watcher["user_id"]
            if uid in notified:
                continue
            notified.add(uid)

            priority = watcher["priority"] or "medium"
            channel_id = watcher["notification_channel"]
            role_id = watcher["notification_role"]

            price_str = f" — 💰 {item['price']}" if item.get("price") else ""
            category_str = item["category"].upper()

            embed = discord.Embed(
                title=f"{'🟡' if priority == 'high' else '🔵'} {item['name']} is now in stock!",
                description=f"**Category:** {category_str}{price_str}",
                color=discord.Color.gold() if priority == "high" else discord.Color.blue(),
                timestamp=datetime.now(timezone.utc),
            )
            embed.set_footer(text="Grow a Garden Monitor")

            # Send to notification channel
            if channel_id:
                try:
                    channel = bot.get_channel(int(channel_id))
                    if channel:
                        mention = f"<@&{role_id}> " if role_id else ""
                        await channel.send(f"{mention}", embed=embed)
                except Exception as e:
                    log_error("ERROR", f"Failed to send channel notification: {e}")

            # DM for high priority
            if priority == "high":
                try:
                    user = await bot.fetch_user(int(uid))
                    await user.send(embed=embed)
                except Exception:
                    pass

            conn.execute(
                "INSERT INTO notification_log (user_id, item_name, sent_at) VALUES (?,?,?)",
                (uid, item["name"], datetime.now(timezone.utc).isoformat())
            )
        conn.commit()

async def check_future_event_watches(bot: discord.Client, event_name: str):
    conn = get_db()
    watchers = conn.execute("SELECT user_id, pattern FROM future_watches").fetchall()
    event_lower = event_name.lower()
    for w in watchers:
        pattern = w["pattern"].replace("*", ".*").lower()
        if re.search(pattern, event_lower):
            try:
                user = await bot.fetch_user(int(w["user_id"]))
                await user.send(f"🎉 Future event you're watching has been detected: **{event_name}**")
            except Exception:
                pass

async def notify_owner(bot: discord.Client, message: str):
    if not BOT_OWNER_ID:
        return
    try:
        user = await bot.fetch_user(BOT_OWNER_ID)
        await user.send(message)
    except Exception:
        pass

# ─────────────────────────────────────────────
# Discord Bot
# ─────────────────────────────────────────────
intents = discord.Intents.default()
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)

def upsert_user(discord_id: str):
    conn = get_db()
    conn.execute("INSERT OR IGNORE INTO users (discord_id) VALUES (?)", (discord_id,))
    conn.commit()

def category_items(category: str) -> list[dict]:
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM stock_cache WHERE category=? ORDER BY item_name", (category,)
    ).fetchall()
    return [dict(r) for r in rows]

def all_items() -> list[dict]:
    conn = get_db()
    rows = conn.execute("SELECT * FROM stock_cache ORDER BY category, item_name").fetchall()
    return [dict(r) for r in rows]

def fuzzy_item_name(query: str) -> Optional[str]:
    """Find best matching item name in DB."""
    conn = get_db()
    rows = conn.execute("SELECT item_name FROM stock_cache").fetchall()
    q = query.lower()
    for row in rows:
        if row["item_name"].lower() == q:
            return row["item_name"]
    for row in rows:
        if q in row["item_name"].lower():
            return row["item_name"]
    return query  # Return original if no match, so user can watch future items

def stock_embed(items: list[dict], title: str, color=discord.Color.green()) -> discord.Embed:
    embed = discord.Embed(title=title, color=color, timestamp=datetime.now(timezone.utc))
    if not items:
        embed.description = "*No items found.*"
        return embed
    in_stock = [i for i in items if i.get("in_stock")]
    out_stock = [i for i in items if not i.get("in_stock")]
    if in_stock:
        lines = []
        for i in in_stock:
            price = f" — 💰 {i['price']}" if i.get("price") else ""
            lines.append(f"✅ **{i['item_name']}**{price}")
        embed.add_field(name="In Stock", value="\n".join(lines[:20]) or "None", inline=False)
    if out_stock:
        names = ", ".join(i["item_name"] for i in out_stock[:20])
        embed.add_field(name="Out of Stock", value=names or "None", inline=False)
    embed.set_footer(text="Grow a Garden Monitor")
    return embed

# ── /setup_channel ──
@tree.command(name="setup_channel", description="Set the channel for stock notifications")
@app_commands.describe(channel="The channel to send notifications to")
async def setup_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    upsert_user(str(interaction.user.id))
    conn = get_db()
    conn.execute("UPDATE users SET notification_channel=? WHERE discord_id=?",
                 (str(channel.id), str(interaction.user.id)))
    conn.commit()
    await interaction.response.send_message(f"✅ Notification channel set to {channel.mention}", ephemeral=True)

# ── /setup_role ──
@tree.command(name="setup_role", description="Set the role to ping for stock notifications")
@app_commands.describe(role="The role to ping")
async def setup_role(interaction: discord.Interaction, role: discord.Role):
    upsert_user(str(interaction.user.id))
    conn = get_db()
    conn.execute("UPDATE users SET notification_role=? WHERE discord_id=?",
                 (str(role.id), str(interaction.user.id)))
    conn.commit()
    await interaction.response.send_message(f"✅ Notification role set to {role.mention}", ephemeral=True)

# ── /watch group ──
watch_group = app_commands.Group(name="watch", description="Manage your stock watchlist")

@watch_group.command(name="add", description="Add an item to your watchlist")
@app_commands.describe(item_name="Item name (partial matching supported)")
async def watch_add(interaction: discord.Interaction, item_name: str):
    upsert_user(str(interaction.user.id))
    resolved = fuzzy_item_name(item_name)
    conn = get_db()
    row = conn.execute("SELECT category FROM stock_cache WHERE item_name=?", (resolved,)).fetchone()
    category = row["category"] if row else "unknown"
    try:
        conn.execute(
            "INSERT OR REPLACE INTO watchlist (user_id, item_name, category) VALUES (?,?,?)",
            (str(interaction.user.id), resolved, category)
        )
        conn.commit()
        await interaction.response.send_message(f"✅ Added **{resolved}** to your watchlist.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"❌ Error: {e}", ephemeral=True)

@watch_group.command(name="add_category", description="Watch an entire category")
@app_commands.describe(category="Category: seeds, gear, eggs, event, easter")
@app_commands.choices(category=[
    app_commands.Choice(name="Seeds", value="seeds"),
    app_commands.Choice(name="Gear", value="gear"),
    app_commands.Choice(name="Eggs", value="eggs"),
    app_commands.Choice(name="Event Items", value="event"),
    app_commands.Choice(name="Easter 2026", value="easter"),
])
async def watch_add_category(interaction: discord.Interaction, category: str):
    upsert_user(str(interaction.user.id))
    conn = get_db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO watchlist (user_id, item_name, category) VALUES (?,?,?)",
            (str(interaction.user.id), f"[CATEGORY:{category}]", category)
        )
        conn.commit()
        await interaction.response.send_message(f"✅ Now watching entire **{category}** category.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"❌ Error: {e}", ephemeral=True)

@watch_group.command(name="list", description="Show your watchlist")
async def watch_list(interaction: discord.Interaction):
    conn = get_db()
    rows = conn.execute(
        "SELECT item_name, category, priority, wildcard_pattern FROM watchlist WHERE user_id=? AND enabled=1",
        (str(interaction.user.id),)
    ).fetchall()
    if not rows:
        await interaction.response.send_message("Your watchlist is empty.", ephemeral=True)
        return
    embed = discord.Embed(title="📋 Your Watchlist", color=discord.Color.blurple())
    lines = []
    for r in rows:
        p = {"high": "🟡", "medium": "🔵", "low": "⚪"}.get(r["priority"], "🔵")
        wp = f" *(wildcard: `{r['wildcard_pattern']}`)*" if r["wildcard_pattern"] else ""
        lines.append(f"{p} **{r['item_name']}** [{r['category']}]{wp}")
    embed.description = "\n".join(lines[:30])
    await interaction.response.send_message(embed=embed, ephemeral=True)

@watch_group.command(name="remove", description="Remove an item from your watchlist")
@app_commands.describe(item_name="Item name to remove")
async def watch_remove(interaction: discord.Interaction, item_name: str):
    conn = get_db()
    conn.execute("DELETE FROM watchlist WHERE user_id=? AND lower(item_name)=?",
                 (str(interaction.user.id), item_name.lower()))
    conn.commit()
    await interaction.response.send_message(f"✅ Removed **{item_name}** from watchlist.", ephemeral=True)

@watch_group.command(name="wildcard", description="Add a wildcard pattern (e.g. *sprout* matches any item with 'sprout')")
@app_commands.describe(pattern="Wildcard pattern, use * as wildcard")
async def watch_wildcard(interaction: discord.Interaction, pattern: str):
    upsert_user(str(interaction.user.id))
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO watchlist (user_id, item_name, wildcard_pattern) VALUES (?,?,?)",
        (str(interaction.user.id), f"[WILDCARD:{pattern}]", pattern)
    )
    conn.commit()
    await interaction.response.send_message(f"✅ Added wildcard pattern: `{pattern}`", ephemeral=True)

tree.add_command(watch_group)

# ── /priority group ──
priority_group = app_commands.Group(name="priority", description="Set priority levels for watched items")

async def _set_priority(interaction: discord.Interaction, item_name: str, priority: str):
    upsert_user(str(interaction.user.id))
    resolved = fuzzy_item_name(item_name)
    conn = get_db()
    existing = conn.execute(
        "SELECT id FROM watchlist WHERE user_id=? AND item_name=?",
        (str(interaction.user.id), resolved)
    ).fetchone()
    if existing:
        conn.execute("UPDATE watchlist SET priority=? WHERE user_id=? AND item_name=?",
                     (priority, str(interaction.user.id), resolved))
    else:
        row = conn.execute("SELECT category FROM stock_cache WHERE item_name=?", (resolved,)).fetchone()
        category = row["category"] if row else "unknown"
        conn.execute(
            "INSERT INTO watchlist (user_id, item_name, category, priority) VALUES (?,?,?,?)",
            (str(interaction.user.id), resolved, category, priority)
        )
    conn.commit()
    labels = {"high": "🟡 High (instant ping + DM)", "medium": "🔵 Medium (channel ping)", "low": "⚪ Low (daily digest)"}
    await interaction.response.send_message(
        f"✅ **{resolved}** set to {labels[priority]}", ephemeral=True
    )

@priority_group.command(name="high", description="High priority: instant ping + DM")
@app_commands.describe(item_name="Item to set as high priority")
async def priority_high(interaction: discord.Interaction, item_name: str):
    await _set_priority(interaction, item_name, "high")

@priority_group.command(name="medium", description="Medium priority: channel ping only")
@app_commands.describe(item_name="Item to set as medium priority")
async def priority_medium(interaction: discord.Interaction, item_name: str):
    await _set_priority(interaction, item_name, "medium")

@priority_group.command(name="low", description="Low priority: daily digest only")
@app_commands.describe(item_name="Item to set as low priority")
async def priority_low(interaction: discord.Interaction, item_name: str):
    await _set_priority(interaction, item_name, "low")

tree.add_command(priority_group)

# ── /stock group ──
stock_group = app_commands.Group(name="stock", description="Check current stock")

@stock_group.command(name="check", description="Check if a specific item is in stock")
@app_commands.describe(item_name="Item name to check")
async def stock_check(interaction: discord.Interaction, item_name: str):
    resolved = fuzzy_item_name(item_name)
    conn = get_db()
    row = conn.execute("SELECT * FROM stock_cache WHERE item_name=?", (resolved,)).fetchone()
    if not row:
        await interaction.response.send_message(f"❓ **{resolved}** not found in database.", ephemeral=True)
        return
    r = dict(row)
    status = "✅ In Stock" if r["in_stock"] else "❌ Out of Stock"
    price = f"\n💰 Price: {r['price']}" if r.get("price") else ""
    last = f"\n🕐 Last seen: {r['last_seen']}" if r.get("last_seen") else ""
    await interaction.response.send_message(
        f"**{r['item_name']}** [{r['category'].upper()}]\n{status}{price}{last}", ephemeral=True
    )

@stock_group.command(name="all", description="Show all current stock")
async def stock_all(interaction: discord.Interaction):
    items = all_items()
    embed = stock_embed(items, "🌱 All Current Stock", discord.Color.green())
    await interaction.response.send_message(embed=embed, ephemeral=True)

tree.add_command(stock_group)

# ── /events group ──
events_group = app_commands.Group(name="events", description="View active events")

def event_embed(event_name: str) -> discord.Embed:
    """Build a rich embed for any event by name (fuzzy match supported)."""
    conn = get_db()
    # Exact match first, then fuzzy
    row = conn.execute("SELECT * FROM events WHERE event_name=?", (event_name,)).fetchone()
    if not row:
        row = conn.execute(
            "SELECT * FROM events WHERE lower(event_name) LIKE ? AND active=1 LIMIT 1",
            (f"%{event_name.lower()}%",)
        ).fetchone()
    if not row:
        embed = discord.Embed(
            title="❓ Event Not Found",
            description=f"No active event matching **{event_name}**.\nUse `/events list` to see all active events.",
            color=discord.Color.red()
        )
        return embed

    data = json.loads(row["data_json"])
    # Pick an emoji based on event_type
    type_emoji = {"easter": "🐰", "fall": "🍂", "summer": "☀️", "winter": "❄️",
                  "halloween": "🎃", "auto_detected": "🔍"}.get(row["event_type"], "🎪")

    embed = discord.Embed(
        title=f"{type_emoji} {row['event_name']}",
        description=data.get("description", ""),
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Type", value=row["event_type"].replace("_", " ").title(), inline=True)
    embed.add_field(name="Status", value="✅ Active" if row["active"] else "❌ Inactive", inline=True)

    if "items" in data:
        lines = []
        for item in data["items"]:
            line = f"• **{item['name']}**"
            if "price" in item:
                line += f" — {item['price']}"
            if "drop_chance" in item:
                line += f" ({item['drop_chance']})"
            if "note" in item:
                line += f" *{item['note']}*"
            if "ability" in item:
                line += f"\n  ✨ *{item['ability']}*"
            lines.append(line)
        embed.add_field(name="Items / Pets", value="\n".join(lines[:15]) or "None", inline=False)

    if "rewards" in data:
        lines = [f"**{tier}:** {reward}" for tier, reward in data["rewards"].items()]
        embed.add_field(name="🎁 Rewards", value="\n".join(lines), inline=False)

    if "tiers" in data:
        t = data["tiers"]
        tier_str = " | ".join(f"{k}: {v}" for k, v in t.items())
        embed.add_field(name="📊 Tiers", value=tier_str, inline=False)

    if "npc" in data:
        embed.add_field(name="🧑 NPC", value=data["npc"], inline=True)

    if "reset_hours" in data:
        embed.add_field(name="🔄 Reset", value=f"Every {data['reset_hours']} hours", inline=True)

    if "mechanic" in data:
        embed.add_field(name="⚙️ Mechanic", value=data["mechanic"], inline=False)

    if "note" in data:
        embed.add_field(name="📝 Note", value=data["note"], inline=False)

    if "end_date" in data:
        embed.add_field(name="⏰ Ends", value=data["end_date"], inline=True)

    detected = row["detected_at"][:10] if row["detected_at"] else "Unknown"
    embed.set_footer(text=f"Grow a Garden Monitor • Detected {detected}")
    return embed

def fuzzy_event_name(query: str) -> Optional[str]:
    """Return the best matching active event name from the DB."""
    conn = get_db()
    rows = conn.execute("SELECT event_name FROM events WHERE active=1").fetchall()
    q = query.lower()
    for r in rows:
        if r["event_name"].lower() == q:
            return r["event_name"]
    for r in rows:
        if q in r["event_name"].lower():
            return r["event_name"]
    return None

@events_group.command(name="list", description="Show all currently active events")
async def events_list(interaction: discord.Interaction):
    conn = get_db()
    rows = conn.execute(
        "SELECT event_name, event_type, detected_at FROM events WHERE active=1 ORDER BY event_type, event_name"
    ).fetchall()
    if not rows:
        await interaction.response.send_message("📭 No active events found.", ephemeral=True)
        return
    embed = discord.Embed(
        title="🎪 Active Events",
        description="Use `/events info <name>` for full details on any event.",
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    # Group by event_type
    by_type: dict[str, list] = {}
    for r in rows:
        by_type.setdefault(r["event_type"], []).append(r["event_name"])
    for etype, names in by_type.items():
        type_emoji = {"easter": "🐰", "fall": "🍂", "summer": "☀️", "winter": "❄️",
                      "halloween": "🎃", "auto_detected": "🔍"}.get(etype, "🎪")
        label = f"{type_emoji} {etype.replace('_', ' ').title()}"
        embed.add_field(name=label, value="\n".join(f"• {n}" for n in names), inline=False)
    embed.set_footer(text="Grow a Garden Monitor")
    await interaction.response.send_message(embed=embed, ephemeral=True)

@events_group.command(name="info", description="Show full details for any active event")
@app_commands.describe(event_name="Event name (partial match works, e.g. 'egg war', 'candy')")
async def events_info(interaction: discord.Interaction, event_name: str):
    resolved = fuzzy_event_name(event_name) or event_name
    await interaction.response.send_message(embed=event_embed(resolved), ephemeral=True)

async def event_name_autocomplete(interaction: discord.Interaction, current: str):
    """Autocomplete active event names for slash commands."""
    conn = get_db()
    rows = conn.execute(
        "SELECT event_name FROM events WHERE active=1 AND lower(event_name) LIKE ? LIMIT 25",
        (f"%{current.lower()}%",)
    ).fetchall()
    return [app_commands.Choice(name=r["event_name"], value=r["event_name"]) for r in rows]

@events_group.command(name="toggle", description="Mark an event as active or inactive (admin only)")
@app_commands.describe(event_name="Event name", active="True to activate, False to deactivate")
@app_commands.autocomplete(event_name=event_name_autocomplete)
async def events_toggle(interaction: discord.Interaction, event_name: str, active: bool):
    if interaction.user.id != BOT_OWNER_ID:
        await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        return
    conn = get_db()
    resolved = fuzzy_event_name(event_name) or event_name
    conn.execute("UPDATE events SET active=? WHERE event_name=?", (int(active), resolved))
    conn.commit()
    state = "✅ activated" if active else "❌ deactivated"
    await interaction.response.send_message(f"Event **{resolved}** {state}.", ephemeral=True)

tree.add_command(events_group)

# ── /future_event group ──
future_event_group = app_commands.Group(name="future_event", description="Monitor for future/unannounced events")

@future_event_group.command(name="watch", description="Get notified when an event matching this name is detected")
@app_commands.describe(event_name="Event name or pattern (e.g. 'summer festival', '*fall*')")
async def future_event_watch(interaction: discord.Interaction, event_name: str):
    upsert_user(str(interaction.user.id))
    conn = get_db()
    conn.execute(
        "INSERT INTO future_watches (user_id, pattern, created_at) VALUES (?,?,?)",
        (str(interaction.user.id), event_name, datetime.now(timezone.utc).isoformat())
    )
    conn.commit()
    await interaction.response.send_message(f"✅ You'll be notified when event matching **{event_name}** is detected.", ephemeral=True)

@future_event_group.command(name="list", description="Show events you're watching for")
async def future_event_list(interaction: discord.Interaction):
    conn = get_db()
    rows = conn.execute("SELECT pattern, created_at FROM future_watches WHERE user_id=?",
                        (str(interaction.user.id),)).fetchall()
    if not rows:
        await interaction.response.send_message("You're not watching for any future events.", ephemeral=True)
        return
    lines = [f"• `{r['pattern']}` (added {r['created_at'][:10]})" for r in rows]
    await interaction.response.send_message("🔮 **Future Event Watches:**\n" + "\n".join(lines), ephemeral=True)

tree.add_command(future_event_group)

# ── /update_db ──
@tree.command(name="update_db", description="Force refresh event data (admin only)")
async def update_db(interaction: discord.Interaction):
    if interaction.user.id != BOT_OWNER_ID:
        await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        return
    await interaction.response.send_message("♻️ Forcing event data refresh...", ephemeral=True)
    init_db()
    await interaction.followup.send("✅ Database refreshed.", ephemeral=True)

# ── Bot events ──
@bot.event
async def on_ready():
    log.info(f"Discord bot logged in as {bot.user} ({bot.user.id})")
    try:
        synced = await tree.sync()
        log.info(f"Synced {len(synced)} slash commands globally.")
    except Exception as e:
        log_error("ERROR", f"Failed to sync commands: {e}")
    # Start game WebSocket listener
    asyncio.create_task(game_ws_listener(bot))

# ─────────────────────────────────────────────
# HTML Website (embedded)
# ─────────────────────────────────────────────
WEBSITE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>🌱 Grow a Garden — Stock Monitor</title>
<style>
  :root {
    --bg: #1a1a2e; --surface: #16213e; --surface2: #0f3460;
    --accent: #e94560; --accent2: #f5a623; --text: #e0e0e0;
    --text-muted: #888; --green: #4caf50; --red: #f44336;
    --gold: #f5a623; --blue: #2196f3; --border: #2a2a4a;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', sans-serif; min-height: 100vh; }
  header { background: var(--surface); padding: 16px 24px; display: flex; align-items: center; justify-content: space-between; border-bottom: 2px solid var(--accent); }
  header h1 { font-size: 1.4rem; color: var(--accent2); }
  #last-updated { color: var(--text-muted); font-size: 0.85rem; }
  .tabs { display: flex; gap: 4px; padding: 16px 24px 0; background: var(--surface); border-bottom: 1px solid var(--border); flex-wrap: wrap; }
  .tab { padding: 8px 18px; border-radius: 8px 8px 0 0; cursor: pointer; background: var(--bg); color: var(--text-muted); border: 1px solid var(--border); border-bottom: none; font-size: 0.9rem; transition: all 0.2s; }
  .tab.active { background: var(--surface2); color: var(--text); border-color: var(--accent); }
  .tab:hover:not(.active) { color: var(--text); background: #1e1e3a; }
  main { padding: 24px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 14px; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 14px; transition: transform 0.15s, border-color 0.15s; }
  .card:hover { transform: translateY(-2px); border-color: var(--accent2); }
  .card.in-stock { border-left: 4px solid var(--green); }
  .card.out-stock { border-left: 4px solid var(--red); opacity: 0.6; }
  .card.high-priority { border-left: 4px solid var(--gold); background: linear-gradient(135deg, var(--surface), #1e1a00); }
  .item-name { font-weight: 600; font-size: 1rem; margin-bottom: 6px; }
  .item-meta { font-size: 0.8rem; color: var(--text-muted); display: flex; flex-wrap: wrap; gap: 6px; }
  .badge { padding: 2px 8px; border-radius: 12px; font-size: 0.75rem; font-weight: 600; }
  .badge.in { background: #1b4a1b; color: var(--green); }
  .badge.out { background: #3a1414; color: var(--red); }
  .badge.cat { background: var(--surface2); color: var(--blue); }
  .price { color: var(--gold); font-weight: 600; }
  .status-bar { position: fixed; bottom: 16px; right: 16px; background: var(--surface2); border: 1px solid var(--accent); border-radius: 8px; padding: 8px 14px; font-size: 0.8rem; color: var(--text-muted); }
  .status-bar.connected { border-color: var(--green); color: var(--green); }
  #empty { text-align: center; padding: 60px; color: var(--text-muted); }
  #empty h2 { font-size: 2rem; margin-bottom: 8px; }
  .section-header { margin-bottom: 16px; font-size: 1.1rem; color: var(--accent2); }
  .loading { text-align: center; padding: 40px; color: var(--text-muted); font-size: 1.1rem; animation: pulse 1.5s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
  @media (max-width: 600px) { main { padding: 12px; } header { flex-direction: column; gap: 8px; } }
</style>
</head>
<body>
<header>
  <h1>🌱 Grow a Garden — Stock Monitor</h1>
  <span id="last-updated">Loading...</span>
</header>
<div class="tabs">
  <div class="tab active" data-tab="all">🌍 All Stock</div>
  <div class="tab" data-tab="seeds">🌱 Seeds</div>
  <div class="tab" data-tab="gear">⚙️ Gear</div>
  <div class="tab" data-tab="eggs">🥚 Eggs</div>
  <div class="tab" data-tab="event">🎪 Events</div>
  <div class="tab" data-tab="easter">🐰 Easter 2026</div>
</div>
<main>
  <div id="content" class="grid"><div class="loading">🌱 Loading stock data...</div></div>
</main>
<div class="status-bar" id="ws-status">⚡ Connecting...</div>

<script>
const HIGH_PRIORITY = ['easter sprout','lyrebird','peryton','stork','orchid mantis','gilded chocolate'];
let currentTab = 'all';
let stockData = {};

function renderCards(items) {
  const container = document.getElementById('content');
  if (!items || items.length === 0) {
    container.innerHTML = '<div id="empty"><h2>📭</h2><p>No items to display.</p></div>';
    return;
  }
  container.className = 'grid';
  container.innerHTML = items.map(item => {
    const inStock = item.in_stock;
    const isHigh = HIGH_PRIORITY.some(hp => item.item_name.toLowerCase().includes(hp));
    const classes = ['card', inStock ? 'in-stock' : 'out-stock', isHigh ? 'high-priority' : ''].join(' ');
    const price = item.price ? `<span class="price">💰 ${item.price}</span>` : '';
    const qty = item.quantity > 0 ? `<span>Qty: ${item.quantity}</span>` : '';
    const lastSeen = item.last_seen ? `<span>Last seen: ${new Date(item.last_seen).toLocaleString()}</span>` : '';
    return `<div class="${classes}">
      <div class="item-name">${isHigh ? '⭐ ' : ''}${item.item_name}</div>
      <div class="item-meta">
        <span class="badge ${inStock ? 'in' : 'out'}">${inStock ? '✅ In Stock' : '❌ Out'}</span>
        <span class="badge cat">${item.category}</span>
        ${price}${qty}${lastSeen}
      </div>
    </div>`;
  }).join('');
}

async function fetchStock(tab = 'all') {
  try {
    const url = tab === 'all' ? '/api/stock' : `/api/stock?category=${tab}`;
    const res = await fetch(url);
    const data = await res.json();
    stockData[tab] = data.items || [];
    document.getElementById('last-updated').textContent = 'Last updated: ' + new Date().toLocaleTimeString();
    if (tab === currentTab) renderCards(stockData[tab]);
  } catch(e) {
    console.error('Fetch error', e);
  }
}

document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    tab.classList.add('active');
    currentTab = tab.dataset.tab;
    if (stockData[currentTab]) {
      renderCards(stockData[currentTab]);
    } else {
      document.getElementById('content').innerHTML = '<div class="loading">Loading...</div>';
      fetchStock(currentTab);
    }
  });
});

// WebSocket for live updates
function connectWS() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  const statusEl = document.getElementById('ws-status');
  ws.onopen = () => { statusEl.textContent = '⚡ Live'; statusEl.classList.add('connected'); };
  ws.onclose = () => {
    statusEl.textContent = '⚡ Reconnecting...';
    statusEl.classList.remove('connected');
    setTimeout(connectWS, 3000);
  };
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'stock_refresh') {
      fetchStock(currentTab);
    }
  };
}

// Initial load + polling
fetchStock('all');
setInterval(() => fetchStock(currentTab), 30000);
connectWS();
</script>
</body>
</html>
"""

# ─────────────────────────────────────────────
# FastAPI App
# ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    log.info("Database initialized.")
    if DISCORD_TOKEN:
        asyncio.create_task(bot.start(DISCORD_TOKEN))
        log.info("Discord bot task started.")
    else:
        log.warning("No DISCORD_TOKEN set. Discord bot will not start.")
    yield
    log.info("Shutting down.")

app = FastAPI(title="Grow a Garden Monitor", lifespan=lifespan)

@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(content=WEBSITE_HTML)

@app.get("/api/stock")
async def api_stock(category: Optional[str] = None):
    conn = get_db()
    if category:
        rows = conn.execute(
            "SELECT * FROM stock_cache WHERE category=? ORDER BY in_stock DESC, item_name",
            (category,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM stock_cache ORDER BY in_stock DESC, category, item_name"
        ).fetchall()
    items = [dict(r) for r in rows]
    return JSONResponse({"items": items, "count": len(items), "timestamp": datetime.now(timezone.utc).isoformat()})

@app.get("/api/events")
async def api_events():
    conn = get_db()
    rows = conn.execute("SELECT * FROM events WHERE active=1 ORDER BY event_type, event_name").fetchall()
    events = []
    for r in rows:
        d = dict(r)
        try:
            d["data"] = json.loads(d.pop("data_json"))
        except Exception:
            d["data"] = {}
        events.append(d)
    return JSONResponse({"events": events, "count": len(events)})

@app.get("/api/watchlist/{user_id}")
async def api_watchlist(user_id: str):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM watchlist WHERE user_id=? AND enabled=1", (user_id,)
    ).fetchall()
    return JSONResponse({"watchlist": [dict(r) for r in rows]})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_ws_clients.append(websocket)
    try:
        while True:
            await asyncio.sleep(30)
            await websocket.send_json({"type": "ping", "timestamp": datetime.now(timezone.utc).isoformat()})
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if websocket in connected_ws_clients:
            connected_ws_clients.remove(websocket)

@app.get("/health")
async def health():
    conn = get_db()
    stock_count = conn.execute("SELECT COUNT(*) FROM stock_cache").fetchone()[0]
    event_count = conn.execute("SELECT COUNT(*) FROM events WHERE active=1").fetchone()[0]
    return {"status": "ok", "stock_items": stock_count, "active_events": event_count,
            "ws_clients": len(connected_ws_clients), "timestamp": datetime.now(timezone.utc).isoformat()}

# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    log.info(f"Starting Grow a Garden Monitor on port {port}")
    uvicorn.run("app:app", host="0.0.0.0", port=port, log_level="info")
