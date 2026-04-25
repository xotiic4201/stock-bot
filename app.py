import asyncio
import json
import logging
import os
import sqlite3
import time
import re
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, List, Optional, Set
from collections import defaultdict

import discord
from discord import app_commands
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import aiohttp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============ Configuration ============
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")
PUBLIC_URL = os.environ.get("PUBLIC_URL")
DB_PATH = os.environ.get("DB_PATH", "/tmp/grow_a_garden.db")

# ============ Database ============
class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_tables()

    def _connect(self):
        return sqlite3.connect(self.db_path, timeout=30)

    def _init_tables(self):
        with self._connect() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    discord_id TEXT PRIMARY KEY,
                    username TEXT,
                    notification_channel TEXT,
                    notification_role TEXT,
                    shekels INTEGER DEFAULT 100,
                    is_bot_owner INTEGER DEFAULT 0,
                    created_at TEXT
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS watchlist (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT,
                    item_name TEXT,
                    priority TEXT DEFAULT 'medium',
                    wildcard_pattern TEXT,
                    enabled INTEGER DEFAULT 1,
                    created_at TEXT,
                    UNIQUE(user_id, item_name)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_cache (
                    item_name TEXT PRIMARY KEY,
                    category TEXT,
                    price REAL,
                    in_stock INTEGER,
                    last_seen TEXT,
                    rarity TEXT DEFAULT 'common'
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    event_name TEXT PRIMARY KEY,
                    event_type TEXT,
                    active INTEGER,
                    phase TEXT,
                    description TEXT,
                    detected_at TEXT
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS marketplace_listings (
                    id TEXT PRIMARY KEY,
                    seller_id TEXT,
                    seller_name TEXT,
                    item_type TEXT,
                    item_name TEXT,
                    image_data TEXT,
                    size_kg REAL,
                    age_days INTEGER,
                    price_shekels INTEGER,
                    price_robux INTEGER,
                    price_paypal REAL,
                    description TEXT,
                    created_at TEXT,
                    status TEXT DEFAULT 'active'
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS future_watches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT,
                    pattern TEXT,
                    created_at TEXT,
                    UNIQUE(user_id, pattern)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS notification_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT,
                    item_name TEXT,
                    sent_at TEXT
                )
            """)
            
            conn.commit()
            logger.info("Database tables created/verified")

    # User methods
    def get_or_create_user(self, discord_id: str, username: str = None):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE discord_id = ?", (discord_id,))
            user = cursor.fetchone()
            if not user:
                cursor.execute(
                    "INSERT INTO users (discord_id, username, shekels, created_at) VALUES (?, ?, ?, ?)",
                    (discord_id, username, 100, datetime.now().isoformat())
                )
                conn.commit()
                return {"discord_id": discord_id, "username": username, "shekels": 100, "is_bot_owner": 0}
            return {"discord_id": user[0], "username": user[1], "shekels": user[4], "is_bot_owner": user[6]}

    def set_bot_owner(self, discord_id: str):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET is_bot_owner = 1 WHERE discord_id = ?", (discord_id,))
            conn.commit()

    def add_shekels(self, discord_id: str, amount: int):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET shekels = shekels + ? WHERE discord_id = ?", (amount, discord_id))
            conn.commit()

    # Marketplace methods
    def create_listing(self, seller_id: str, seller_name: str, item_type: str, item_name: str,
                       image_data: str, size_kg: float, age_days: int, price_shekels: int = None,
                       price_robux: int = None, price_paypal: float = None, description: str = None) -> str:
        listing_id = str(uuid.uuid4())[:8]
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO marketplace_listings 
                (id, seller_id, seller_name, item_type, item_name, image_data, size_kg, age_days,
                 price_shekels, price_robux, price_paypal, description, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (listing_id, seller_id, seller_name, item_type, item_name, image_data,
                  size_kg, age_days, price_shekels, price_robux, price_paypal, description,
                  datetime.now().isoformat()))
            conn.commit()
        return listing_id

    def get_listings(self, item_type: str = None) -> List[Dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            if item_type:
                cursor.execute(
                    "SELECT * FROM marketplace_listings WHERE status = 'active' AND item_type = ? ORDER BY created_at DESC",
                    (item_type,)
                )
            else:
                cursor.execute("SELECT * FROM marketplace_listings WHERE status = 'active' ORDER BY created_at DESC")
            rows = cursor.fetchall()
            return [{
                "id": r[0], "seller_id": r[1], "seller_name": r[2], "item_type": r[3],
                "item_name": r[4], "image_data": r[5], "size_kg": r[6], "age_days": r[7],
                "price_shekels": r[8], "price_robux": r[9], "price_paypal": r[10],
                "description": r[11], "created_at": r[12]
            } for r in rows]

    def delete_listing(self, listing_id: str, seller_id: str) -> bool:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE marketplace_listings SET status = 'deleted' WHERE id = ? AND seller_id = ?",
                (listing_id, seller_id)
            )
            conn.commit()
            return cursor.rowcount > 0

    # Stock methods
    def update_stock(self, item_name: str, category: str, price: float, in_stock: bool, rarity: str = "common"):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO stock_cache (item_name, category, price, in_stock, last_seen, rarity)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(item_name) DO UPDATE SET
                    category = excluded.category,
                    price = excluded.price,
                    in_stock = excluded.in_stock,
                    last_seen = excluded.last_seen,
                    rarity = excluded.rarity
            """, (item_name, category, price, 1 if in_stock else 0, datetime.now().isoformat(), rarity))
            conn.commit()

    def get_all_stock(self) -> List[Dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT item_name, category, price, in_stock, last_seen, rarity FROM stock_cache WHERE in_stock = 1 ORDER BY price DESC")
            rows = cursor.fetchall()
            return [{"name": r[0], "category": r[1], "price": r[2], "in_stock": bool(r[3]), "last_seen": r[4], "rarity": r[5]} for r in rows]

    def get_active_events(self) -> List[Dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT event_name, event_type, phase, description FROM events WHERE active = 1")
            rows = cursor.fetchall()
            return [{"name": r[0], "type": r[1], "phase": r[2], "description": r[3]} for r in rows]

    def add_watch(self, user_id: str, item_name: str, priority: str = "medium", wildcard: str = None):
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO watchlist (user_id, item_name, priority, wildcard_pattern, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (user_id, item_name.lower(), priority, wildcard, datetime.now().isoformat()))
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def remove_watch(self, user_id: str, item_name: str):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM watchlist WHERE user_id = ? AND item_name = ?", (user_id, item_name.lower()))
            conn.commit()
            return cursor.rowcount > 0

    def get_user_watchlist(self, user_id: str) -> List[Dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT item_name, priority, wildcard_pattern FROM watchlist WHERE user_id = ? AND enabled = 1", (user_id,))
            rows = cursor.fetchall()
            return [{"item_name": r[0], "priority": r[1], "wildcard": r[2]} for r in rows]

    def get_users_watching_item(self, item_name: str) -> List[Dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT w.user_id, w.priority, u.notification_channel, u.notification_role
                FROM watchlist w
                LEFT JOIN users u ON w.user_id = u.discord_id
                WHERE w.item_name = ? AND w.enabled = 1
            """, (item_name.lower(),))
            exact = cursor.fetchall()
            
            cursor.execute("SELECT user_id, wildcard_pattern, priority FROM watchlist WHERE wildcard_pattern IS NOT NULL AND enabled = 1")
            wildcards = cursor.fetchall()
            
            users = {}
            for row in exact:
                users[row[0]] = {"priority": row[1], "channel": row[2], "role": row[3]}
            
            for row in wildcards:
                pattern = row[1].replace("*", ".*")
                if re.search(pattern, item_name, re.IGNORECASE):
                    if row[0] not in users:
                        cursor.execute("SELECT notification_channel, notification_role FROM users WHERE discord_id = ?", (row[0],))
                        urow = cursor.fetchone()
                        if urow:
                            users[row[0]] = {"priority": row[2], "channel": urow[0], "role": urow[1]}
            
            return [{"user_id": u, "priority": d["priority"], "channel": d["channel"], "role": d["role"]} for u, d in users.items()]

    def add_future_watch(self, user_id: str, pattern: str):
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("INSERT INTO future_watches (user_id, pattern, created_at) VALUES (?, ?, ?)",
                               (user_id, pattern.lower(), datetime.now().isoformat()))
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def get_future_watches(self) -> List[Dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id, pattern FROM future_watches")
            return [{"user_id": r[0], "pattern": r[1]} for r in cursor.fetchall()]

    def log_notification(self, user_id: str, item_name: str):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO notification_log (user_id, item_name, sent_at) VALUES (?, ?, ?)",
                           (user_id, item_name, datetime.now().isoformat()))
            conn.commit()

db = Database(DB_PATH)

# ============ Discord Bot ============
intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)
bot_owner_id = None
bot_ready = False

@bot.event
async def on_ready():
    global bot_owner_id, bot_ready
    logger.info(f" Discord Bot ONLINE! Logged in as {bot.user}")
    
    await tree.sync()
    logger.info(" Slash commands synced globally")
    
    try:
        app_info = await bot.application_info()
        bot_owner_id = app_info.owner.id
        db.set_bot_owner(str(bot_owner_id))
        logger.info(f"Bot owner ID: {bot_owner_id}")
    except Exception as e:
        logger.error(f"Failed to get owner info: {e}")
    
    bot_ready = True
    await bot.change_presence(activity=discord.Game(name="[+] Xotiis Market | /help"))

# ============ Discord Slash Commands ============
@tree.command(name="help", description="Show all commands")
async def help_command(interaction: discord.Interaction):
    embed = discord.Embed(title="[+] XOTIIS BOT COMMANDS", color=0x4ade80)
    embed.add_field(name="ECONOMY", value="`/shekels` - Check balance", inline=False)
    embed.add_field(name="NOTIFICATIONS", value="`/watch <item>` - Watch item\n`/unwatch <item>` - Remove\n`/watches` - List watches", inline=False)
    embed.add_field(name="STOCK", value="`/stock` - Current stock\n`/stock <item>` - Search item", inline=False)
    embed.add_field(name="EVENTS", value="`/events` - Active events\n`/future <pattern>` - Watch events", inline=False)
    embed.add_field(name="MARKETPLACE", value="`/marketplace` - Browse listings", inline=False)
    
    # Only bot owner sees the sell command in help
    if interaction.user.id == bot_owner_id:
        embed.add_field(name="OWNER ONLY", value="`/sell` - List item on marketplace", inline=False)
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tree.command(name="shekels", description="Check your balance")
async def shekels_command(interaction: discord.Interaction):
    user = db.get_or_create_user(str(interaction.user.id), interaction.user.name)
    embed = discord.Embed(title="[+] YOUR BALANCE", color=0x4ade80)
    embed.add_field(name="Shekels", value=f"**{user['shekels']}** :coin:", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tree.command(name="watch", description="Watch for an item in stock")
async def watch_command(interaction: discord.Interaction, item_name: str, priority: str = "medium"):
    db.get_or_create_user(str(interaction.user.id), interaction.user.name)
    success = db.add_watch(str(interaction.user.id), item_name, priority)
    if success:
        await interaction.response.send_message(f"[+] Watching `{item_name}` (Priority: {priority.upper()})", ephemeral=True)
    else:
        await interaction.response.send_message(f"[-] Already watching `{item_name}`", ephemeral=True)

@tree.command(name="unwatch", description="Remove from watchlist")
async def unwatch_command(interaction: discord.Interaction, item_name: str):
    success = db.remove_watch(str(interaction.user.id), item_name)
    if success:
        await interaction.response.send_message(f"[+] Removed `{item_name}` from watchlist", ephemeral=True)
    else:
        await interaction.response.send_message(f"[-] `{item_name}` not in watchlist", ephemeral=True)

@tree.command(name="watches", description="List your watchlist")
async def watches_command(interaction: discord.Interaction):
    items = db.get_user_watchlist(str(interaction.user.id))
    if not items:
        await interaction.response.send_message("[-] Your watchlist is empty. Use `/watch` to add items.", ephemeral=True)
        return
    msg = "**YOUR WATCHLIST:**\n" + "\n".join([f"• {i['item_name']} [{i['priority'].upper()}]" for i in items])
    await interaction.response.send_message(msg, ephemeral=True)

@tree.command(name="stock", description="Check current stock")
async def stock_command(interaction: discord.Interaction, item_name: str = None):
    items = db.get_all_stock()
    if item_name:
        items = [i for i in items if item_name.lower() in i["name"].lower()]
    if not items:
        await interaction.response.send_message("[-] No items currently in stock.", ephemeral=True)
        return
    msg = "**CURRENT STOCK:**\n" + "\n".join([f"• {i['name']} :coin: {i['price']:,.0f}" for i in items[:15]])
    await interaction.response.send_message(msg, ephemeral=True)

@tree.command(name="marketplace", description="Browse Xotiis Marketplace")
async def marketplace_command(interaction: discord.Interaction, item_type: str = None):
    listings = db.get_listings(item_type)
    if not listings:
        await interaction.response.send_message("[-] No active listings.", ephemeral=True)
        return
    msg = "**XOTIIS MARKETPLACE:**\n"
    for listing in listings[:10]:
        msg += f"• {listing['item_name']} ({listing['item_type']}) - :coin: {listing['price_shekels']}\n"
    await interaction.response.send_message(msg, ephemeral=True)

@tree.command(name="events", description="Show active events")
async def events_command(interaction: discord.Interaction):
    events = db.get_active_events()
    if not events:
        await interaction.response.send_message("[-] No active events.", ephemeral=True)
        return
    msg = "**ACTIVE EVENTS:**\n" + "\n".join([f"• {e['name']} - {e.get('phase', 'Active')}" for e in events])
    await interaction.response.send_message(msg, ephemeral=True)

@tree.command(name="future", description="Watch for future events")
async def future_command(interaction: discord.Interaction, pattern: str):
    db.get_or_create_user(str(interaction.user.id), interaction.user.name)
    success = db.add_future_watch(str(interaction.user.id), pattern)
    if success:
        await interaction.response.send_message(f"[+] Watching for events matching `{pattern}`", ephemeral=True)
    else:
        await interaction.response.send_message(f"[-] Already watching `{pattern}`", ephemeral=True)

# ============ OWNER-ONLY SELL COMMAND ============
@tree.command(name="sell", description="[OWNER] List an item on marketplace")
async def sell_command(
    interaction: discord.Interaction,
    item_type: str,
    item_name: str,
    price_shekels: int,
    size_kg: float = None,
    age_days: int = None,
    description: str = None,
    image_url: str = None
):
    # OWNER ONLY CHECK
    if interaction.user.id != bot_owner_id:
        await interaction.response.send_message("[-] This command is restricted to the bot owner only.", ephemeral=True)
        return
    
    await interaction.response.defer(ephemeral=True)
    
    user = db.get_or_create_user(str(interaction.user.id), interaction.user.name)
    
    # Use placeholder image data
    image_data = image_url or "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
    
    listing_id = db.create_listing(
        str(interaction.user.id), interaction.user.name,
        item_type, item_name, image_data, size_kg or 0, age_days or 0,
        price_shekels, None, None, description
    )
    
    embed = discord.Embed(title="[+] ITEM LISTED", color=0x4ade80)
    embed.add_field(name="Item", value=f"{item_name} ({item_type})", inline=True)
    embed.add_field(name="Price", value=f":coin: {price_shekels}", inline=True)
    embed.add_field(name="Listing ID", value=listing_id, inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)
    
    # Also notify in a public channel if configured
    for guild in bot.guilds:
        for channel in guild.text_channels:
            if "marketplace" in channel.name.lower() or "shop" in channel.name.lower():
                await channel.send(f"**NEW LISTING!** {item_name} ({item_type}) for :coin: {price_shekels} shekels! Use `/marketplace` to view.")
                break

# ============ Game WebSocket Listener ============
websocket_connections: Set[WebSocket] = set()

async def send_discord_notification(user_id: str, item_name: str, price: float, priority: str, channel_id: str, role_id: str = None):
    try:
        user = await bot.fetch_user(int(user_id))
        if not user:
            return
        
        message = f"[+] **{item_name}** is in stock for :coin: {price:,.0f}!"
        
        if priority == "high":
            try:
                await user.send(f"[HIGH PRIORITY] {message}")
            except:
                pass
        
        if channel_id:
            channel = bot.get_channel(int(channel_id))
            if channel:
                ping = f"<@{user_id}>" + (f" <@&{role_id}>" if role_id else "")
                await channel.send(f"{ping}\n{message}")
        
        db.log_notification(user_id, item_name)
    except Exception as e:
        logger.error(f"Failed to send notification: {e}")

async def game_websocket_listener():
    ws_urls = [
        "wss://api.jstudio.ai/grow-a-garden/ws",
        "wss://ws.growagarden.com/socket.io/?EIO=4&transport=websocket"
    ]
    
    while True:
        for ws_url in ws_urls:
            try:
                async with aiohttp.ClientSession() as session:
                    logger.info(f"Connecting to: {ws_url}")
                    async with session.ws_connect(ws_url, timeout=30) as ws:
                        logger.info(f"Connected to {ws_url}")
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(msg.data)
                                    if data.get("type") == "stock_refresh":
                                        for item in data.get("items", []):
                                            db.update_stock(
                                                item.get("name", "Unknown"),
                                                item.get("category", "unknown"),
                                                item.get("price", 0),
                                                item.get("in_stock", False),
                                                item.get("rarity", "common")
                                            )
                                            if item.get("in_stock"):
                                                watchers = db.get_users_watching_item(item["name"])
                                                for w in watchers:
                                                    await send_discord_notification(
                                                        w["user_id"], item["name"], item["price"],
                                                        w["priority"], w["channel"], w["role"]
                                                    )
                                except json.JSONDecodeError:
                                    pass
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
            await asyncio.sleep(5)

# ============ FastAPI Web Server ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(bot.start(DISCORD_TOKEN))
    asyncio.create_task(game_websocket_listener())
    yield
    await bot.close()

app = FastAPI(title="Xotiis Garden", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# API Endpoints
@app.get("/api/stock")
async def get_stock():
    return db.get_all_stock()

@app.get("/api/events")
async def get_events():
    return db.get_active_events()

@app.get("/api/marketplace/listings")
async def get_listings(item_type: str = None):
    return db.get_listings(item_type)

@app.get("/api/stats")
async def get_stats():
    stock = db.get_all_stock()
    events = db.get_active_events()
    listings = db.get_listings()
    return {
        "items_in_stock": len(stock),
        "active_events": len(events),
        "total_listings": len(listings),
        "status": "online"
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    websocket_connections.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        websocket_connections.discard(websocket)

# ============ Pixel Art HTML Website ============
HTML_PAGE = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>XOTIIS GARDEN</title>
<link href="https://fonts.googleapis.com/css2?family=VT323&display=swap" rel="stylesheet">
<style>
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

body {
  background: #0a0c0f;
  font-family: 'VT323', monospace;
  color: #8bc34a;
  min-height: 100vh;
  image-rendering: pixelated;
  image-rendering: crisp-edges;
  image-rendering: pixelated;
}

/* Pixel background */
.pixel-bg {
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  z-index: -1;
  background-image: 
    linear-gradient(#1a1e24 1px, transparent 1px),
    linear-gradient(90deg, #1a1e24 1px, transparent 1px);
  background-size: 32px 32px;
}

/* Pixel header */
.pixel-header {
  border-bottom: 4px solid #4ade80;
  background: #0f1117;
  padding: 12px 20px;
  position: sticky;
  top: 0;
  z-index: 100;
}

.header-inner {
  max-width: 1300px;
  margin: 0 auto;
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 16px;
}

.logo {
  display: flex;
  align-items: center;
  gap: 12px;
  font-size: 28px;
  font-weight: bold;
  color: #4ade80;
  text-shadow: 2px 2px 0 #1a4a2a;
  letter-spacing: 2px;
}

.logo-mark {
  width: 40px;
  height: 40px;
  background: #4ade80;
  color: #0f1117;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 24px;
  image-rendering: pixelated;
}

.nav-buttons {
  display: flex;
  gap: 4px;
  flex-wrap: wrap;
}

.nav-btn {
  background: #1a1e24;
  border: 2px solid #2a2e35;
  color: #8bc34a;
  font-family: 'VT323', monospace;
  font-size: 18px;
  padding: 6px 16px;
  cursor: pointer;
  transition: all 0.1s ease;
  image-rendering: pixelated;
}

.nav-btn:hover {
  background: #2a2e35;
  border-color: #4ade80;
}

.nav-btn.active {
  background: #4ade80;
  color: #0f1117;
  border-color: #4ade80;
}

.conn-status {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 14px;
  color: #6b7280;
}

.pixel-dot {
  width: 10px;
  height: 10px;
  background: #6b7280;
  image-rendering: pixelated;
}

.pixel-dot.live {
  background: #4ade80;
  box-shadow: 0 0 4px #4ade80;
}

.pixel-dot.err {
  background: #f87171;
}

/* Main container */
.container {
  max-width: 1300px;
  margin: 0 auto;
  padding: 24px 20px 60px;
}

/* Pages */
.page {
  display: none;
}

.page.active {
  display: block;
}

/* Pixel cards */
.stats-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 12px;
  margin-bottom: 32px;
}

.stat-card {
  background: #0f1117;
  border: 2px solid #2a2e35;
  padding: 16px 20px;
  image-rendering: pixelated;
}

.stat-label {
  font-size: 12px;
  color: #6b7280;
  letter-spacing: 1px;
  margin-bottom: 8px;
}

.stat-value {
  font-size: 32px;
  font-weight: bold;
  color: #4ade80;
}

.stat-value.gold {
  color: #f5c542;
}

/* Stock grid */
.stock-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
  gap: 12px;
  margin-top: 16px;
}

.stock-card {
  background: #0f1117;
  border: 2px solid #2a2e35;
  padding: 16px;
  position: relative;
  transition: all 0.1s ease;
}

.stock-card:hover {
  border-color: #4ade80;
  transform: translateY(-2px);
}

.stock-card.rare {
  border-color: #f5c542;
}

.stock-badge {
  position: absolute;
  top: 8px;
  right: 8px;
  font-size: 10px;
  background: #1a1e24;
  padding: 2px 8px;
  border: 1px solid #2a2e35;
}

.stock-cat {
  font-size: 11px;
  color: #6b7280;
  text-transform: uppercase;
  margin-bottom: 8px;
}

.stock-name {
  font-size: 18px;
  font-weight: bold;
  margin-bottom: 12px;
  color: #e2e8f0;
}

.stock-price {
  font-size: 22px;
  color: #4ade80;
}

.stock-price.gold {
  color: #f5c542;
}

.stock-rarity {
  display: inline-block;
  font-size: 11px;
  padding: 2px 8px;
  margin-top: 8px;
  border: 1px solid;
}

.rarity-common { border-color: #6b7280; color: #6b7280; }
.rarity-uncommon { border-color: #4ade80; color: #4ade80; }
.rarity-rare { border-color: #38bdf8; color: #38bdf8; }
.rarity-epic { border-color: #a78bfa; color: #a78bfa; }
.rarity-legendary { border-color: #f5c542; color: #f5c542; }

/* Events grid */
.events-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
  gap: 12px;
  margin-top: 16px;
}

.event-card {
  background: #0f1117;
  border: 2px solid #f5c54230;
  padding: 20px;
}

.event-type {
  display: inline-block;
  background: #f5c54220;
  padding: 2px 12px;
  font-size: 11px;
  margin-bottom: 12px;
  color: #f5c542;
}

.event-name {
  font-size: 20px;
  font-weight: bold;
  margin-bottom: 8px;
}

.event-phase {
  color: #a78bfa;
  margin-bottom: 12px;
}

.event-desc {
  font-size: 14px;
  color: #9ca3af;
  line-height: 1.5;
}

/* Shop styles */
.shop-header {
  background: #0f1117;
  border: 2px solid #2a2e35;
  padding: 24px;
  margin-bottom: 24px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 16px;
}

.shop-header h2 {
  font-size: 24px;
  margin-bottom: 8px;
}

.pixel-btn {
  background: #1a1e24;
  border: 2px solid #2a2e35;
  color: #8bc34a;
  font-family: 'VT323', monospace;
  font-size: 16px;
  padding: 8px 20px;
  cursor: pointer;
  transition: all 0.1s ease;
}

.pixel-btn:hover {
  border-color: #4ade80;
  background: #2a2e35;
}

.pixel-btn.green {
  background: #4ade80;
  color: #0f1117;
  border-color: #4ade80;
}

/* Listing grid */
.listings-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
  gap: 16px;
  margin-top: 16px;
}

.listing-card {
  background: #0f1117;
  border: 2px solid #2a2e35;
  overflow: hidden;
}

.listing-img {
  height: 160px;
  background: #1a1e24;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 48px;
  position: relative;
}

.listing-type {
  position: absolute;
  top: 8px;
  left: 8px;
  font-size: 10px;
  padding: 2px 8px;
  border: 1px solid;
}

.type-pet { border-color: #a78bfa; color: #a78bfa; }
.type-fruit { border-color: #f5c542; color: #f5c542; }

.listing-body {
  padding: 16px;
}

.listing-name {
  font-size: 18px;
  font-weight: bold;
  margin-bottom: 8px;
}

.listing-meta {
  display: flex;
  gap: 12px;
  font-size: 12px;
  color: #6b7280;
  margin-bottom: 12px;
}

.listing-price {
  font-size: 20px;
  color: #4ade80;
  margin-bottom: 8px;
}

.listing-price small {
  font-size: 12px;
  color: #6b7280;
}

.listing-contact {
  width: 100%;
  background: #1a1e24;
  border: 1px solid #2a2e35;
  color: #8bc34a;
  font-family: 'VT323', monospace;
  padding: 8px;
  cursor: pointer;
  margin-top: 12px;
}

.listing-contact:hover {
  border-color: #4ade80;
}

/* Upload form */
.upload-form {
  background: #0f1117;
  border: 2px solid #2a2e35;
  padding: 24px;
  margin-bottom: 24px;
  display: none;
}

.upload-form.open {
  display: block;
}

.form-grid {
  display: grid;
  grid-template-columns: 200px 1fr;
  gap: 24px;
}

@media (max-width: 700px) {
  .form-grid { grid-template-columns: 1fr; }
}

.image-drop {
  aspect-ratio: 1;
  border: 2px dashed #2a2e35;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 8px;
  cursor: pointer;
  text-align: center;
  padding: 16px;
}

.image-drop.has-img {
  border-style: solid;
  padding: 0;
}

.image-drop.has-img img {
  width: 100%;
  height: 100%;
  object-fit: cover;
}

.fields-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}

.field {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.field.full {
  grid-column: 1/-1;
}

.field label {
  font-size: 12px;
  color: #6b7280;
}

.field input, .field select, .field textarea {
  background: #1a1e24;
  border: 1px solid #2a2e35;
  padding: 8px 12px;
  font-family: 'VT323', monospace;
  font-size: 14px;
  color: #e2e8f0;
}

.field input:focus, .field select:focus, .field textarea:focus {
  outline: none;
  border-color: #4ade80;
}

.pay-toggle {
  display: flex;
  gap: 8px;
  margin-top: 4px;
}

.pay-opt {
  flex: 1;
  background: #1a1e24;
  border: 1px solid #2a2e35;
  padding: 8px;
  cursor: pointer;
  text-align: center;
  font-family: 'VT323', monospace;
}

.pay-opt.sel-robux {
  border-color: #4ade80;
  background: #4ade8020;
  color: #4ade80;
}

.pay-opt.sel-paypal {
  border-color: #38bdf8;
  background: #38bdf820;
  color: #38bdf8;
}

/* Help section */
.help-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
  gap: 12px;
  margin-bottom: 24px;
}

.help-card {
  background: #0f1117;
  border: 2px solid #2a2e35;
  padding: 20px;
}

.help-icon {
  font-size: 28px;
  margin-bottom: 12px;
}

.help-title {
  font-size: 18px;
  margin-bottom: 8px;
}

.help-text {
  font-size: 14px;
  color: #9ca3af;
  line-height: 1.6;
}

.help-text code {
  background: #1a1e24;
  padding: 2px 6px;
  color: #4ade80;
}

.faq-wrap {
  background: #0f1117;
  border: 2px solid #2a2e35;
  padding: 20px;
}

.faq-item {
  border-bottom: 1px solid #2a2e35;
}

.faq-item:last-child {
  border-bottom: none;
}

.faq-question {
  display: flex;
  justify-content: space-between;
  padding: 14px 0;
  cursor: pointer;
  font-size: 16px;
  font-weight: bold;
}

.faq-question:hover {
  color: #4ade80;
}

.faq-arrow {
  transition: transform 0.2s;
}

.faq-item.open .faq-arrow {
  transform: rotate(180deg);
}

.faq-answer {
  max-height: 0;
  overflow: hidden;
  transition: max-height 0.3s ease;
  font-size: 14px;
  color: #9ca3af;
  line-height: 1.6;
}

.faq-item.open .faq-answer {
  max-height: 200px;
  padding-bottom: 16px;
}

/* Filter pills */
.filter-bar {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
  margin-bottom: 16px;
}

.filter-pill {
  background: #1a1e24;
  border: 1px solid #2a2e35;
  padding: 4px 14px;
  cursor: pointer;
  font-family: 'VT323', monospace;
  font-size: 14px;
}

.filter-pill:hover, .filter-pill.active {
  border-color: #4ade80;
  color: #4ade80;
}

/* Empty state */
.empty {
  text-align: center;
  padding: 60px 20px;
  color: #6b7280;
}

.empty-icon {
  font-size: 48px;
  display: block;
  margin-bottom: 16px;
}

/* Loading shimmer */
.shimmer {
  background: linear-gradient(90deg, #0f1117 25%, #1a1e24 50%, #0f1117 75%);
  background-size: 200% 100%;
  animation: shimmer 1s infinite;
  height: 150px;
  border: 2px solid #2a2e35;
}

@keyframes shimmer {
  0% { background-position: -200% 0; }
  100% { background-position: 200% 0; }
}

/* Toast */
.toast {
  position: fixed;
  bottom: 24px;
  right: 24px;
  background: #1a1e24;
  border: 2px solid #4ade80;
  padding: 12px 20px;
  font-size: 14px;
  transform: translateY(100px);
  opacity: 0;
  transition: all 0.3s;
  z-index: 1000;
}

.toast.show {
  transform: translateY(0);
  opacity: 1;
}

/* Misc */
.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
  flex-wrap: wrap;
  gap: 12px;
}

.section-title {
  font-size: 24px;
}

.divider {
  height: 2px;
  background: #2a2e35;
  margin: 24px 0;
}

.feature-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 12px;
  margin-top: 20px;
}

.feature-card {
  background: #0f1117;
  border: 2px solid #2a2e35;
  padding: 20px;
}

.hero {
  padding: 48px 0 32px;
}

.hero h1 {
  font-size: 48px;
  margin-bottom: 16px;
}

.hero h1 em {
  color: #4ade80;
  font-style: normal;
}

.hero p {
  font-size: 18px;
  color: #9ca3af;
  max-width: 500px;
  margin-bottom: 24px;
}

.hero-eyebrow {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  background: #1a1e24;
  padding: 4px 12px;
  margin-bottom: 20px;
  border: 1px solid #2a2e35;
}

.button-group {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
}
</style>
</head>
<body>
<div class="pixel-bg"></div>

<div class="pixel-header">
  <div class="header-inner">
    <div class="logo">
      <div class="logo-mark">[X]</div>
      <span>XOTIIS GARDEN</span>
    </div>
    <div class="nav-buttons">
      <button class="nav-btn active" onclick="nav('home',this)">[HOME]</button>
      <button class="nav-btn" onclick="nav('stock',this)">[STOCK]</button>
      <button class="nav-btn" onclick="nav('events',this)">[EVENTS]</button>
      <button class="nav-btn" onclick="nav('shop',this)">[SHOP]</button>
      <button class="nav-btn" onclick="nav('help',this)">[HELP]</button>
    </div>
    <div class="conn-status">
      <div class="pixel-dot" id="connDot"></div>
      <span id="connLabel">CONNECTING...</span>
    </div>
  </div>
</div>

<div class="container">
  <!-- HOME PAGE -->
  <div class="page active" id="page-home">
    <div class="hero">
      <div class="hero-eyebrow">
        <div class="pixel-dot live"></div>
        <span>LIVE STOCK MONITOR</span>
      </div>
      <h1>TRACK, TRADE &amp; <em>GROW</em></h1>
      <p>Real-time stock alerts, event detection, and community marketplace.</p>
      <div class="button-group">
        <button class="pixel-btn green" onclick="nav('stock',document.querySelectorAll('.nav-btn')[1])">[ VIEW STOCK ]</button>
        <button class="pixel-btn" onclick="nav('shop',document.querySelectorAll('.nav-btn')[3])">[ OPEN SHOP ]</button>
      </div>
    </div>

    <div class="stats-row">
      <div class="stat-card"><div class="stat-label">ITEMS IN STOCK</div><div class="stat-value" id="statStock">--</div></div>
      <div class="stat-card"><div class="stat-label">ACTIVE EVENTS</div><div class="stat-value gold" id="statEvents">--</div></div>
      <div class="stat-card"><div class="stat-label">SHOP LISTINGS</div><div class="stat-value" id="statListings">0</div></div>
    </div>

    <div class="divider"></div>
    <div class="section-title">[ WHAT WE OFFER ]</div>
    <div class="feature-grid">
      <div class="feature-card"><div class="help-icon">[+]</div><div class="help-title">Live Stock Feed</div><div class="help-text">Real-time WebSocket connection to game data.</div></div>
      <div class="feature-card"><div class="help-icon">[*]</div><div class="help-title">Event Tracking</div><div class="help-text">Automatic seasonal event detection.</div></div>
      <div class="feature-card"><div class="help-icon">[$]</div><div class="help-title">Xotiis Shop</div><div class="help-text">Sell pets and fruits to the community.</div></div>
      <div class="feature-card"><div class="help-icon">[@]</div><div class="help-title">Discord Bot</div><div class="help-text">Get pinged when items appear in stock.</div></div>
    </div>
  </div>

  <!-- STOCK PAGE -->
  <div class="page" id="page-stock">
    <div class="section-header">
      <div><div class="section-title">[ LIVE STOCK ]</div><div style="font-size:12px;color:#6b7280;">Items currently in shop</div></div>
      <button class="pixel-btn" onclick="loadStock()">[ REFRESH ]</button>
    </div>
    <div class="filter-bar" id="stockFilters">
      <button class="filter-pill active" onclick="filterStock('all',this)">ALL</button>
      <button class="filter-pill" onclick="filterStock('seeds',this)">SEEDS</button>
      <button class="filter-pill" onclick="filterStock('gear',this)">GEAR</button>
      <button class="filter-pill" onclick="filterStock('eggs',this)">EGGS</button>
      <button class="filter-pill" onclick="filterStock('event',this)">EVENT</button>
      <button class="filter-pill" onclick="filterStock('pet',this)">PETS</button>
    </div>
    <div id="stockGrid" class="stock-grid"></div>
  </div>

  <!-- EVENTS PAGE -->
  <div class="page" id="page-events">
    <div class="section-header">
      <div><div class="section-title">[ ACTIVE EVENTS ]</div></div>
      <button class="pixel-btn" onclick="loadEvents()">[ REFRESH ]</button>
    </div>
    <div id="eventsGrid" class="events-grid"></div>
  </div>

  <!-- SHOP PAGE -->
  <div class="page" id="page-shop">
    <div class="shop-header">
      <div><h2>[ XOTIIS SHOP ]</h2><p style="font-size:14px;color:#9ca3af;">Buy and sell with the community</p></div>
      <div class="button-group">
        <button class="pixel-btn green" onclick="openForm('pet')">[ SELL PET ]</button>
        <button class="pixel-btn" onclick="openForm('fruit')">[ SELL FRUIT ]</button>
      </div>
    </div>

    <!-- Upload Form -->
    <div class="upload-form" id="uploadForm">
      <div class="section-title" id="formTitle">SELL A PET</div>
      <div class="form-grid">
        <div>
          <div class="image-drop" id="imageDrop" onclick="document.getElementById('fileInput').click()">
            <span id="dropIcon">[+]</span>
            <span>CLICK TO UPLOAD</span>
            <span style="font-size:10px;">PNG / JPG</span>
          </div>
          <input type="file" id="fileInput" accept="image/*" style="display:none" onchange="handleImage(this)">
        </div>
        <div>
          <input type="hidden" id="formType" value="pet">
          <div class="fields-grid">
            <div class="field"><label>NAME</label><input id="itemName" placeholder="e.g. Crystal Bunny"></div>
            <div class="field"><label>PRICE (SHEKELS)</label><input id="itemPrice" type="number" placeholder="50000"></div>
            <div class="field"><label>WEIGHT (KG)</label><input id="itemKg" type="number" step="0.1" placeholder="2.5"></div>
            <div class="field"><label>AGE (DAYS)</label><input id="itemAge" type="number" placeholder="30"></div>
            <div class="field full"><label>DESCRIPTION</label><textarea id="itemDesc" placeholder="Describe your listing..."></textarea></div>
            <div class="field full">
              <label>PAYMENT TYPE</label>
              <div class="pay-toggle">
                <button class="pay-opt sel-robux" id="payRobux" onclick="setPayment('robux')">[ ROBUX ]</button>
                <button class="pay-opt" id="payPaypal" onclick="setPayment('paypal')">[ PAYPAL ]</button>
              </div>
            </div>
          </div>
          <div class="button-group" style="margin-top:20px;">
            <button class="pixel-btn green" onclick="submitListing()">[ LIST FOR SALE ]</button>
            <button class="pixel-btn" onclick="closeForm()">[ CANCEL ]</button>
          </div>
        </div>
      </div>
    </div>

    <div class="section-header">
      <div class="section-title">[ LISTINGS ]</div>
      <div class="filter-bar" style="margin:0;">
        <button class="filter-pill active" onclick="filterListings('all',this)">ALL</button>
        <button class="filter-pill" onclick="filterListings('pet',this)">PETS</button>
        <button class="filter-pill" onclick="filterListings('fruit',this)">FRUITS</button>
      </div>
    </div>
    <div id="listingsGrid" class="listings-grid"></div>
  </div>

  <!-- HELP PAGE -->
  <div class="page" id="page-help">
    <div class="section-title">[ HELP & INFO ]</div>
    <div class="help-grid">
      <div class="help-card"><div class="help-icon">[?]</div><div class="help-title">Stock Tracking</div><div class="help-text">WebSocket connection provides real-time updates when items appear in shop.</div></div>
      <div class="help-card"><div class="help-icon">[@]</div><div class="help-title">Discord Commands</div><div class="help-text"><code>/watch &lt;item&gt;</code> - Get alerts<br><code>/stock &lt;item&gt;</code> - Check stock<br><code>/events</code> - Active events</div></div>
      <div class="help-card"><div class="help-icon">[$]</div><div class="help-title">Selling Items</div><div class="help-text">Click "Sell Pet/Fruit", upload image, set price in Shekels, choose Robux or PayPal.</div></div>
      <div class="help-card"><div class="help-icon">[!]</div><div class="help-title">Payment Options</div><div class="help-text"><span style="color:#4ade80">ROBUX</span> or <span style="color:#38bdf8">PAYPAL</span>. Always verify payment before trading.</div></div>
    </div>
    <div class="faq-wrap">
      <div class="section-title" style="margin-bottom:16px;">[ FAQ ]</div>
      <div id="faqList"></div>
    </div>
  </div>
</div>

<div class="toast" id="toastMsg"></div>

<script>
// ==================== CONFIG ====================
const API_BASE = window.location.origin;

// ==================== GLOBAL STATE ====================
let allStock = [];
let stockFilter = 'all';
let listings = JSON.parse(localStorage.getItem('xotiis_v2') || '[]');
let listingFilter = 'all';
let currentPayment = 'robux';
let currentImageData = null;
let currentFormType = 'pet';

// ==================== INIT ====================
document.addEventListener('DOMContentLoaded', () => {
  renderFaq();
  loadStats();
  loadStock();
  loadEvents();
  renderListings();
  checkConnection();
  setInterval(() => { loadStats(); if (document.getElementById('page-stock').classList.contains('active')) loadStock(); }, 30000);
  setInterval(() => { if (document.getElementById('page-shop').classList.contains('active')) renderListings(); }, 60000);
});

// ==================== HELPERS ====================
function formatPrice(n) {
  if (n >= 1e6) return (n/1e6).toFixed(2) + 'M';
  if (n >= 1e3) return (n/1e3).toFixed(1) + 'K';
  return String(n);
}

function escapeHtml(s) {
  const div = document.createElement('div');
  div.textContent = s || '';
  return div.innerHTML;
}

function showToast(msg) {
  const t = document.getElementById('toastMsg');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 3000);
}

// ==================== API ====================
async function fetchAPI(endpoint) {
  try {
    const res = await fetch(`${API_BASE}${endpoint}`);
    if (!res.ok) throw new Error();
    return await res.json();
  } catch(e) {
    return null;
  }
}

async function checkConnection() {
  const data = await fetchAPI('/api/stats');
  const dot = document.getElementById('connDot');
  const label = document.getElementById('connLabel');
  if (data && data.status === 'online') {
    dot.className = 'pixel-dot live';
    label.textContent = 'LIVE';
  } else {
    dot.className = 'pixel-dot err';
    label.textContent = 'OFFLINE';
  }
}

// ==================== STATS ====================
async function loadStats() {
  const data = await fetchAPI('/api/stats');
  if (data) {
    document.getElementById('statStock').textContent = data.items_in_stock || '0';
    document.getElementById('statEvents').textContent = data.active_events || '0';
  }
  document.getElementById('statListings').textContent = listings.length;
}

// ==================== STOCK ====================
async function loadStock() {
  const container = document.getElementById('stockGrid');
  container.innerHTML = '<div class="shimmer" style="grid-column:1/-1;"></div>';
  
  const data = await fetchAPI('/api/stock');
  if (!data) {
    container.innerHTML = '<div class="empty"><span class="empty-icon">[X]</span><h3>Could not load stock</h3></div>';
    return;
  }
  
  allStock = data;
  renderStock();
}

function renderStock() {
  const container = document.getElementById('stockGrid');
  let items = allStock.filter(i => i.in_stock);
  if (stockFilter !== 'all') {
    items = items.filter(i => (i.category || '').toLowerCase() === stockFilter);
  }
  
  if (!items.length) {
    container.innerHTML = '<div class="empty"><span class="empty-icon">[ ]</span><h3>No items in stock</h3></div>';
    return;
  }
  
  const rarityClass = (r) => {
    const map = { 'common':'rarity-common', 'uncommon':'rarity-uncommon', 'rare':'rarity-rare', 'epic':'rarity-epic', 'legendary':'rarity-legendary' };
    return map[r?.toLowerCase()] || 'rarity-common';
  };
  
  container.innerHTML = items.map(i => `
    <div class="stock-card ${i.rarity === 'legendary' || i.rarity === 'epic' ? 'rare' : ''}">
      <span class="stock-badge">[IN STOCK]</span>
      <div class="stock-cat">${escapeHtml(i.category || 'general')}</div>
      <div class="stock-name">${escapeHtml(i.name)}</div>
      <div class="stock-price ${i.rarity === 'legendary' ? 'gold' : ''}">:coin: ${formatPrice(i.price)}</div>
      <span class="stock-rarity ${rarityClass(i.rarity)}">${(i.rarity || 'COMMON').toUpperCase()}</span>
    </div>
  `).join('');
}

function filterStock(cat, btn) {
  stockFilter = cat;
  document.querySelectorAll('#stockFilters .filter-pill').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderStock();
}

// ==================== EVENTS ====================
async function loadEvents() {
  const container = document.getElementById('eventsGrid');
  container.innerHTML = '<div class="shimmer" style="height:160px;grid-column:1/-1;"></div>';
  
  const data = await fetchAPI('/api/events');
  if (!data || !data.length) {
    container.innerHTML = '<div class="empty"><span class="empty-icon">[*]</span><h3>No active events</h3></div>';
    return;
  }
  
  container.innerHTML = data.map(e => `
    <div class="event-card">
      <div class="event-type">${escapeHtml(e.type || 'EVENT')}</div>
      <div class="event-name">${escapeHtml(e.name)}</div>
      ${e.phase ? `<div class="event-phase">PHASE: ${escapeHtml(e.phase)}</div>` : ''}
      <div class="event-desc">${escapeHtml(e.description || 'Seasonal event active now!')}</div>
    </div>
  `).join('');
}

// ==================== SHOP ====================
function saveListings() { localStorage.setItem('xotiis_v2', JSON.stringify(listings)); }

function openForm(type) {
  currentFormType = type;
  document.getElementById('formTitle').textContent = type === 'pet' ? 'SELL A PET' : 'SELL A FRUIT';
  document.getElementById('dropIcon').textContent = type === 'pet' ? '[P]' : '[F]';
  document.getElementById('formType').value = type;
  document.getElementById('itemName').value = '';
  document.getElementById('itemPrice').value = '';
  document.getElementById('itemKg').value = '';
  document.getElementById('itemAge').value = '';
  document.getElementById('itemDesc').value = '';
  currentImageData = null;
  currentPayment = 'robux';
  updatePayUI();
  
  const drop = document.getElementById('imageDrop');
  drop.className = 'image-drop';
  drop.innerHTML = `<span id="dropIcon">${type === 'pet' ? '[P]' : '[F]'}</span><span>CLICK TO UPLOAD</span><span style="font-size:10px;">PNG / JPG</span>`;
  drop.onclick = () => document.getElementById('fileInput').click();
  
  document.getElementById('uploadForm').classList.add('open');
  document.getElementById('uploadForm').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function closeForm() {
  document.getElementById('uploadForm').classList.remove('open');
  currentImageData = null;
}

function handleImage(input) {
  const file = input.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = (e) => {
    currentImageData = e.target.result;
    const drop = document.getElementById('imageDrop');
    drop.className = 'image-drop has-img';
    drop.innerHTML = `<img src="${currentImageData}" alt="preview">`;
    drop.onclick = () => document.getElementById('fileInput').click();
  };
  reader.readAsDataURL(file);
}

function setPayment(pay) {
  currentPayment = pay;
  updatePayUI();
}

function updatePayUI() {
  const robuxBtn = document.getElementById('payRobux');
  const paypalBtn = document.getElementById('payPaypal');
  if (currentPayment === 'robux') {
    robuxBtn.className = 'pay-opt sel-robux';
    paypalBtn.className = 'pay-opt';
  } else {
    robuxBtn.className = 'pay-opt';
    paypalBtn.className = 'pay-opt sel-paypal';
  }
}

function submitListing() {
  const name = document.getElementById('itemName').value.trim();
  const price = parseInt(document.getElementById('itemPrice').value);
  if (!name || !price) {
    showToast('[!] Name and price required');
    return;
  }
  
  const listing = {
    id: Date.now(),
    type: currentFormType,
    name: name,
    price: price,
    kg: parseFloat(document.getElementById('itemKg').value) || null,
    age: parseInt(document.getElementById('itemAge').value) || null,
    desc: document.getElementById('itemDesc').value.trim() || null,
    payment: currentPayment,
    image: currentImageData,
    date: new Date().toLocaleDateString()
  };
  
  listings.unshift(listing);
  saveListings();
  closeForm();
  renderListings();
  document.getElementById('statListings').textContent = listings.length;
  showToast('[+] Listing added!');
}

function filterListings(cat, btn) {
  listingFilter = cat;
  document.querySelectorAll('#page-shop .filter-pill').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  renderListings();
}

function renderListings() {
  const container = document.getElementById('listingsGrid');
  let items = listingFilter === 'all' ? listings : listings.filter(l => l.type === listingFilter);
  
  if (!items.length) {
    container.innerHTML = '<div class="empty"><span class="empty-icon">[$]</span><h3>No listings yet</h3><p>Click "Sell Pet" or "Sell Fruit" to start</p></div>';
    return;
  }
  
  container.innerHTML = items.map(l => `
    <div class="listing-card">
      <div class="listing-img">
        ${l.image ? `<img src="${l.image}" style="width:100%;height:100%;object-fit:cover;">` : `<span style="font-size:48px;">${l.type === 'pet' ? '[P]' : '[F]'}</span>`}
        <span class="listing-type type-${l.type}">[${l.type.toUpperCase()}]</span>
      </div>
      <div class="listing-body">
        <div class="listing-name">${escapeHtml(l.name)}</div>
        <div class="listing-meta">
          ${l.kg ? `<span>⚖️ ${l.kg}kg</span>` : ''}
          ${l.age ? `<span>📅 ${l.age} days</span>` : ''}
          <span>📅 ${l.date}</span>
        </div>
        ${l.desc ? `<div style="font-size:12px;color:#9ca3af;margin-bottom:10px;">${escapeHtml(l.desc)}</div>` : ''}
        <div class="listing-price">:coin: ${formatPrice(l.price)} <small>shekels</small></div>
        <div style="font-size:12px;margin-bottom:8px;">${l.payment === 'robux' ? '[ROBUX]' : '[PAYPAL]'}</div>
        <button class="listing-contact" onclick="showToast('Contact seller on Discord to purchase!')">[ CONTACT SELLER ]</button>
      </div>
    </div>
  `).join('');
}

// ==================== FAQ ====================
const faqs = [
  { q: 'How does live stock tracking work?', a: 'The backend connects to the game\'s WebSocket feed. Items appear the moment they hit the shop.' },
  { q: 'What are Shekels?', a: 'Shekels are the in-game currency. You set your price in Shekels as a reference.' },
  { q: 'How do I get Discord alerts?', a: 'Use /watch <item> in Discord. Use /priority_high for instant DMs.' },
  { q: 'Is trading safe?', a: 'Always verify payment before trading. Use a middleman for high-value trades.' },
  { q: 'How do I remove a listing?', a: 'Listings are stored locally. Clear browser storage to remove.' }
];

function renderFaq() {
  const container = document.getElementById('faqList');
  container.innerHTML = faqs.map((f, i) => `
    <div class="faq-item" id="faq${i}">
      <div class="faq-question" onclick="toggleFaq(${i})">
        ${escapeHtml(f.q)}
        <span class="faq-arrow">▼</span>
      </div>
      <div class="faq-answer">${escapeHtml(f.a)}</div>
    </div>
  `).join('');
}

function toggleFaq(i) {
  document.getElementById(`faq${i}`).classList.toggle('open');
}

// ==================== NAVIGATION ====================
function nav(pageId, btn) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
  document.getElementById(`page-${pageId}`).classList.add('active');
  if (btn) btn.classList.add('active');
  
  if (pageId === 'stock') loadStock();
  if (pageId === 'events') loadEvents();
  if (pageId === 'shop') renderListings();
  window.scrollTo({ top: 0, behavior: 'smooth' });
}
</script>
</body>
</html>
'''

@app.get("/")
async def root():
    return HTMLResponse(content=HTML_PAGE)

# ============ Main Entry Point ============
def main():
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"Starting server on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")

if __name__ == "__main__":
    if not DISCORD_TOKEN:
        logger.warning("DISCORD_TOKEN not set! Bot will not work.")
    main()
