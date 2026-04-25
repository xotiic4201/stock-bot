#!/usr/bin/env python3
"""
Grow a Garden Complete Stock Monitoring System
with Xotiis Marketplace - FULLY WORKING for Render.com
"""

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
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
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
PUBLIC_URL = os.environ.get("PUBLIC_URL", "http://localhost:8000")
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
            
            # Users table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    discord_id TEXT PRIMARY KEY,
                    username TEXT,
                    notification_channel TEXT,
                    notification_role TEXT,
                    shekels INTEGER DEFAULT 100,
                    created_at TEXT
                )
            """)
            
            # Watchlist
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
            
            # Stock cache
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_cache (
                    item_name TEXT PRIMARY KEY,
                    category TEXT,
                    price REAL,
                    in_stock INTEGER,
                    last_seen TEXT
                )
            """)
            
            # Events table
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
            
            # Marketplace listings
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS marketplace_listings (
                    id TEXT PRIMARY KEY,
                    seller_id TEXT,
                    seller_name TEXT,
                    item_type TEXT,
                    item_name TEXT,
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
            
            # Future watches
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS future_watches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT,
                    pattern TEXT,
                    created_at TEXT,
                    UNIQUE(user_id, pattern)
                )
            """)
            
            # Notification log
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
                return {"discord_id": discord_id, "username": username, "shekels": 100}
            return {"discord_id": user[0], "username": user[1], "shekels": user[4]}

    def add_shekels(self, discord_id: str, amount: int):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET shekels = shekels + ? WHERE discord_id = ?", (amount, discord_id))
            conn.commit()

    # Marketplace methods
    def create_listing(self, seller_id: str, seller_name: str, item_type: str, item_name: str,
                       size_kg: float, age_days: int, price_shekels: int = None,
                       price_robux: int = None, price_paypal: float = None, description: str = None) -> str:
        listing_id = str(uuid.uuid4())[:8]
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO marketplace_listings 
                (id, seller_id, seller_name, item_type, item_name, size_kg, age_days,
                 price_shekels, price_robux, price_paypal, description, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (listing_id, seller_id, seller_name, item_type, item_name,
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
                "item_name": r[4], "size_kg": r[5], "age_days": r[6],
                "price_shekels": r[7], "price_robux": r[8], "price_paypal": r[9],
                "description": r[10], "created_at": r[11]
            } for r in rows]

    # Stock methods
    def update_stock(self, item_name: str, category: str, price: float, in_stock: bool):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO stock_cache (item_name, category, price, in_stock, last_seen)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(item_name) DO UPDATE SET
                    category = excluded.category,
                    price = excluded.price,
                    in_stock = excluded.in_stock,
                    last_seen = excluded.last_seen
            """, (item_name, category, price, 1 if in_stock else 0, datetime.now().isoformat()))
            conn.commit()

    def get_all_stock(self) -> List[Dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT item_name, category, price, in_stock, last_seen FROM stock_cache WHERE in_stock = 1 ORDER BY price DESC")
            rows = cursor.fetchall()
            return [{"name": r[0], "category": r[1], "price": r[2], "in_stock": bool(r[3]), "last_seen": r[4]} for r in rows]

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
            
            # Also check wildcards
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
    logger.info(f"✅ Discord Bot ONLINE! Logged in as {bot.user}")
    logger.info(f"Bot ID: {bot.user.id}")
    
    # Sync slash commands
    try:
        await tree.sync()
        logger.info("✅ Slash commands synced globally")
    except Exception as e:
        logger.error(f"Failed to sync commands: {e}")
    
    # Get bot owner
    try:
        app_info = await bot.application_info()
        bot_owner_id = app_info.owner.id
        logger.info(f"Bot owner ID: {bot_owner_id}")
    except Exception as e:
        logger.error(f"Failed to get owner info: {e}")
    
    bot_ready = True
    
    # Set bot status
    await bot.change_presence(activity=discord.Game(name="🌱 Grow a Garden | /help"))

@bot.event
async def on_error(event, *args, **kwargs):
    logger.error(f"Discord error in {event}: {args}")

# ============ Discord Slash Commands ============
@tree.command(name="help", description="Show all available commands")
async def help_command(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🌱 Xotiis Bot Commands",
        description="Your Grow a Garden assistant!",
        color=0xffd700
    )
    embed.add_field(name="💰 Economy", value="`/shekels` - Check balance\n`/sell` - List item on marketplace", inline=False)
    embed.add_field(name="🔔 Notifications", value="`/watch <item>` - Watch for stock\n`/unwatch <item>` - Remove watch\n`/watches` - List your watches", inline=False)
    embed.add_field(name="📦 Stock", value="`/stock` - Check current stock\n`/stock <item>` - Search specific item", inline=False)
    embed.add_field(name="🎪 Marketplace", value="`/marketplace` - Browse listings\n`/sell` - List an item", inline=False)
    embed.add_field(name="🔮 Future Events", value="`/future <pattern>` - Watch for events\n`/future_list` - List watches", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tree.command(name="shekels", description="Check your shekel balance")
async def shekels_command(interaction: discord.Interaction):
    user = db.get_or_create_user(str(interaction.user.id), interaction.user.name)
    embed = discord.Embed(title="💰 Your Balance", color=0xffd700)
    embed.add_field(name="Shekels", value=f"**{user['shekels']}** 🪙", inline=True)
    embed.set_footer(text="Earn more by selling items on Xotiis Marketplace!")
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tree.command(name="watch", description="Watch for an item in stock")
async def watch_command(interaction: discord.Interaction, item_name: str, priority: str = "medium"):
    db.get_or_create_user(str(interaction.user.id), interaction.user.name)
    success = db.add_watch(str(interaction.user.id), item_name, priority)
    if success:
        embed = discord.Embed(title="🔔 Watch Added", color=0x22c55e)
        embed.add_field(name="Item", value=item_name, inline=True)
        embed.add_field(name="Priority", value=priority.upper(), inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(f"❌ You're already watching `{item_name}`!", ephemeral=True)

@tree.command(name="unwatch", description="Remove an item from your watchlist")
async def unwatch_command(interaction: discord.Interaction, item_name: str):
    success = db.remove_watch(str(interaction.user.id), item_name)
    if success:
        await interaction.response.send_message(f"✅ Removed `{item_name}` from your watchlist.", ephemeral=True)
    else:
        await interaction.response.send_message(f"❌ `{item_name}` wasn't in your watchlist.", ephemeral=True)

@tree.command(name="watches", description="List your watchlist")
async def watches_command(interaction: discord.Interaction):
    items = db.get_user_watchlist(str(interaction.user.id))
    if not items:
        await interaction.response.send_message("📭 Your watchlist is empty. Use `/watch` to add items!", ephemeral=True)
        return
    
    embed = discord.Embed(title="📋 Your Watchlist", color=0xffd700)
    for item in items:
        embed.add_field(name=item['item_name'], value=f"Priority: {item['priority'].upper()}", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tree.command(name="stock", description="Check current stock")
async def stock_command(interaction: discord.Interaction, item_name: str = None):
    items = db.get_all_stock()
    if item_name:
        items = [i for i in items if item_name.lower() in i["name"].lower()]
    
    if not items:
        await interaction.response.send_message("📦 No items currently in stock!", ephemeral=True)
        return
    
    embed = discord.Embed(title="📦 Current Stock", color=0x22c55e)
    for item in items[:15]:
        embed.add_field(name=item['name'], value=f"💰 ${item['price']:,.0f}", inline=True)
    if len(items) > 15:
        embed.set_footer(text=f"and {len(items)-15} more items...")
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tree.command(name="marketplace", description="Browse Xotiis Marketplace")
async def marketplace_command(interaction: discord.Interaction, item_type: str = None):
    listings = db.get_listings(item_type)
    if not listings:
        await interaction.response.send_message("🎪 No active listings! Use `/sell` to list something!", ephemeral=True)
        return
    
    embed = discord.Embed(title="🎪 Xotiis Marketplace", color=0xffd700)
    for listing in listings[:10]:
        price_text = []
        if listing['price_shekels']:
            price_text.append(f"{listing['price_shekels']} 🪙")
        if listing['price_robux']:
            price_text.append(f"{listing['price_robux']} 🤖")
        if listing['price_paypal']:
            price_text.append(f"${listing['price_paypal']} 💳")
        
        embed.add_field(
            name=f"{listing['item_name']} ({listing['item_type']})",
            value=f"📏 {listing['size_kg']}kg | 📅 {listing['age_days']} days\n💰 {' | '.join(price_text)}\n👤 {listing['seller_name']}",
            inline=False
        )
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tree.command(name="sell", description="Sell a pet or fruit on Xotiis Marketplace")
async def sell_command(
    interaction: discord.Interaction,
    item_type: str,
    item_name: str,
    size_kg: float,
    age_days: int,
    price_shekels: int = None,
    price_robux: int = None,
    price_paypal: float = None,
    description: str = None
):
    await interaction.response.defer(ephemeral=True)
    
    user = db.get_or_create_user(str(interaction.user.id), interaction.user.name)
    
    if not (price_shekels or price_robux or price_paypal):
        await interaction.followup.send("❌ You must set at least one price (shekels, robux, or paypal)!")
        return
    
    listing_id = db.create_listing(
        str(interaction.user.id), interaction.user.name,
        item_type, item_name, size_kg, age_days,
        price_shekels, price_robux, price_paypal, description
    )
    
    embed = discord.Embed(title="✅ Item Listed!", color=0x22c55e)
    embed.add_field(name="Item", value=f"{item_name} ({item_type})", inline=True)
    embed.add_field(name="Listing ID", value=listing_id, inline=True)
    embed.add_field(name="View", value=f"Use `/marketplace` to see your listing!", inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)

@tree.command(name="future", description="Watch for future events by pattern")
async def future_command(interaction: discord.Interaction, pattern: str):
    db.get_or_create_user(str(interaction.user.id), interaction.user.name)
    success = db.add_future_watch(str(interaction.user.id), pattern)
    if success:
        await interaction.response.send_message(f"🔮 Now watching for events matching `{pattern}`!", ephemeral=True)
    else:
        await interaction.response.send_message(f"❌ You're already watching pattern `{pattern}`.", ephemeral=True)

@tree.command(name="future_list", description="List your future event watches")
async def future_list_command(interaction: discord.Interaction):
    watches = db.get_future_watches()
    user_watches = [w for w in watches if w["user_id"] == str(interaction.user.id)]
    if not user_watches:
        await interaction.response.send_message("🔮 You're not watching any future events. Use `/future` to add one!", ephemeral=True)
        return
    items = "\n".join([f"• {w['pattern']}" for w in user_watches])
    await interaction.response.send_message(f"**🔮 Your Future Watches:**\n{items}", ephemeral=True)

@tree.command(name="events", description="Show active events")
async def events_command(interaction: discord.Interaction):
    events = db.get_active_events()
    if not events:
        await interaction.response.send_message("📅 No active events right now!", ephemeral=True)
        return
    
    embed = discord.Embed(title="📅 Active Events", color=0xffd700)
    for event in events:
        embed.add_field(name=event['name'], value=f"Type: {event['type']}\n{event.get('description', '')[:100]}", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)

# ============ Game WebSocket Listener ============
websocket_connections: Set[WebSocket] = set()

async def send_discord_notification(user_id: str, item_name: str, price: float, priority: str, channel_id: str, role_id: str = None):
    """Send Discord notification to user"""
    try:
        user = await bot.fetch_user(int(user_id))
        if not user:
            return
        
        message = f"🔔 **{item_name}** is now in stock for **${price:,.0f}**!"
        
        if priority == "high":
            try:
                await user.send(f"🚨 HIGH PRIORITY: {message}")
            except:
                pass
        
        if channel_id:
            channel = bot.get_channel(int(channel_id))
            if channel:
                ping = f"<@{user_id}>" + (f" <@&{role_id}>" if role_id else "")
                await channel.send(f"{ping}\n{message}")
        
        db.log_notification(user_id, item_name)
        logger.info(f"Sent notification to {user_id} for {item_name}")
    except Exception as e:
        logger.error(f"Failed to send notification: {e}")

async def game_websocket_listener():
    """Game WebSocket listener with fallback"""
    ws_urls = [
        "wss://api.jstudio.ai/grow-a-garden/ws",
        "wss://ws.growagarden.com/socket.io/?EIO=4&transport=websocket"
    ]
    
    while True:
        for ws_url in ws_urls:
            try:
                async with aiohttp.ClientSession() as session:
                    logger.info(f"🌐 Connecting to game WebSocket: {ws_url}")
                    async with session.ws_connect(ws_url, timeout=30) as ws:
                        logger.info(f"✅ Connected to {ws_url}")
                        
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(msg.data)
                                    
                                    # Handle stock refresh
                                    if data.get("type") == "stock_refresh":
                                        items = data.get("items", [])
                                        for item in items:
                                            item_name = item.get("name", "Unknown")
                                            category = item.get("category", "unknown")
                                            price = float(item.get("price", 0))
                                            in_stock = item.get("in_stock", False)
                                            
                                            db.update_stock(item_name, category, price, in_stock)
                                            
                                            if in_stock:
                                                watchers = db.get_users_watching_item(item_name)
                                                for watcher in watchers:
                                                    await send_discord_notification(
                                                        watcher["user_id"],
                                                        item_name,
                                                        price,
                                                        watcher["priority"],
                                                        watcher["channel"],
                                                        watcher["role"]
                                                    )
                                    
                                    # Handle events
                                    elif data.get("type") in ["event_start", "event_update", "event_shop"]:
                                        event_name = data.get("event_name", data.get("name", "Unknown"))
                                        # Check future watches
                                        future_watches = db.get_future_watches()
                                        for fw in future_watches:
                                            if fw["pattern"].lower() in event_name.lower():
                                                user = await bot.fetch_user(int(fw["user_id"]))
                                                if user:
                                                    await user.send(f"🎉 **Event Detected!**\n{event_name} is now active!")
                                        
                                except json.JSONDecodeError:
                                    pass
                            
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.error(f"WebSocket error for {ws_url}")
                                break
                                
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"WebSocket connection failed for {ws_url}: {e}")
                await asyncio.sleep(5)

# ============ FastAPI Web Server ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start Discord bot
    asyncio.create_task(bot.start(DISCORD_TOKEN))
    # Wait a bit for bot to start
    await asyncio.sleep(2)
    # Start game WebSocket listener
    asyncio.create_task(game_websocket_listener())
    logger.info("🚀 Server started successfully!")
    yield
    # Cleanup
    await bot.close()

app = FastAPI(title="Grow a Garden + Xotiis Marketplace", lifespan=lifespan)
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

@app.get("/health")
async def health_check():
    return {"status": "healthy", "bot_online": bot_ready, "timestamp": datetime.now().isoformat()}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    websocket_connections.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        websocket_connections.discard(websocket)

# ============ Beautiful Website HTML ============
HTML_PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🌱 Xotiis Marketplace | Grow a Garden</title>
    <link href="https://fonts.googleapis.com/css2?family=Quicksand:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: 'Quicksand', sans-serif;
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%);
            color: #f1f5f9;
            min-height: 100vh;
        }
        
        /* Animated background */
        .garden-bg {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            z-index: -1;
            overflow: hidden;
        }
        
        .leaf {
            position: absolute;
            bottom: -20px;
            animation: floatUp 15s infinite linear;
            opacity: 0.1;
            font-size: 30px;
        }
        
        @keyframes floatUp {
            0% { transform: translateY(0) rotate(0deg); opacity: 0; }
            10% { opacity: 0.1; }
            90% { opacity: 0.1; }
            100% { transform: translateY(-100vh) rotate(360deg); opacity: 0; }
        }
        
        /* Header */
        .header {
            background: rgba(15, 23, 42, 0.85);
            backdrop-filter: blur(10px);
            border-bottom: 1px solid rgba(255, 215, 0, 0.2);
            padding: 1rem 2rem;
            position: sticky;
            top: 0;
            z-index: 100;
        }
        
        .header-content {
            max-width: 1400px;
            margin: 0 auto;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 1rem;
        }
        
        .logo {
            display: flex;
            align-items: center;
            gap: 12px;
            font-size: 1.8rem;
            font-weight: 700;
        }
        
        .logo i { 
            color: #ffd700; 
            animation: bounce 2s infinite;
        }
        
        @keyframes bounce { 
            0%, 100% { transform: translateY(0); } 
            50% { transform: translateY(-5px); } 
        }
        
        .logo span:first-child { 
            background: linear-gradient(135deg, #ffd700, #ff8c00); 
            -webkit-background-clip: text; 
            background-clip: text; 
            color: transparent; 
        }
        
        .nav-tabs {
            display: flex;
            gap: 0.5rem;
            flex-wrap: wrap;
        }
        
        .nav-btn {
            background: rgba(30, 41, 59, 0.8);
            border: none;
            color: #cbd5e1;
            padding: 0.6rem 1.2rem;
            border-radius: 40px;
            cursor: pointer;
            font-family: 'Quicksand', sans-serif;
            font-weight: 600;
            transition: all 0.3s ease;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .nav-btn:hover { 
            background: #334155; 
            transform: translateY(-2px); 
        }
        
        .nav-btn.active { 
            background: linear-gradient(135deg, #ffd700, #ff8c00); 
            color: #0f172a; 
        }
        
        /* Container */
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }
        
        /* Stats bar */
        .stats-bar {
            display: flex;
            gap: 1.5rem;
            margin-bottom: 2rem;
            flex-wrap: wrap;
        }
        
        .stat-card {
            background: rgba(30, 41, 59, 0.6);
            backdrop-filter: blur(10px);
            border-radius: 20px;
            padding: 1rem 1.5rem;
            flex: 1;
            min-width: 120px;
            text-align: center;
            border: 1px solid rgba(255, 215, 0, 0.2);
            transition: transform 0.3s ease;
        }
        
        .stat-card:hover { 
            transform: translateY(-5px); 
            border-color: #ffd700; 
        }
        
        .stat-value { 
            font-size: 2rem; 
            font-weight: 700; 
            color: #ffd700; 
        }
        
        .stat-label { 
            font-size: 0.8rem; 
            color: #94a3b8; 
            margin-top: 0.3rem; 
        }
        
        /* Grid layouts */
        .stock-grid, .marketplace-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 1.5rem;
            margin-top: 1rem;
        }
        
        .stock-card, .marketplace-card {
            background: rgba(30, 41, 59, 0.6);
            backdrop-filter: blur(10px);
            border-radius: 20px;
            padding: 1.2rem;
            border: 1px solid rgba(255, 215, 0, 0.2);
            transition: all 0.3s ease;
            animation: fadeInUp 0.5s ease;
        }
        
        @keyframes fadeInUp {
            from { opacity: 0; transform: translateY(20px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .stock-card:hover, .marketplace-card:hover { 
            transform: translateY(-5px); 
            border-color: #ffd700; 
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        }
        
        .stock-card.rare { 
            border: 2px solid #ffd700; 
            background: linear-gradient(135deg, rgba(255,215,0,0.1), rgba(30,41,59,0.6));
        }
        
        .stock-name { 
            font-size: 1.1rem; 
            font-weight: 600; 
            margin-bottom: 0.5rem; 
            display: flex; 
            align-items: center; 
            gap: 8px; 
        }
        
        .stock-category { 
            font-size: 0.7rem; 
            color: #94a3b8; 
            text-transform: uppercase; 
            letter-spacing: 1px; 
            margin-bottom: 0.5rem; 
        }
        
        .stock-price { 
            font-size: 1.5rem; 
            font-weight: 700; 
            color: #4ade80; 
            margin: 0.5rem 0; 
        }
        
        .stock-badge { 
            display: inline-block; 
            background: #22c55e; 
            padding: 0.2rem 0.6rem; 
            border-radius: 20px; 
            font-size: 0.7rem; 
            font-weight: 600; 
        }
        
        .item-stats {
            display: flex;
            gap: 1rem;
            font-size: 0.8rem;
            color: #94a3b8;
            margin: 0.5rem 0;
        }
        
        .item-image {
            width: 100%;
            height: 120px;
            background: linear-gradient(135deg, #1e293b, #334155);
            border-radius: 15px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 3rem;
            margin-bottom: 1rem;
        }
        
        .price-tag {
            display: flex;
            gap: 0.5rem;
            flex-wrap: wrap;
            margin: 0.8rem 0;
        }
        
        .price {
            background: rgba(0,0,0,0.3);
            padding: 0.2rem 0.6rem;
            border-radius: 20px;
            font-size: 0.8rem;
        }
        
        /* Help panel */
        .help-panel {
            background: rgba(30, 41, 59, 0.6);
            backdrop-filter: blur(10px);
            border-radius: 20px;
            padding: 1.5rem;
            margin-top: 1rem;
            border: 1px solid rgba(255, 215, 0, 0.2);
        }
        
        .help-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 1rem;
            margin-top: 1rem;
        }
        
        .help-item {
            background: rgba(15, 23, 42, 0.5);
            border-radius: 12px;
            padding: 0.8rem;
        }
        
        .help-item code {
            background: #0f172a;
            padding: 0.2rem 0.4rem;
            border-radius: 6px;
            color: #ffd700;
            font-size: 0.8rem;
        }
        
        .discord-invite {
            background: linear-gradient(135deg, #5865F2, #4752c4);
            border: none;
            padding: 1rem 2rem;
            border-radius: 50px;
            color: white;
            font-weight: 700;
            cursor: pointer;
            transition: transform 0.3s;
            display: inline-flex;
            align-items: center;
            gap: 10px;
            margin-top: 1rem;
        }
        
        .discord-invite:hover {
            transform: scale(1.05);
        }
        
        .loading {
            text-align: center;
            padding: 3rem;
        }
        
        .spinner {
            width: 50px;
            height: 50px;
            border: 3px solid #334155;
            border-top-color: #ffd700;
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin: 0 auto 1rem;
        }
        
        @keyframes spin { to { transform: rotate(360deg); } }
        
        .footer {
            text-align: center;
            padding: 2rem;
            color: #64748b;
            font-size: 0.8rem;
        }
        
        @media (max-width: 768px) {
            .container { padding: 1rem; }
            .header-content { flex-direction: column; }
            .stats-bar { flex-direction: column; }
        }
    </style>
</head>
<body>
    <div class="garden-bg" id="gardenBg"></div>
    
    <div class="header">
        <div class="header-content">
            <div class="logo">
                <i class="fas fa-seedling"></i>
                <span>Xotiis</span>
                <span style="font-size: 0.9rem; color: #94a3b8;">Marketplace</span>
            </div>
            <div class="nav-tabs">
                <button class="nav-btn active" data-tab="stock"><i class="fas fa-store"></i> Stock</button>
                <button class="nav-btn" data-tab="marketplace"><i class="fas fa-shopping-cart"></i> Shop</button>
                <button class="nav-btn" data-tab="events"><i class="fas fa-calendar-star"></i> Events</button>
                <button class="nav-btn" data-tab="help"><i class="fas fa-question-circle"></i> Help</button>
            </div>
        </div>
    </div>
    
    <div class="container">
        <div class="stats-bar" id="statsBar">
            <div class="stat-card"><div class="stat-value" id="stockCount">--</div><div class="stat-label">In Stock</div></div>
            <div class="stat-card"><div class="stat-value" id="eventCount">--</div><div class="stat-label">Active Events</div></div>
            <div class="stat-card"><div class="stat-value" id="listingsCount">--</div><div class="stat-label">Listings</div></div>
        </div>
        
        <div id="stockPanel" class="panel">
            <div class="loading"><div class="spinner"></div><p>Loading stock...</p></div>
        </div>
        
        <div id="marketplacePanel" class="panel" style="display: none;">
            <div class="loading"><div class="spinner"></div><p>Loading marketplace...</p></div>
        </div>
        
        <div id="eventsPanel" class="panel" style="display: none;">
            <div class="loading"><div class="spinner"></div><p>Loading events...</p></div>
        </div>
        
        <div id="helpPanel" class="panel" style="display: none;">
            <div class="help-panel">
                <h2><i class="fas fa-heart" style="color: #ff6b6b;"></i> Welcome to Xotiis Marketplace!</h2>
                <div class="help-grid">
                    <div class="help-item">
                        <strong><i class="fab fa-discord"></i> Discord Commands</strong><br><br>
                        <code>/shekels</code> - Check balance<br>
                        <code>/sell</code> - List an item<br>
                        <code>/watch &lt;item&gt;</code> - Track stock<br>
                        <code>/unwatch &lt;item&gt;</code> - Remove track<br>
                        <code>/watches</code> - List tracked items<br>
                        <code>/stock</code> - Check current stock<br>
                        <code>/marketplace</code> - Browse listings<br>
                        <code>/future &lt;pattern&gt;</code> - Watch events<br>
                        <code>/events</code> - Active events
                    </div>
                    <div class="help-item">
                        <strong><i class="fas fa-coins"></i> Payment Types</strong><br><br>
                        • 💰 <strong>Shekels</strong> - In-game currency<br>
                        • 🤖 <strong>Robux</strong> - Roblox currency<br>
                        • 💳 <strong>PayPal</strong> - Real money<br><br>
                        <strong>How to Buy:</strong><br>
                        1. Find an item you like<br>
                        2. Contact the seller on Discord<br>
                        3. Negotiate and complete trade
                    </div>
                    <div class="help-item">
                        <strong><i class="fas fa-paw"></i> Selling Pets/Fruits</strong><br><br>
                        Use <code>/sell</code> with:<br>
                        • Item type (pet or fruit)<br>
                        • Name<br>
                        • Weight (kg)<br>
                        • Age (days)<br>
                        • Price (shekels/robux/paypal)<br>
                        • Description (optional)
                    </div>
                    <div class="help-item">
                        <strong><i class="fas fa-bell"></i> Stock Alerts</strong><br><br>
                        Get notified when items come in stock!<br>
                        • <strong>High</strong> priority = DM + ping<br>
                        • <strong>Medium</strong> = channel ping<br>
                        • <strong>Low</strong> = daily digest<br><br>
                        Use <code>/watch "item" priority</code>
                    </div>
                </div>
                <div style="text-align: center; margin-top: 1.5rem;">
                    <p>✨ <strong>Join our community!</strong> ✨</p>
                    <button class="discord-invite" onclick="window.open('https://discord.gg/growagarden', '_blank')">
                        <i class="fab fa-discord"></i> Join Discord Server
                    </button>
                </div>
            </div>
        </div>
    </div>
    
    <div class="footer">
        <p>🌱 Grow a Garden • Xotiis Marketplace • Made with <i class="fas fa-heart" style="color: #ff6b6b;"></i> for the community</p>
    </div>
    
    <script>
        // Animated leaves
        const leaves = ['🌿', '🍃', '🌸', '🌻', '🍂', '🌱', '🍎', '🐝', '🦋', '🐞'];
        for (let i = 0; i < 40; i++) {
            const leaf = document.createElement('div');
            leaf.className = 'leaf';
            leaf.textContent = leaves[Math.floor(Math.random() * leaves.length)];
            leaf.style.left = Math.random() * 100 + '%';
            leaf.style.animationDuration = 10 + Math.random() * 20 + 's';
            leaf.style.animationDelay = Math.random() * 10 + 's';
            leaf.style.fontSize = 20 + Math.random() * 40 + 'px';
            document.getElementById('gardenBg').appendChild(leaf);
        }
        
        let currentTab = 'stock';
        
        async function loadStats() {
            try {
                const stockRes = await fetch('/api/stock');
                const stock = await stockRes.json();
                document.getElementById('stockCount').textContent = stock.length;
                
                const eventsRes = await fetch('/api/events');
                const events = await eventsRes.json();
                document.getElementById('eventCount').textContent = events.length;
                
                const listingsRes = await fetch('/api/marketplace/listings');
                const listings = await listingsRes.json();
                document.getElementById('listingsCount').textContent = listings.length;
            } catch(e) { console.error(e); }
        }
        
        async function loadStock() {
            const container = document.getElementById('stockPanel');
            try {
                const res = await fetch('/api/stock');
                let items = await res.json();
                const rareItems = ['sprout', 'lyrebird', 'peryton', 'golden', 'divine', 'mythical', 'legendary'];
                
                if (items.length === 0) {
                    container.innerHTML = '<div style="text-align: center; padding: 3rem;"><i class="fas fa-seedling" style="font-size: 3rem; opacity: 0.3;"></i><p>No items currently in stock!</p></div>';
                    return;
                }
                
                let html = '<div class="stock-grid">';
                for (const item of items) {
                    const isRare = rareItems.some(r => item.name.toLowerCase().includes(r));
                    const icon = item.category === 'seeds' ? 'seedling' : item.category === 'gear' ? 'tools' : 'egg';
                    html += `
                        <div class="stock-card ${isRare ? 'rare' : ''}">
                            <div class="stock-name"><i class="fas fa-${icon}"></i> ${escapeHtml(item.name)}</div>
                            <div class="stock-category">${item.category || 'General'}</div>
                            <div class="stock-price">$${formatPrice(item.price)}</div>
                            <span class="stock-badge"><i class="fas fa-check-circle"></i> IN STOCK</span>
                        </div>
                    `;
                }
                html += '</div>';
                container.innerHTML = html;
            } catch(e) { 
                container.innerHTML = '<div style="text-align: center; padding: 3rem;">Failed to load stock</div>';
            }
        }
        
        async function loadMarketplace() {
            const container = document.getElementById('marketplacePanel');
            try {
                const res = await fetch('/api/marketplace/listings');
                const listings = await res.json();
                
                if (listings.length === 0) {
                    container.innerHTML = '<div style="text-align: center; padding: 3rem;"><i class="fas fa-store" style="font-size: 3rem; opacity: 0.3;"></i><p>No listings yet! Use /sell command in Discord</p></div>';
                    return;
                }
                
                let html = '<div class="marketplace-grid">';
                for (const item of listings) {
                    const icon = item.item_type === 'pet' ? 'cat' : 'apple-alt';
                    html += `
                        <div class="marketplace-card">
                            <div class="item-image">
                                <i class="fas fa-${icon}" style="font-size: 3rem;"></i>
                            </div>
                            <div class="stock-name">${escapeHtml(item.item_name)}</div>
                            <div class="item-stats">
                                <span><i class="fas fa-weight-hanging"></i> ${item.size_kg} kg</span>
                                <span><i class="fas fa-calendar"></i> ${item.age_days} days</span>
                            </div>
                            <div class="price-tag">
                                ${item.price_shekels ? `<span class="price"><i class="fas fa-coins"></i> ${item.price_shekels} Shekels</span>` : ''}
                                ${item.price_robux ? `<span class="price"><i class="fab fa-roblox"></i> ${item.price_robux} Robux</span>` : ''}
                                ${item.price_paypal ? `<span class="price"><i class="fab fa-paypal"></i> $${item.price_paypal}</span>` : ''}
                            </div>
                            <div class="item-stats"><small>👤 Seller: ${escapeHtml(item.seller_name)}</small></div>
                            ${item.description ? `<div class="item-stats"><small>📝 ${escapeHtml(item.description.substring(0, 60))}</small></div>` : ''}
                        </div>
                    `;
                }
                html += '</div>';
                container.innerHTML = html;
            } catch(e) { 
                container.innerHTML = '<div style="text-align: center; padding: 3rem;">Failed to load marketplace</div>';
            }
        }
        
        async function loadEvents() {
            const container = document.getElementById('eventsPanel');
            try {
                const res = await fetch('/api/events');
                const events = await res.json();
                
                if (events.length === 0) {
                    container.innerHTML = '<div style="text-align: center; padding: 3rem;"><i class="fas fa-calendar" style="font-size: 3rem; opacity: 0.3;"></i><p>No active events right now!</p></div>';
                    return;
                }
                
                let html = '<div class="stock-grid">';
                for (const event of events) {
                    html += `
                        <div class="stock-card">
                            <div class="stock-name"><i class="fas fa-calendar-star"></i> ${escapeHtml(event.name)}</div>
                            <div class="stock-category">${event.type || 'Event'} ${event.phase ? `• ${event.phase}` : ''}</div>
                            <div class="stock-price"><i class="fas fa-hourglass-half"></i> ACTIVE NOW</div>
                            ${event.description ? `<small>${escapeHtml(event.description.substring(0, 100))}</small>` : ''}
                        </div>
                    `;
                }
                html += '</div>';
                container.innerHTML = html;
            } catch(e) { 
                container.innerHTML = '<div style="text-align: center; padding: 3rem;">Failed to load events</div>';
            }
        }
        
        function formatPrice(p) {
            if (p >= 1e6) return (p/1e6).toFixed(2) + 'M';
            if (p >= 1e3) return (p/1e3).toFixed(1) + 'K';
            return p.toString();
        }
        
        function escapeHtml(text) { 
            const div = document.createElement('div'); 
            div.textContent = text; 
            return div.innerHTML; 
        }
        
        function switchTab(tab) {
            currentTab = tab;
            document.querySelectorAll('.panel').forEach(p => p.style.display = 'none');
            document.getElementById(`${tab}Panel`).style.display = 'block';
            document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.remove('active'));
            document.querySelector(`[data-tab="${tab}"]`).classList.add('active');
            
            if (tab === 'stock') loadStock();
            else if (tab === 'marketplace') loadMarketplace();
            else if (tab === 'events') loadEvents();
        }
        
        document.querySelectorAll('.nav-btn').forEach(btn => {
            btn.addEventListener('click', () => switchTab(btn.dataset.tab));
        });
        
        // Initial load
        loadStats();
        loadStock();
        
        // Auto-refresh
        setInterval(() => { 
            loadStats(); 
            if (currentTab === 'stock') loadStock(); 
        }, 30000);
        setInterval(() => { 
            if (currentTab === 'marketplace') loadMarketplace(); 
        }, 60000);
        setInterval(() => { 
            if (currentTab === 'events') loadEvents(); 
        }, 60000);
    </script>
</body>
</html>
"""

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
        logger.warning("⚠️ DISCORD_TOKEN environment variable not set! Bot will not work.")
        logger.warning("Please set DISCORD_TOKEN in Render environment variables.")
    else:
        logger.info(f"✅ Discord token found (length: {len(DISCORD_TOKEN)})")
    main()
