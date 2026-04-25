import asyncio
import json
import logging
import os
import sqlite3
import time
import re
import random
import hashlib
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
from collections import defaultdict
from dataclasses import dataclass, asdict
from enum import Enum

import discord
from discord import app_commands
from discord.ext import tasks
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
import aiohttp
import aiofiles

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============ Configuration ============
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")
PUBLIC_URL = os.environ.get("PUBLIC_URL")

# Primary WebSocket endpoints (tries multiple if one fails)
WEBSOCKET_ENDPOINTS = [
    "wss://api.jstudio.ai/grow-a-garden/ws",
    "wss://ws.growagarden.com/socket.io/?EIO=4&transport=websocket",
    "wss://game.growagarden.com/ws"
]

# Fallback: Use HTTP polling if WebSocket fails
USE_FALLBACK_POLLING = True
FALLBACK_API_URLS = [
    "https://api.jstudio.ai/grow-a-garden/stock",
    "https://growagarden.com/api/shop",
    "https://game.growagarden.com/api/stock"
]

DB_PATH = os.environ.get("DB_PATH", "/tmp/grow_a_garden.db")

# ============ Data Models ============
class ItemCategory(str, Enum):
    SEEDS = "seeds"
    GEAR = "gear"
    EGGS = "eggs"
    EVENT = "event"
    SEASONAL = "seasonal"
    PET = "pet"
    UNKNOWN = "unknown"

@dataclass
class StockItem:
    name: str
    category: str
    price: float
    in_stock: bool
    quantity: Optional[int] = None
    rarity: str = "common"
    last_seen: datetime = None
    
    def __post_init__(self):
        if self.last_seen is None:
            self.last_seen = datetime.now()

@dataclass
class GameEvent:
    name: str
    event_type: str
    active: bool
    phase: Optional[str] = None
    description: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    rewards: List[str] = None
    requirements: Dict[str, Any] = None

# ============ Advanced Database Layer ============
class AdvancedDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_tables()
        self._migrate_if_needed()

    def _connect(self):
        return sqlite3.connect(self.db_path, timeout=30)

    def _init_tables(self):
        with self._connect() as conn:
            cursor = conn.cursor()
            
            # Users table with more fields
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    discord_id TEXT PRIMARY KEY,
                    notification_channel TEXT,
                    notification_role TEXT,
                    timezone TEXT DEFAULT 'UTC',
                    daily_digest_enabled INTEGER DEFAULT 1,
                    created_at TEXT,
                    last_active TEXT
                )
            """)
            
            # Watchlist with more options
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS watchlist (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT,
                    item_name TEXT,
                    category TEXT,
                    priority TEXT DEFAULT 'medium',
                    wildcard_pattern TEXT,
                    min_price REAL,
                    max_price REAL,
                    enabled INTEGER DEFAULT 1,
                    created_at TEXT,
                    last_notified TEXT,
                    notify_count INTEGER DEFAULT 0,
                    UNIQUE(user_id, item_name)
                )
            """)
            
            # Stock cache with history
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_cache (
                    item_name TEXT PRIMARY KEY,
                    category TEXT,
                    price REAL,
                    in_stock INTEGER,
                    quantity INTEGER,
                    rarity TEXT,
                    last_seen TEXT,
                    first_seen TEXT,
                    times_seen INTEGER DEFAULT 1
                )
            """)
            
            # Stock price history
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS price_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_name TEXT,
                    price REAL,
                    timestamp TEXT,
                    UNIQUE(item_name, timestamp)
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
                    start_date TEXT,
                    end_date TEXT,
                    data_json TEXT,
                    detected_at TEXT,
                    updated_at TEXT
                )
            """)
            
            # Notification log with more details
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS notification_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT,
                    item_name TEXT,
                    price REAL,
                    notification_type TEXT,
                    sent_at TEXT,
                    delivered INTEGER DEFAULT 1
                )
            """)
            
            # Future watches
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS future_watches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT,
                    pattern TEXT,
                    event_type TEXT,
                    created_at TEXT,
                    last_checked TEXT,
                    UNIQUE(user_id, pattern)
                )
            """)
            
            # System metrics
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS system_metrics (
                    metric_name TEXT PRIMARY KEY,
                    metric_value TEXT,
                    updated_at TEXT
                )
            """)
            
            # Error logs
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS error_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    error_type TEXT,
                    message TEXT,
                    stack_trace TEXT,
                    timestamp TEXT
                )
            """)
            
            conn.commit()

    def _migrate_if_needed(self):
        """Handle database schema migrations"""
        with self._connect() as conn:
            cursor = conn.cursor()
            # Check for missing columns and add them
            cursor.execute("PRAGMA table_info(watchlist)")
            columns = [col[1] for col in cursor.fetchall()]
            
            if "min_price" not in columns:
                cursor.execute("ALTER TABLE watchlist ADD COLUMN min_price REAL")
            if "max_price" not in columns:
                cursor.execute("ALTER TABLE watchlist ADD COLUMN max_price REAL")
            if "last_notified" not in columns:
                cursor.execute("ALTER TABLE watchlist ADD COLUMN last_notified TEXT")
            if "notify_count" not in columns:
                cursor.execute("ALTER TABLE watchlist ADD COLUMN notify_count INTEGER DEFAULT 0")
            
            conn.commit()

    # ============ User Methods ============
    def get_user_config(self, discord_id: str) -> Dict:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT notification_channel, notification_role, timezone, daily_digest_enabled FROM users WHERE discord_id = ?", (discord_id,))
            row = cursor.fetchone()
            if row:
                return {"channel": row[0], "role": row[1], "timezone": row[2], "daily_digest_enabled": bool(row[3])}
            return {"channel": None, "role": None, "timezone": "UTC", "daily_digest_enabled": True}

    def set_user_config(self, discord_id: str, channel: str = None, role: str = None, timezone: str = None, daily_digest: bool = None):
        with self._connect() as conn:
            cursor = conn.cursor()
            existing = self.get_user_config(discord_id)
            now = datetime.now().isoformat()
            
            if existing["channel"] is None and existing["role"] is None:
                cursor.execute("""
                    INSERT INTO users (discord_id, notification_channel, notification_role, timezone, daily_digest_enabled, created_at, last_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (discord_id, channel, role, timezone or "UTC", 1 if daily_digest is None else daily_digest, now, now))
            else:
                updates = []
                params = []
                if channel is not None:
                    updates.append("notification_channel = ?")
                    params.append(channel)
                if role is not None:
                    updates.append("notification_role = ?")
                    params.append(role)
                if timezone is not None:
                    updates.append("timezone = ?")
                    params.append(timezone)
                if daily_digest is not None:
                    updates.append("daily_digest_enabled = ?")
                    params.append(1 if daily_digest else 0)
                updates.append("last_active = ?")
                params.append(now)
                params.append(discord_id)
                
                cursor.execute(f"UPDATE users SET {', '.join(updates)} WHERE discord_id = ?", params)
            conn.commit()

    # ============ Watchlist Methods ============
    def add_watch(self, user_id: str, item_name: str, category: str = None, priority: str = "medium", 
                  wildcard: str = None, min_price: float = None, max_price: float = None):
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO watchlist (user_id, item_name, category, priority, wildcard_pattern, min_price, max_price, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (user_id, item_name, category, priority, wildcard, min_price, max_price, datetime.now().isoformat()))
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def remove_watch(self, user_id: str, item_name: str):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM watchlist WHERE user_id = ? AND item_name = ?", (user_id, item_name))
            conn.commit()
            return cursor.rowcount > 0

    def get_user_watchlist(self, user_id: str) -> List[Dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT item_name, category, priority, wildcard_pattern, min_price, max_price, enabled, notify_count 
                FROM watchlist WHERE user_id = ? AND enabled = 1
            """, (user_id,))
            rows = cursor.fetchall()
            return [{
                "item_name": r[0], "category": r[1], "priority": r[2], 
                "wildcard": r[3], "min_price": r[4], "max_price": r[5],
                "enabled": bool(r[6]), "notify_count": r[7]
            } for r in rows]

    def get_users_watching_item(self, item_name: str, price: float = None) -> List[Dict]:
        """Find all users who should be notified about this item"""
        with self._connect() as conn:
            cursor = conn.cursor()
            
            # Exact matches with price filters
            query = """
                SELECT w.user_id, w.priority, u.notification_channel, u.notification_role, w.min_price, w.max_price
                FROM watchlist w
                JOIN users u ON w.user_id = u.discord_id
                WHERE w.item_name = ? AND w.enabled = 1
            """
            params = [item_name]
            
            if price is not None:
                query += " AND (w.min_price IS NULL OR w.min_price <= ?) AND (w.max_price IS NULL OR w.max_price >= ?)"
                params.extend([price, price])
            
            cursor.execute(query, params)
            exact = cursor.fetchall()

            # Wildcard matches
            cursor.execute("SELECT user_id, wildcard_pattern, priority, min_price, max_price FROM watchlist WHERE wildcard_pattern IS NOT NULL AND enabled = 1")
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
                            # Check price constraints for wildcard
                            if price is not None:
                                if (row[3] is not None and price < row[3]) or (row[4] is not None and price > row[4]):
                                    continue
                            users[row[0]] = {"priority": row[2], "channel": urow[0], "role": urow[1]}

            return [{"user_id": u, "priority": d["priority"], "channel": d["channel"], "role": d["role"]} 
                    for u, d in users.items()]

    # ============ Stock Methods ============
    def update_stock(self, item: StockItem):
        with self._connect() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            
            # Check if exists
            cursor.execute("SELECT first_seen, times_seen FROM stock_cache WHERE item_name = ?", (item.name,))
            existing = cursor.fetchone()
            
            if existing:
                first_seen = existing[0]
                times_seen = existing[1] + 1
            else:
                first_seen = now
                times_seen = 1
            
            cursor.execute("""
                INSERT INTO stock_cache (item_name, category, price, in_stock, quantity, rarity, last_seen, first_seen, times_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(item_name) DO UPDATE SET
                    category = excluded.category,
                    price = excluded.price,
                    in_stock = excluded.in_stock,
                    quantity = excluded.quantity,
                    rarity = excluded.rarity,
                    last_seen = excluded.last_seen,
                    times_seen = excluded.times_seen
            """, (item.name, item.category, item.price, 1 if item.in_stock else 0, 
                  item.quantity, item.rarity, now, first_seen, times_seen))
            
            # Record price history
            if item.price > 0:
                cursor.execute("""
                    INSERT OR IGNORE INTO price_history (item_name, price, timestamp)
                    VALUES (?, ?, ?)
                """, (item.name, item.price, now))
            
            conn.commit()

    def get_all_stock(self, include_out_of_stock: bool = False) -> List[Dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            if include_out_of_stock:
                cursor.execute("SELECT item_name, category, price, in_stock, quantity, rarity, last_seen, times_seen FROM stock_cache ORDER BY price DESC")
            else:
                cursor.execute("SELECT item_name, category, price, in_stock, quantity, rarity, last_seen, times_seen FROM stock_cache WHERE in_stock = 1 ORDER BY price DESC")
            rows = cursor.fetchall()
            return [{
                "name": r[0], "category": r[1], "price": r[2], 
                "in_stock": bool(r[3]), "quantity": r[4], "rarity": r[5],
                "last_seen": r[6], "times_seen": r[7]
            } for r in rows]

    def get_price_history(self, item_name: str, days: int = 7) -> List[Dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            cursor.execute("""
                SELECT price, timestamp FROM price_history 
                WHERE item_name = ? AND timestamp > ?
                ORDER BY timestamp DESC
            """, (item_name, cutoff))
            return [{"price": r[0], "timestamp": r[1]} for r in cursor.fetchall()]

    # ============ Event Methods ============
    def update_event(self, event: GameEvent):
        with self._connect() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            cursor.execute("""
                INSERT INTO events (event_name, event_type, active, phase, description, start_date, end_date, data_json, detected_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_name) DO UPDATE SET
                    event_type = excluded.event_type,
                    active = excluded.active,
                    phase = excluded.phase,
                    description = excluded.description,
                    start_date = excluded.start_date,
                    end_date = excluded.end_date,
                    data_json = excluded.data_json,
                    updated_at = excluded.updated_at
            """, (event.name, event.event_type, 1 if event.active else 0, event.phase, 
                  event.description, event.start_date.isoformat() if event.start_date else None,
                  event.end_date.isoformat() if event.end_date else None,
                  json.dumps(asdict(event) if hasattr(event, '__dict__') else {}), now, now))
            conn.commit()

    def get_active_events(self) -> List[Dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT event_name, event_type, phase, description, start_date, end_date, data_json 
                FROM events WHERE active = 1
                ORDER BY detected_at DESC
            """)
            rows = cursor.fetchall()
            return [{
                "name": r[0], "type": r[1], "phase": r[2], "description": r[3],
                "start_date": r[4], "end_date": r[5], "data": json.loads(r[6]) if r[6] else {}
            } for r in rows]

    def get_event_details(self, event_name: str) -> Dict:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT event_name, event_type, phase, description, start_date, end_date, data_json, detected_at
                FROM events WHERE event_name = ?
            """, (event_name,))
            row = cursor.fetchone()
            if row:
                return {
                    "name": row[0], "type": row[1], "phase": row[2], "description": row[3],
                    "start_date": row[4], "end_date": row[5], "data": json.loads(row[6]) if row[6] else {},
                    "detected_at": row[7]
                }
            return None

    # ============ Future Watches ============
    def add_future_watch(self, user_id: str, pattern: str, event_type: str = None):
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO future_watches (user_id, pattern, event_type, created_at)
                    VALUES (?, ?, ?, ?)
                """, (user_id, pattern, event_type, datetime.now().isoformat()))
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def get_future_watches(self) -> List[Dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id, pattern, event_type FROM future_watches")
            return [{"user_id": r[0], "pattern": r[1], "event_type": r[2]} for r in cursor.fetchall()]

    # ============ Notification Methods ============
    def log_notification(self, user_id: str, item_name: str, price: float, notification_type: str = "auto"):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO notification_log (user_id, item_name, price, notification_type, sent_at, delivered)
                VALUES (?, ?, ?, ?, ?, 1)
            """, (user_id, item_name, price, notification_type, datetime.now().isoformat()))
            
            # Update notify count on watchlist
            cursor.execute("""
                UPDATE watchlist 
                SET notify_count = notify_count + 1, last_notified = ?
                WHERE user_id = ? AND item_name = ?
            """, (datetime.now().isoformat(), user_id, item_name))
            conn.commit()

    def should_notify(self, user_id: str, item_name: str, cooldown_minutes: int = 60) -> bool:
        """Check if user should be notified (rate limiting)"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cutoff = (datetime.now() - timedelta(minutes=cooldown_minutes)).isoformat()
            cursor.execute("""
                SELECT COUNT(*) FROM notification_log 
                WHERE user_id = ? AND item_name = ? AND sent_at > ?
            """, (user_id, item_name, cutoff))
            count = cursor.fetchone()[0]
            return count == 0

    # ============ Metrics ============
    def update_metric(self, metric_name: str, metric_value: str):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO system_metrics (metric_name, metric_value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(metric_name) DO UPDATE SET
                    metric_value = excluded.metric_value,
                    updated_at = excluded.updated_at
            """, (metric_name, metric_value, datetime.now().isoformat()))
            conn.commit()

    def get_metric(self, metric_name: str) -> str:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT metric_value FROM system_metrics WHERE metric_name = ?", (metric_name,))
            row = cursor.fetchone()
            return row[0] if row else None

    def log_error(self, error_type: str, message: str, stack_trace: str = None):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO error_logs (error_type, message, stack_trace, timestamp)
                VALUES (?, ?, ?, ?)
            """, (error_type, message, stack_trace, datetime.now().isoformat()))
            conn.commit()

db = AdvancedDatabase(DB_PATH)

# ============ Advanced WebSocket Listener ============
class AdvancedGameListener:
    def __init__(self):
        self.websocket = None
        self.session = None
        self.running = False
        self.reconnect_delay = 5
        self.max_reconnect_delay = 60
        self.current_endpoint_index = 0
        self.stock_cache = {}
        self.event_callbacks = []
        self.last_heartbeat = None
        self.message_count = 0
        self.start_time = datetime.now()
        
    def on_event(self, callback):
        self.event_callbacks.append(callback)
    
    async def notify_callbacks(self, event_type: str, data: dict):
        for callback in self.event_callbacks:
            try:
                await callback(event_type, data)
            except Exception as e:
                logger.error(f"Callback error: {e}")
    
    async def connect(self):
        """Connect to WebSocket with fallback endpoints"""
        if self.session is None:
            self.session = aiohttp.ClientSession()
        
        for attempt in range(len(WEBSOCKET_ENDPOINTS)):
            endpoint = WEBSOCKET_ENDPOINTS[self.current_endpoint_index]
            try:
                logger.info(f"Connecting to WebSocket: {endpoint}")
                self.websocket = await self.session.ws_connect(
                    endpoint,
                    timeout=30,
                    heartbeat=55,
                    autoclose=True,
                    autoping=True
                )
                logger.info(f"Connected to {endpoint}")
                db.update_metric("active_websocket", endpoint)
                db.update_metric("last_connection", datetime.now().isoformat())
                self.reconnect_delay = 5  # Reset delay on successful connect
                return True
            except Exception as e:
                logger.error(f"Failed to connect to {endpoint}: {e}")
                self.current_endpoint_index = (self.current_endpoint_index + 1) % len(WEBSOCKET_ENDPOINTS)
                await asyncio.sleep(2)
        
        return False
    
    async def listen(self):
        """Main listening loop"""
        self.running = True
        
        while self.running:
            try:
                if not self.websocket or self.websocket.closed:
                    if not await self.connect():
                        logger.warning(f"All endpoints failed, retrying in {self.reconnect_delay}s...")
                        await asyncio.sleep(self.reconnect_delay)
                        self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
                        continue
                
                async for msg in self.websocket:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self.process_message(msg.data)
                        self.message_count += 1
                    elif msg.type == aiohttp.WSMsgType.PING:
                        await self.websocket.pong()
                    elif msg.type == aiohttp.WSMsgType.CLOSE:
                        logger.warning("WebSocket closed by server")
                        break
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error(f"WebSocket error: {self.websocket.exception()}")
                        break
                        
            except Exception as e:
                logger.error(f"Listen loop error: {e}")
                db.log_error("websocket", str(e))
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
    
    async def process_message(self, raw_message: str):
        """Process incoming WebSocket messages with intelligent parsing"""
        try:
            # Try to parse as JSON
            data = json.loads(raw_message)
            msg_type = data.get("type", "unknown")
            
            # Update metrics
            db.update_metric(f"last_msg_type_{msg_type}", datetime.now().isoformat())
            
            # Handle different message types
            if msg_type == "stock_refresh" or msg_type == "shop_update":
                await self.process_stock_update(data)
            elif msg_type == "event_start" or msg_type == "event_update" or msg_type == "event_shop":
                await self.process_event_update(data)
            elif msg_type == "weather_update":
                await self.process_weather_update(data)
            elif msg_type == "pong" or msg_type == "heartbeat":
                self.last_heartbeat = datetime.now()
            else:
                # Unknown type - try to infer from content
                await self.process_unknown_message(data)
                
            await self.notify_callbacks("message", {"type": msg_type, "data": data})
            
        except json.JSONDecodeError:
            # Not JSON - try to parse as custom protocol
            logger.warning(f"Non-JSON message: {raw_message[:200]}")
            db.log_error("non_json_message", raw_message[:500])
            
    async def process_stock_update(self, data: dict):
        """Process stock refresh messages"""
        items = data.get("items", data.get("stock", []))
        
        for item_data in items:
            if isinstance(item_data, dict):
                item = StockItem(
                    name=item_data.get("name", "Unknown"),
                    category=self.detect_category(item_data),
                    price=float(item_data.get("price", item_data.get("cost", 0))),
                    in_stock=item_data.get("in_stock", item_data.get("available", True)),
                    quantity=item_data.get("quantity", item_data.get("stock", None)),
                    rarity=item_data.get("rarity", "common")
                )
                
                db.update_stock(item)
                
                # Check for watchers
                if item.in_stock:
                    await self.check_and_notify_watchers(item)
                
                await self.notify_callbacks("stock_update", asdict(item))
    
    async def process_event_update(self, data: dict):
        """Process event updates"""
        event = GameEvent(
            name=data.get("event_name", data.get("name", "Unknown Event")),
            event_type=data.get("event_type", data.get("type", "unknown")),
            active=data.get("active", data.get("is_active", True)),
            phase=self.extract_phase(data),
            description=data.get("description", data.get("details", "")),
            rewards=data.get("rewards", []),
            requirements=data.get("requirements", {})
        )
        
        # Try to parse dates
        if data.get("start_date"):
            try:
                event.start_date = datetime.fromisoformat(data["start_date"].replace('Z', '+00:00'))
            except:
                pass
        if data.get("end_date"):
            try:
                event.end_date = datetime.fromisoformat(data["end_date"].replace('Z', '+00:00'))
            except:
                pass
        
        db.update_event(event)
        
        # Check future watches
        future_watches = db.get_future_watches()
        for fw in future_watches:
            pattern = fw["pattern"].lower()
            if pattern in event.name.lower() or (fw["event_type"] and fw["event_type"] == event.event_type):
                await self.notify_future_watch(fw["user_id"], event)
        
        await self.notify_callbacks("event_update", asdict(event))
    
    async def process_weather_update(self, data: dict):
        """Process weather updates"""
        weather_type = data.get("weather", data.get("type", "unknown"))
        await self.notify_callbacks("weather", {"weather": weather_type, "data": data})
    
    async def process_unknown_message(self, data: dict):
        """Intelligently handle unknown message types"""
        # Try to detect if it contains stock data
        if "items" in data or "stock" in data:
            await self.process_stock_update(data)
        elif "event" in data or "event_name" in data:
            await self.process_event_update(data)
        else:
            logger.info(f"Unknown message structure: {list(data.keys())}")
            db.log_error("unknown_structure", json.dumps(data)[:500])
    
    def detect_category(self, item_data: dict) -> str:
        """Intelligently detect item category"""
        name = item_data.get("name", "").lower()
        category = item_data.get("category", "").lower()
        
        if category:
            return category
        
        # Heuristic detection
        if any(word in name for word in ["seed", "sprout", "plant", "crop"]):
            return "seeds"
        elif any(word in name for word in ["sprinkler", "hoe", "watering", "tool", "gear"]):
            return "gear"
        elif "egg" in name:
            return "eggs"
        elif any(word in name for word in ["event", "special", "limited"]):
            return "event"
        elif any(word in name for word in ["easter", "halloween", "christmas", "summer", "winter", "fall", "spring"]):
            return "seasonal"
        elif any(word in name for word in ["pet", "bird", "animal", "fox", "rabbit"]):
            return "pet"
        
        return "unknown"
    
    def extract_phase(self, data: dict) -> str:
        """Extract event phase from data"""
        phase_keywords = ["phase", "part", "stage", "act", "chapter"]
        for keyword in phase_keywords:
            if keyword in data:
                return str(data[keyword])
        
        description = data.get("description", "").lower()
        if "part 2" in description or "phase 2" in description:
            return "Part 2"
        elif "part 3" in description or "phase 3" in description:
            return "Part 3"
        elif "finale" in description or "final" in description:
            return "Finale"
        
        return None
    
    async def check_and_notify_watchers(self, item: StockItem):
        """Check all watchers and send notifications"""
        watchers = db.get_users_watching_item(item.name.lower(), item.price)
        
        for watcher in watchers:
            # Rate limiting
            if not db.should_notify(watcher["user_id"], item.name, cooldown_minutes=30):
                continue
            
            await self.send_discord_notification(
                watcher["user_id"],
                item.name,
                item.price,
                watcher["priority"],
                watcher["channel"],
                watcher["role"]
            )
    
    async def send_discord_notification(self, user_id: str, item_name: str, price: float, 
                                        priority: str, channel_id: str, role_id: str = None):
        """Send Discord notification"""
        try:
            user = await bot.fetch_user(int(user_id))
            if not user:
                return
            
            emoji = "🔔" if priority == "medium" else "🚨" if priority == "high" else "📋"
            message = f"{emoji} **{item_name}** is now in stock for **${price:,.0f}**!"
            
            if priority == "high":
                # DM user
                try:
                    await user.send(f"🚨 **HIGH PRIORITY ALERT!**\n{message}")
                except:
                    pass
                
                # Also ping in channel
                if channel_id:
                    channel = bot.get_channel(int(channel_id))
                    if channel:
                        ping = f"<@{user_id}>" + (f" <@&{role_id}>" if role_id else "")
                        await channel.send(f"{ping}\n{message}")
                        
            elif priority == "medium":
                if channel_id:
                    channel = bot.get_channel(int(channel_id))
                    if channel:
                        ping = f"<@{user_id}>" + (f" <@&{role_id}>" if role_id else "")
                        await channel.send(f"{ping}\n{message}")
            
            # Log notification
            db.log_notification(user_id, item_name, price, priority)
            
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")
    
    async def notify_future_watch(self, user_id: str, event: GameEvent):
        """Notify user about future event match"""
        try:
            user = await bot.fetch_user(int(user_id))
            if user:
                message = f"🎉 **Event Alert!**\n{event.name} has been detected!\n\n{event.description or ''}"
                await user.send(message)
        except Exception as e:
            logger.error(f"Failed to send future watch notification: {e}")
    
    async def poll_fallback(self):
        """Fallback HTTP polling if WebSocket fails"""
        logger.info("Starting fallback HTTP polling...")
        
        while self.running and (not self.websocket or self.websocket.closed):
            for api_url in FALLBACK_API_URLS:
                try:
                    async with self.session.get(api_url, timeout=10) as response:
                        if response.status == 200:
                            data = await response.json()
                            await self.process_stock_update({"items": data.get("items", data)})
                except Exception as e:
                    logger.debug(f"Fallback poll failed for {api_url}: {e}")
            
            await asyncio.sleep(30)  # Poll every 30 seconds
    
    async def start(self):
        """Start the listener with both WebSocket and fallback"""
        asyncio.create_task(self.listen())
        if USE_FALLBACK_POLLING:
            asyncio.create_task(self.poll_fallback())
    
    def stop(self):
        self.running = False

# ============ Discord Bot ============
intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)

bot_owner_id = None
game_listener = None

@bot.event
async def on_ready():
    global bot_owner_id, game_listener
    logger.info(f"Bot logged in as {bot.user}")
    await tree.sync()
    logger.info("Slash commands synced")
    
    if bot.user:
        app_info = await bot.application_info()
        bot_owner_id = app_info.owner.id
    
    # Start game listener
    game_listener = AdvancedGameListener()
    game_listener.on_event(handle_game_event)
    asyncio.create_task(game_listener.start())

async def handle_game_event(event_type: str, data: dict):
    """Handle events from the game listener"""
    # This can be extended for additional bot reactions
    pass

# ============ Discord Commands ============
@tree.command(name="setup_channel", description="Set the notification channel")
async def setup_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    db.set_user_config(str(interaction.user.id), channel=str(channel.id))
    await interaction.response.send_message(f"Notification channel set to {channel.mention}", ephemeral=True)

@tree.command(name="setup_role", description="Set role to ping for notifications")
async def setup_role(interaction: discord.Interaction, role: discord.Role):
    db.set_user_config(str(interaction.user.id), role=str(role.id))
    await interaction.response.send_message(f"Notification role set to {role.mention}", ephemeral=True)

@tree.command(name="watch_add", description="Add an item to your watchlist")
async def watch_add(interaction: discord.Interaction, item_name: str, min_price: float = None, max_price: float = None):
    success = db.add_watch(str(interaction.user.id), item_name.lower(), min_price=min_price, max_price=max_price)
    if success:
        price_filter = ""
        if min_price and max_price:
            price_filter = f" (${min_price:,.0f} - ${max_price:,.0f})"
        elif min_price:
            price_filter = f" (min ${min_price:,.0f})"
        elif max_price:
            price_filter = f" (max ${max_price:,.0f})"
        await interaction.response.send_message(f"Added `{item_name}`{price_filter} to your watchlist!", ephemeral=True)
    else:
        await interaction.response.send_message(f"`{item_name}` is already in your watchlist.", ephemeral=True)

@tree.command(name="watch_add_category", description="Watch an entire category")
async def watch_add_category(interaction: discord.Interaction, category: str):
    valid_categories = ["seeds", "gear", "eggs", "event", "seasonal", "pet"]
    if category.lower() not in valid_categories:
        await interaction.response.send_message(f"Invalid category. Choose from: {', '.join(valid_categories)}", ephemeral=True)
        return
    success = db.add_watch(str(interaction.user.id), f"category:{category.lower()}", category=category.lower())
    if success:
        await interaction.response.send_message(f"Now watching all items in category: {category}", ephemeral=True)
    else:
        await interaction.response.send_message(f"You're already watching category: {category}", ephemeral=True)

@tree.command(name="watch_list", description="Show your watchlist")
async def watch_list(interaction: discord.Interaction):
    items = db.get_user_watchlist(str(interaction.user.id))
    if not items:
        await interaction.response.send_message("Your watchlist is empty.", ephemeral=True)
        return
    
    msg = "**Your Watchlist:**\n"
    for i in items:
        price_filter = ""
        if i.get('min_price') or i.get('max_price'):
            min_p = i.get('min_price') or "any"
            max_p = i.get('max_price') or "any"
            price_filter = f" [${min_p} - ${max_p}]"
        msg += f"• {i['item_name']} (Priority: {i['priority']}, Notified: {i['notify_count']}x){price_filter}\n"
    
    await interaction.response.send_message(msg, ephemeral=True)

@tree.command(name="watch_remove", description="Remove an item from your watchlist")
async def watch_remove(interaction: discord.Interaction, item_name: str):
    success = db.remove_watch(str(interaction.user.id), item_name.lower())
    if success:
        await interaction.response.send_message(f"Removed `{item_name}` from your watchlist.", ephemeral=True)
    else:
        await interaction.response.send_message(f"`{item_name}` was not in your watchlist.", ephemeral=True)

@tree.command(name="watch_wildcard", description="Add a wildcard pattern (e.g., *sprout*)")
async def watch_wildcard(interaction: discord.Interaction, pattern: str, min_price: float = None, max_price: float = None):
    success = db.add_watch(str(interaction.user.id), pattern, wildcard=pattern, min_price=min_price, max_price=max_price)
    if success:
        await interaction.response.send_message(f"Added wildcard `{pattern}` - you'll be notified of any matching items!", ephemeral=True)
    else:
        await interaction.response.send_message(f"Wildcard `{pattern}` is already in your watchlist.", ephemeral=True)

@tree.command(name="priority_high", description="Set an item to high priority (instant ping + DM)")
async def priority_high(interaction: discord.Interaction, item_name: str):
    db.remove_watch(str(interaction.user.id), item_name.lower())
    db.add_watch(str(interaction.user.id), item_name.lower(), priority="high")
    await interaction.response.send_message(f"`{item_name}` set to **HIGH** priority. You will receive instant ping + DM.", ephemeral=True)

@tree.command(name="priority_medium", description="Set an item to medium priority (channel ping only)")
async def priority_medium(interaction: discord.Interaction, item_name: str):
    db.remove_watch(str(interaction.user.id), item_name.lower())
    db.add_watch(str(interaction.user.id), item_name.lower(), priority="medium")
    await interaction.response.send_message(f"`{item_name}` set to **MEDIUM** priority. You will receive channel pings.", ephemeral=True)

@tree.command(name="priority_low", description="Set an item to low priority (daily digest)")
async def priority_low(interaction: discord.Interaction, item_name: str):
    db.remove_watch(str(interaction.user.id), item_name.lower())
    db.add_watch(str(interaction.user.id), item_name.lower(), priority="low")
    await interaction.response.send_message(f"`{item_name}` set to **LOW** priority. You will receive daily digest.", ephemeral=True)

@tree.command(name="stock_check", description="Check current stock for an item")
async def stock_check(interaction: discord.Interaction, item_name: str):
    items = db.get_all_stock(include_out_of_stock=True)
    found = [i for i in items if item_name.lower() in i["name"].lower()]
    if not found:
        await interaction.response.send_message(f"No stock information found for `{item_name}`.", ephemeral=True)
        return
    msg = f"**Stock for '{item_name}':**\n"
    for i in found[:10]:
        status = "✅ IN STOCK" if i['in_stock'] else "❌ OUT OF STOCK"
        msg += f"• {i['name']}: {status} (${i['price']:,.0f}) [Seen {i['times_seen']}x]\n"
    await interaction.response.send_message(msg, ephemeral=True)

@tree.command(name="stock_all", description="Show all current stock")
async def stock_all(interaction: discord.Interaction):
    items = db.get_all_stock()
    if not items:
        await interaction.response.send_message("No items currently in stock.", ephemeral=True)
        return
    
    # Group by category
    by_category = defaultdict(list)
    for i in items:
        by_category[i['category']].append(i)
    
    msg = f"**Items in stock ({len(items)}):**\n"
    for category, cat_items in sorted(by_category.items())[:5]:
        msg += f"\n**{category.upper()}** ({len(cat_items)}):\n"
        for i in cat_items[:5]:
            msg += f"  • {i['name']}: ${i['price']:,.0f}\n"
        if len(cat_items) > 5:
            msg += f"  ... and {len(cat_items) - 5} more\n"
    
    await interaction.response.send_message(msg, ephemeral=True)

@tree.command(name="stock_history", description="Show price history for an item")
async def stock_history(interaction: discord.Interaction, item_name: str, days: int = 7):
    history = db.get_price_history(item_name, days)
    if not history:
        await interaction.response.send_message(f"No price history found for `{item_name}`.", ephemeral=True)
        return
    
    msg = f"**Price history for {item_name} (last {days} days):**\n"
    for h in history[:20]:
        dt = datetime.fromisoformat(h['timestamp']).strftime("%Y-%m-%d %H:%M")
        msg += f"• ${h['price']:,.0f} at {dt}\n"
    
    await interaction.response.send_message(msg, ephemeral=True)

@tree.command(name="events", description="Show all active events")
async def events(interaction: discord.Interaction):
    active = db.get_active_events()
    if not active:
        await interaction.response.send_message("No active events detected.", ephemeral=True)
        return
    
    embed = discord.Embed(title="📅 Active Events", color=0x00ff88, timestamp=datetime.now())
    for event in active:
        phase_text = f"**Phase:** {event['phase']}\n" if event.get('phase') else ""
        date_text = ""
        if event.get('start_date'):
            date_text += f"Start: {event['start_date'][:10]}\n"
        if event.get('end_date'):
            date_text += f"End: {event['end_date'][:10]}\n"
            
        embed.add_field(
            name=f"🎯 {event['name']}",
            value=f"{phase_text}{date_text}{event.get('description', '')[:200]}",
            inline=False
        )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tree.command(name="event_details", description="Get detailed information about a specific event")
async def event_details(interaction: discord.Interaction, event_name: str):
    event = db.get_event_details(event_name)
    if not event:
        await interaction.response.send_message(f"No event found with name: {event_name}", ephemeral=True)
        return
    
    embed = discord.Embed(title=f"📋 {event['name']}", color=0xffaa00, timestamp=datetime.now())
    embed.add_field(name="Event Type", value=event['type'], inline=True)
    if event.get('phase'):
        embed.add_field(name="Current Phase", value=event['phase'], inline=True)
    if event.get('start_date'):
        embed.add_field(name="Start Date", value=event['start_date'][:16], inline=True)
    if event.get('end_date'):
        embed.add_field(name="End Date", value=event['end_date'][:16], inline=True)
    if event.get('description'):
        embed.add_field(name="Description", value=event['description'][:400], inline=False)
    
    # Add rewards if present
    rewards = event.get('data', {}).get('rewards', [])
    if rewards:
        embed.add_field(name="Rewards", value="\n".join(rewards[:10]), inline=False)
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tree.command(name="future_event_watch", description="Watch for a future event")
async def future_event_watch(interaction: discord.Interaction, pattern: str, event_type: str = None):
    success = db.add_future_watch(str(interaction.user.id), pattern.lower(), event_type)
    if success:
        msg = f"Now watching for events matching: `{pattern}`"
        if event_type:
            msg += f" (type: {event_type})"
        await interaction.response.send_message(msg, ephemeral=True)
    else:
        await interaction.response.send_message(f"You're already watching pattern: `{pattern}`.", ephemeral=True)

@tree.command(name="future_event_list", description="List future events being watched")
async def future_event_list(interaction: discord.Interaction):
    watches = db.get_future_watches()
    user_watches = [w for w in watches if w["user_id"] == str(interaction.user.id)]
    if not user_watches:
        await interaction.response.send_message("You aren't watching any future events.", ephemeral=True)
        return
    msg = "**Future events you're watching:**\n" + "\n".join([f"• {w['pattern']}" + (f" ({w['event_type']})" if w['event_type'] else "") for w in user_watches])
    await interaction.response.send_message(msg, ephemeral=True)

@tree.command(name="stats", description="Show bot statistics")
async def stats(interaction: discord.Interaction):
    items_in_stock = len(db.get_all_stock())
    active_events = len(db.get_active_events())
    total_watches = 0
    
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM watchlist")
        total_watches = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users")
        total_users = cursor.fetchone()[0]
    
    uptime = datetime.now() - game_listener.start_time if game_listener else timedelta(seconds=0)
    
    embed = discord.Embed(title="📊 Bot Statistics", color=0x9b59b6)
    embed.add_field(name="Stock Items Tracked", value=str(items_in_stock), inline=True)
    embed.add_field(name="Active Events", value=str(active_events), inline=True)
    embed.add_field(name="Total Users", value=str(total_users), inline=True)
    embed.add_field(name="Total Watchlist Items", value=str(total_watches), inline=True)
    embed.add_field(name="Messages Processed", value=str(game_listener.message_count) if game_listener else "0", inline=True)
    embed.add_field(name="Uptime", value=str(uptime).split('.')[0], inline=True)
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tree.command(name="update_db", description="Force refresh event data (admin only)")
async def update_db(interaction: discord.Interaction):
    if bot_owner_id and interaction.user.id != bot_owner_id:
        await interaction.response.send_message("This command is for the bot owner only.", ephemeral=True)
        return
    await interaction.response.send_message("Force refreshing...", ephemeral=True)
    db.update_metric("last_manual_refresh", datetime.now().isoformat())

# ============ FastAPI Web Server ============
websocket_connections: Set[WebSocket] = set()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start Discord bot
    asyncio.create_task(bot.start(DISCORD_TOKEN))
    yield
    # Cleanup
    if game_listener:
        game_listener.stop()
    await bot.close()

app = FastAPI(title="Grow a Garden Stock Monitor", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Endpoints
@app.get("/api/stock")
async def get_stock(include_out: bool = False):
    return db.get_all_stock(include_out_of_stock=include_out)

@app.get("/api/events")
async def get_events():
    return db.get_active_events()

@app.get("/api/watchlist")
async def get_watchlist(user_id: str = None):
    if user_id:
        return db.get_user_watchlist(user_id)
    return []

@app.get("/api/stats")
async def get_stats():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM stock_cache WHERE in_stock = 1")
        in_stock = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM events WHERE active = 1")
        active_events = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users")
        users = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM watchlist WHERE enabled = 1")
        watches = cursor.fetchone()[0]
    
    return {
        "items_in_stock": in_stock,
        "active_events": active_events,
        "total_users": users,
        "total_watches": watches,
        "websocket_connected": bool(game_listener and game_listener.websocket and not game_listener.websocket.closed)
    }

@app.get("/api/price_history/{item_name}")
async def price_history(item_name: str, days: int = 7):
    return db.get_price_history(item_name, days)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    websocket_connections.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        websocket_connections.discard(websocket)

# HTML Website (simplified for length - full version in previous response)
HTML_PAGE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Grow a Garden Stock Monitor</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { background: #0a0c10; color: #e0e0e0; font-family: -apple-system, BlinkMacSystemFont, sans-serif; padding: 20px; }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { margin-bottom: 20px; color: #ffd700; }
        .stats-bar { display: flex; gap: 20px; margin-bottom: 20px; flex-wrap: wrap; }
        .stat-card { background: #1e2227; padding: 15px; border-radius: 8px; flex: 1; min-width: 120px; text-align: center; }
        .stat-value { font-size: 28px; font-weight: bold; color: #ffd700; }
        .stat-label { font-size: 12px; color: #888; margin-top: 5px; }
        .tabs { display: flex; gap: 5px; margin-bottom: 20px; flex-wrap: wrap; }
        .tab-btn { background: #1e2227; border: none; color: #aaa; padding: 10px 20px; cursor: pointer; border-radius: 8px; }
        .tab-btn.active { background: #ffd700; color: #0a0c10; }
        .stock-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 15px; }
        .stock-card { background: #1e2227; border-radius: 12px; padding: 15px; }
        .stock-card.highlight { border: 2px solid #ffd700; background: #2a2510; }
        .stock-name { font-weight: bold; margin-bottom: 8px; }
        .stock-price { font-size: 20px; font-weight: bold; color: #4caf50; }
        .stock-status { display: inline-block; font-size: 12px; padding: 2px 8px; border-radius: 20px; background: #2e7d32; }
        .no-items { text-align: center; padding: 40px; color: #888; }
        .loading { text-align: center; padding: 40px; }
        @media (max-width: 600px) { .stock-grid { grid-template-columns: 1fr; } }
    </style>
</head>
<body>
    <div class="container">
        <h1>🌱 Grow a Garden Stock Monitor</h1>
        <div class="stats-bar" id="stats-bar">
            <div class="stat-card"><div class="stat-value" id="stock-count">--</div><div class="stat-label">Items in Stock</div></div>
            <div class="stat-card"><div class="stat-value" id="event-count">--</div><div class="stat-label">Active Events</div></div>
            <div class="stat-card"><div class="stat-value" id="watch-count">--</div><div class="stat-label">Total Watches</div></div>
        </div>
        <div class="tabs">
            <button class="tab-btn active" data-tab="all">All Stock</button>
            <button class="tab-btn" data-tab="seeds">Seeds</button>
            <button class="tab-btn" data-tab="gear">Gear</button>
            <button class="tab-btn" data-tab="eggs">Eggs</button>
            <button class="tab-btn" data-tab="event">Event</button>
            <button class="tab-btn" data-tab="seasonal">Seasonal</button>
        </div>
        <div id="stock-container"><div class="loading">Loading...</div></div>
    </div>
    <script>
        let currentTab = "all";
        async function loadStats() {
            const res = await fetch("/api/stats");
            const stats = await res.json();
            document.getElementById("stock-count").textContent = stats.items_in_stock;
            document.getElementById("event-count").textContent = stats.active_events;
            document.getElementById("watch-count").textContent = stats.total_watches;
        }
        async function loadStock() {
            const res = await fetch("/api/stock");
            let items = await res.json();
            if (currentTab !== "all") items = items.filter(i => i.category === currentTab);
            const inStock = items.filter(i => i.in_stock);
            const container = document.getElementById("stock-container");
            if (inStock.length === 0) { container.innerHTML = '<div class="no-items">No items in stock</div>'; return; }
            let html = '<div class="stock-grid">';
            for (const item of inStock) {
                const highlight = item.name.toLowerCase().includes("sprout") || item.name.toLowerCase().includes("lyrebird");
                html += `<div class="stock-card ${highlight ? 'highlight' : ''}">
                    <div class="stock-name">${escapeHtml(item.name)}</div>
                    <div class="stock-category">${item.category || 'General'}</div>
                    <div class="stock-price">$${formatPrice(item.price)}</div>
                    <span class="stock-status">IN STOCK</span>
                </div>`;
            }
            html += '</div>';
            container.innerHTML = html;
        }
        function formatPrice(p) { if (p >= 1e6) return (p/1e6).toFixed(2)+'M'; if (p>=1e3) return (p/1e3).toFixed(1)+'K'; return p; }
        function escapeHtml(t) { const d=document.createElement('div'); d.textContent=t; return d.innerHTML; }
        document.querySelectorAll(".tab-btn").forEach(btn => {
            btn.addEventListener("click", () => {
                document.querySelectorAll(".tab-btn").forEach(b=>b.classList.remove("active"));
                btn.classList.add("active");
                currentTab = btn.dataset.tab;
                loadStock();
            });
        });
        loadStats(); loadStock();
        setInterval(() => { loadStats(); loadStock(); }, 30000);
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
    uvicorn.run(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    if not DISCORD_TOKEN:
        logger.warning("DISCORD_TOKEN environment variable not set. Discord bot will not work.")
    main()
