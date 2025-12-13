# -*- coding: utf-8 -*-
import os
import re
import sys
import json
import time
import random
import requests
import pytz
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta, date
from urllib.parse import quote
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
from requests.exceptions import RequestException, SSLError, ProxyError
from matplotlib.ticker import FuncFormatter
import html
import io
from io import BytesIO
import argparse
import shutil
import hashlib # Для кэша
import psutil # Для мониторинга нагрузки
import logging # Для логирования
from PIL import Image, ImageDraw, ImageFont, ImageEnhance # Для наложения лого
import base64 # Для кодирования лого если нужно
# ===================== НАСТРОЙКИ =====================
BYMYKEL_URL = "https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/ru/all.json"
LOCAL_DB = "items.json"
APPID = 730
# Telegram
TOKEN = os.environ.get("TELEGRAM_TOKEN", "8427688497:AAGkBisiTfJM3RDc8DOG9Kx9l9EnekoFGQk")
CHAT_ID = os.environ.get("CHAT_ID", "-1003143360650")
MONITOR_BOT_CHAT_ID = os.environ.get("MONITOR_BOT_CHAT_ID", "873939087") # ID приватного чата с монитор-ботом (опционально)
# Steam sessionid
SESSIONID = os.environ.get("STEAM_SESSIONID", None)
# Прокси
USE_PROXY_BY_DEFAULT = False # Изменено на False для обхода таймаута
PROXY_HTTP_URL = "http://mm4pkP:a6K4yx@95.181.155.167:8000"
PROXY_HTTP_ALT = "http://lte6:LVxqnyQiMH@65.109.79.15:13014"
# Поведение запросов (увеличено для снижения 429)
REQUEST_DELAY = 5.5 # Увеличено до 7s для снижения 429
JITTER = 3.0
MAX_RETRIES = 2
MAX_RETRIES_429 = 3
BACKOFF_BASE = 2.0
RATE_LIMIT_PAUSE = 30
RATE_LIMIT_COUNT = 0
# Фильтры
VOLATILITY_THRESHOLD = 6.0
PRICE_CHANGE_THRESHOLD = 8.0
BREAKOUT_THRESHOLD = 1.5
MIN_PRICE = 1.5
MIN_VOLUME_24H = 1
HISTORY_DAYS = 7
USD_RATE = 83.4
# Папка для CSV/PNG
OUT_DIR = "out"
os.makedirs(OUT_DIR, exist_ok=True)
# Таймзона
EEST_TZ = pytz.timezone("Europe/Tallinn")
TZ = pytz.timezone("Europe/Moscow")
DEFAULT_SUMMARY_TIME = "00:00"
# Время подготовки к summary
SUMMARY_PREP_MINUTES = 0.5
# SSL
ALLOW_INSECURE = False
# Лог
LOG_FILE = "posted_items.json"
SUMMARY_LOG = "summary_log.json"
POSTED_HISTORY_FILE = "posted_history.json"
POSTED_ITEMS_FILE = "posted_items_data.json"
# Кэш
CACHE_FILE = "item_cache.json"
CACHE_TTL = 1800
# Кэш изображений
IMAGE_CACHE_FILE = "image_cache.json"
IMAGE_CACHE_TTL = 3600
# Мониторинг
MONITOR_INTERVAL = 50
PROGRESS_INTERVAL = 10
# Логирование
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
BOT_LOG_FILE = os.path.join(LOG_DIR, "bot_events.log")
# Лого
LOGO_PATH = "VS1.png"
LOGO_OPACITY = 0.7 # ИЗМЕНЕНО: Уменьшено для summary (было 1.0), чтобы водяной знак был заметнее, но не доминировал
CATEGORY_KEYWORDS = {
    'pistol': ['пистолет', 'pistol'],
    'rifle': ['винтовк', 'shurmov', 'снайперск', 'rifle'],
    'smg': ['пистолет-пулемёт', 'пп', 'smg'],
    'shotgun': ['дробовик', 'shotgun'],
    'machinegun': ['пулемёт', 'machine gun'],
    'knife': ['нож', 'knife'],
    'gloves': ['перчатки', 'gloves'],
    'heavy': ['тяжелое', 'heavy'],
    'sniper': ['снайперская винтовка', 'sniper rifle']
}
CATEGORY_HASHTAGS = {
    'pistol': '#пистолет',
    'rifle': '#винтовка',
    'smg': '#пп',
    'shotgun': '#дробовик',
    'machinegun': '#пулемет',
    'knife': '#нож',
    'gloves': '#перчатки',
    'heavy': '#тяжелое',
    'sniper': '#снайперскаявинтовка',
    'other': '#другое'
}
# Карты для тегов команд и турниров (улучшено для стикеров)
TEAM_MAP = {
    'faze clan': 'fazeclan',
    'nav i': 'navi',
    'astralis': 'astralis',
    'fnatic': 'fnatic',
    'virtus pro': 'virtuspro',
    'virtus.pro': 'virtuspro',
    'team liquid': 'liquid',
    'g2': 'g2',
    'g2 esports': 'g2',
    'nip': 'nip',
    'ence': 'ence',
    'team spirit': 'spirit',
    'cloud9': 'cloud9',
    'evil geniuses': 'evil',
    'heroic': 'heroic',
    'imperium': 'imperium',
    'the mongolz': 'mongolz',
    'mongolz': 'mongolz'
}
TOURNAMENT_MAP = {
    'berlin': 'берлин2019',
    'stockholm': 'стокгольм2021',
    'antwerp': 'антверпен2022',
    'copenhagen': 'копенгаген2024',
    'paris': 'париж2023',
    'shanghai': 'шанхай2024',
    'bangkok': 'бангкок2024', # Если актуально
    'katowice': 'катовице2014',
    'dreamhack': 'дримхак',
    'iem': 'iem',
    'major': 'мейджор'
}
# Заглушка
MAINTENANCE_MESSAGE = "⚠️ Я временно недоступен, ведутся технические работы!\nНе теряйте меня! Подписывайтесь на наши новости: @valvestreet_media"
# ===================== НАСТРОЙКА ЛОГИРОВАНИЯ =====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(BOT_LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
def log_event(stage: str, description: str, item_name: str = None):
    msg = f"Stage: {stage} | Description: {description}"
    if item_name:
        msg += f" | Item: {item_name}"
    logger.info(msg)
# ===================== ФУНКЦИЯ ПОЛУЧЕНИЯ КУРСА USD/RUB =====================
def get_usd_to_rub_rate():
    url = "https://www.cbr-xml-daily.ru/daily_json.js"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        usd_rate = data['Valute']['USD']['Value']
        return usd_rate
    except Exception as e:
        logger.warning(f"Failed to fetch USD rate: {e}, using default {USD_RATE}")
        return USD_RATE
# ===================== КЭШ =====================
def load_cache() -> Dict[str, Dict]:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                cache = json.load(f)
                now_ts = time.time()
                for key in list(cache.keys()):
                    if now_ts - cache[key].get("timestamp", 0) > CACHE_TTL:
                        del cache[key]
                logger.info(f"Loaded cache with {len(cache)} valid entries")
                return cache
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
            pass
    return {}
def save_cache(cache: Dict[str, Dict]):
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error saving cache: {e}")
def clear_cache(): # ИЗМЕНЕНО: Новая функция для полной очистки кэша после summary
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
        logger.info("Cache cleared after summary")
# Кэш изображений
def load_image_cache() -> Dict[str, Dict]:
    if os.path.exists(IMAGE_CACHE_FILE):
        try:
            with open(IMAGE_CACHE_FILE, "r", encoding="utf-8") as f:
                cache = json.load(f)
                now_ts = time.time()
                for key in list(cache.keys()):
                    if now_ts - cache[key].get("timestamp", 0) > IMAGE_CACHE_TTL:
                        del cache[key]
                logger.info(f"Loaded image cache with {len(cache)} valid entries")
                return cache
        except Exception as e:
            logger.error(f"Error loading image cache: {e}")
            pass
    return {}
def save_image_cache(cache: Dict[str, Dict]):
    try:
        with open(IMAGE_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error saving image cache: {e}")
def clear_image_cache():
    if os.path.exists(IMAGE_CACHE_FILE):
        os.remove(IMAGE_CACHE_FILE)
        logger.info("Image cache cleared after summary")
# ===================== ФУНКЦИЯ МОНИТОРИНГА НАГРУЗКИ =====================
def print_resource_usage(item_count: int, total_items: int):
    try:
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory_percent = psutil.virtual_memory().percent
        print(f"[MONITOR] After {item_count}/{total_items} items: CPU {cpu_percent:.1f}%, Memory {memory_percent:.1f}%")
    except Exception as e:
        logger.warning(f"Resource monitoring failed: {e}")
def print_progress(item_count: int, total_items: int):
    print(f"[PROGRESS] Processed {item_count}/{total_items} items ({item_count/total_items*100:.1f}%)")
    log_event("progress_update", f"Processed {item_count}/{total_items} items ({item_count/total_items*100:.1f}%)")
# ===================== Сессия requests =====================
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/118.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.5"
})
session.trust_env = False
session.verify = True
if SESSIONID:
    session.cookies.set("sessionid", SESSIONID)
def enable_proxy(proxy_url: str):
    global RATE_LIMIT_COUNT
    RATE_LIMIT_COUNT = 0
    session.proxies.update({"http": proxy_url, "https": proxy_url})
def disable_proxy():
    global RATE_LIMIT_COUNT
    RATE_LIMIT_COUNT = 0
    session.proxies.clear()
USE_PROXY = USE_PROXY_BY_DEFAULT
if USE_PROXY:
    enable_proxy(PROXY_HTTP_URL)
else:
    disable_proxy()
# ===================== УТИЛИТЫ =====================
def safe_json_loads(s: str) -> Optional[Any]:
    try:
        s2 = s.replace("'", '"')
        s2 = re.sub(r",\s*]", "]", s2)
        s2 = re.sub(r",\s*}", "}", s2)
        return json.loads(s2)
    except Exception:
        return None
def parse_date(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            if value > 1e12:
                return datetime.fromtimestamp(value / 1000.0)
            return datetime.fromtimestamp(value)
        except:
            return None
    if isinstance(value, str):
        s = value.strip()
        s = re.sub(r"\s*\+?\d+$", "", s).strip()
        formats = [
            "%b %d %Y %H:%M:%S",
            "%b %d %Y %H:%M",
            "%b %d %Y %H:",
            "%Y-%m-%d %H:%M:%S",
            "%d %b %Y %H:%M:%S",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(s, fmt)
            except:
                pass
        m = re.match(r"([A-Za-z]{3}\s+\d{1,2}\s+\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?", s)
        if m:
            try:
                base = m.group(1)
                hh = int(m.group(2)); mm = int(m.group(3)); ss = int(m.group(4)) if m.group(4) else 0
                dt = datetime.strptime(base, "%b %d %Y").replace(hour=hh, minute=mm, second=ss)
                return dt
            except:
                pass
    return None
# ===================== ЛОГ ПОСЛЕДНЕЙ СВОДКИ =====================
def load_last_summary() -> Optional[datetime]:
    if os.path.exists(SUMMARY_LOG):
        try:
            with open(SUMMARY_LOG, "r", encoding="utf-8") as f:
                data = json.load(f)
                return datetime.fromisoformat(data['last_sent'].replace('Z', '+00:00'))
        except Exception as e:
            logger.warning(f"Error loading last summary: {e}")
            pass
    return None
def save_last_summary(dt: datetime):
    try:
        with open(SUMMARY_LOG, "w", encoding="utf-8") as f:
            json.dump({'last_sent': dt.isoformat()}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error saving last summary: {e}")
# ===================== НОВЫЕ ФУНКЦИИ ДЛЯ POSTED ITEMS =====================
def load_posted_items(for_summary: bool = False) -> List[Dict]:
    if os.path.exists(POSTED_ITEMS_FILE):
        try:
            with open(POSTED_ITEMS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                now_ts = time.time()
                filtered = []
                for p in data:
                    if isinstance(p, dict) and "growth" in p and now_ts - p.get("timestamp", 0) < 86400:
                        # Валидация: убедимся, что ключевые поля присутствуют
                        if all(k in p for k in ["name", "price_rub", "volatility", "growth"]):
                            filtered.append(p)
                if for_summary:
                    logger.info(f"Loaded {len(filtered)} valid recent posted items for summary")
                return filtered
        except Exception as e:
            logger.error(f"Error loading posted items: {e}")
            pass
    return []
def save_posted_item(item: Dict):
    try:
        posted_items = load_posted_items(for_summary=False) # No log spam
        item_copy = make_serializable(item.copy())
        item_copy["timestamp"] = time.time()
        posted_items.append(item_copy)
        now_ts = time.time()
        posted_items = [p for p in posted_items if isinstance(p, dict) and now_ts - p.get("timestamp", 0) < 86400]
        with open(POSTED_ITEMS_FILE, "w", encoding="utf-8") as f:
            json.dump(posted_items, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error saving posted item: {e}")
# ===================== УВЕДОМЛЕНИЕ МОНИТОР-БОТА =====================
def notify_monitor_bot(event_type: str, details: str = ""):
    """Отправляет уведомление монитор-боту в приватный чат для симуляции 'поста в канале'."""
    if not MONITOR_BOT_CHAT_ID:
        logger.info("No MONITOR_BOT_CHAT_ID set, skipping notification")
        return
    timestamp = datetime.now().isoformat()
    message = f"{event_type}:{timestamp}:{details}"
    send_url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": MONITOR_BOT_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        r = session.post(send_url, data=payload, timeout=12)
        if r.status_code == 200 and r.json().get("ok"):
            logger.info(f"Notification sent to monitor bot: {message}")
        else:
            logger.warning(f"Notification to monitor bot failed: {r.text if r else 'No response'}")
    except Exception as e:
        logger.error(f"Error sending notification to monitor bot: {e}")
# ===================== РЕКВЕСТЫ С RETRY / FALLBACK =====================
def request_with_retries(url: str, params=None, headers=None, timeout=15, allow_429_backoff=True, force_direct=False):
    global RATE_LIMIT_COUNT, USE_PROXY
    if force_direct:
        session.proxies.clear()
    attempt = 0
    consecutive_429 = 0
    while attempt <= MAX_RETRIES:
        try:
            r = session.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code == 429 and allow_429_backoff:
                consecutive_429 += 1
                RATE_LIMIT_COUNT += 1
                attempt_429 = 0
                while attempt_429 < MAX_RETRIES_429:
                    attempt_429 += 1
                    backoff = BACKOFF_BASE ** attempt_429 + random.random()
                    time.sleep(backoff)
                    r = session.get(url, params=params, headers=headers, timeout=timeout)
                    if r.status_code != 429:
                        consecutive_429 = 0
                        break
                if consecutive_429 >= 3:
                    time.sleep(RATE_LIMIT_PAUSE)
                    consecutive_429 = 0
                    if USE_PROXY:
                        enable_proxy(PROXY_HTTP_ALT)
                        USE_PROXY = True
                    else:
                        disable_proxy()
            return r
        except (SSLError, ProxyError) as err:
            consecutive_429 = 0
            old_proxies = session.proxies.copy()
            try:
                if "mm4pkP" in PROXY_HTTP_URL and USE_PROXY:
                    enable_proxy(PROXY_HTTP_ALT)
                else:
                    disable_proxy()
                r = session.get(url, params=params, headers=headers, timeout=timeout)
                if r.status_code == 429 and allow_429_backoff:
                    attempt_429 = 0
                    while attempt_429 < MAX_RETRIES_429:
                        attempt_429 += 1
                        backoff = BACKOFF_BASE ** attempt_429 + random.random()
                        time.sleep(backoff)
                        r = session.get(url, params=params, headers=headers, timeout=timeout)
                        if r.status_code != 429:
                            break
                return r
            except Exception:
                pass
            finally:
                session.proxies = old_proxies
            attempt += 1
            backoff = BACKOFF_BASE ** attempt + random.random()
            time.sleep(backoff)
            continue
        except RequestException as e:
            attempt += 1
            backoff = BACKOFF_BASE ** attempt + random.random()
            time.sleep(backoff)
            continue
    logger.warning(f"Request to {url} failed after {MAX_RETRIES} retries")
    return None
# ===================== ЗАГРУЗКА ПРЕДМЕТОВ =====================
def load_items(force_update: bool = False) -> Dict[str, Any]:
    items = {} # Инициализируем пустым
    if os.path.exists(LOCAL_DB) and not force_update:
        try:
            with open(LOCAL_DB, "r", encoding="utf-8") as f:
                items = json.load(f)
                log_event("load_local", f"Loaded {len(items)} items from local DB")
                return items
        except json.JSONDecodeError as e:
            log_event("json_error", f"JSON error in local DB: {e}. Forcing update.")
            force_update = True
        except Exception as e:
            logger.error(f"Error loading local DB: {e}")
            force_update = True
    # ИСПРАВЛЕНИЕ: Если файла нет или force_update=True — скачиваем
    if not os.path.exists(LOCAL_DB) or force_update:
        log_event("fetch_attempt", "Attempting to fetch items from API")
        r = request_with_retries(BYMYKEL_URL, timeout=60)
        if r and r.status_code == 200:
            items = r.json()
            try:
                with open(LOCAL_DB, "w", encoding="utf-8") as f:
                    json.dump(items, f, ensure_ascii=False, indent=2)
                log_event("fetch_success", f"Saved {len(items)} items to local DB")
                return items
            except Exception as e:
                log_event("save_error", f"Failed to save items: {e}")
                return items
        else:
            log_event("fetch_failed", f"Failed to fetch items (status: {r.status_code if r else 'None'}). Using empty dict.")
    else:
        log_event("no_local", "No local DB found. Using empty dict.")
    log_event("empty_fallback", "Returning empty items dict")
    return items # Возвращаем items (может быть пустым)
def get_valid_items(items: dict) -> List[Dict[str, Any]]:
    valid = []
    for it in items.values():
        if isinstance(it, dict) and it.get("name"):
            valid.append(it)
    logger.info(f"Validated {len(valid)} items out of {len(items)}")
    return valid
def build_market_hash_name(item: dict) -> str:
    if item.get("market_hash_name"):
        return item["market_hash_name"]
    name = item.get("name", "").strip()
    if not name:
        return ""
    exterior = item.get("wear", {}).get("name") if isinstance(item.get("wear"), dict) else item.get("exterior")
    if exterior:
        name += f" ({exterior})"
    if item.get("stattrak"):
        name = f"StatTrak™ {name}"
    if item.get("souvenir"):
        name = f"Souvenir {name}"
    return name
def parse_order_table(soup: BeautifulSoup, table_id: str) -> List[List[float]]:
    div = soup.find("div", {"id": table_id})
    if not div:
        return []
    rows = div.find_all("tr")[1:]
    price_qty = []
    for row in rows:
        tds = row.find_all("td")
        if len(tds) != 2:
            continue
        price_text = tds[0].text.strip()
        qty_text = tds[1].text.strip()
        # parse price
        if 'и ' in price_text:
            m = re.match(r'(.+?)( руб\. и)', price_text)
            if m:
                price_str = m.group(1).replace(',', '.').strip()
            else:
                price_str = price_text.replace(' руб.', '').replace(',', '.').strip()
        else:
            price_str = price_text.replace(' руб.', '').replace(',', '.').strip()
        try:
            price = float(price_str)
        except ValueError:
            continue
        # Improved qty parsing with better cleaning
        qty_text_clean = qty_text.replace(',', '').replace(' ', '').replace('\xa0', '')
        qty_text_clean = re.sub(r'[^\d]', '', qty_text_clean)
        try:
            qty = int(qty_text_clean)
        except ValueError:
            qty = 0
        if qty > 0:
            price_qty.append([price, qty])
    price_qty.sort(key=lambda x: x[0])
    graph = []
    cumul = 0
    for p, q in price_qty:
        cumul += q
        graph.append([p, float(cumul)])
    return graph
# ===================== ЛЁГКИЙ ПАРСИНГ ИСТОРИИ =====================
def quick_parse_history(raw_history, usd_rate):
    if not raw_history:
        return {
            "current_price_usd": 0.0,
            "volume_24h": 0,
            "prices": [],
            "price_growth": 0.0,
            "volume_growth": 0.0
        }
    now = datetime.now(tz=TZ)
    last_24h_start = now - timedelta(hours=24)
    prev_24h_start = now - timedelta(hours=48)
    prev_24h_end = last_24h_start
    cutoff_date = now - timedelta(days=HISTORY_DAYS)
    rows = []
    for p in raw_history: # Убрал reversed — парсим в исходном порядке (Steam: newest first)
        try:
            date_raw, price_str, volume_str = p
            date_raw = re.sub(r' \+\d+$', '', date_raw).strip()
            if ':' in date_raw:
                date_raw = date_raw.rstrip(':').strip()
            dt = pd.to_datetime(date_raw, utc=True, dayfirst=False, errors='coerce')
            if pd.isna(dt):
                continue
            dt = dt.tz_convert('Europe/Moscow')
            if dt < cutoff_date:
                continue # Не break, чтобы собрать все
            price = parse_price_text(price_str)
            volume = parse_volume(volume_str)
            rows.append({"timestamp": dt, "price_usd": price, "volume": volume})
        except Exception:
            continue
    if not rows:
        return {
            "current_price_usd": 0.0,
            "volume_24h": 0,
            "prices": [],
            "price_growth": 0.0,
            "volume_growth": 0.0
        }
    df = pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True) # Сортируем по времени (oldest first)
    # Current price: последняя цена в last_24h (или общая последняя, если нет)
    last_24h_df = df[df["timestamp"] >= last_24h_start]
    current_price_usd = last_24h_df["price_usd"].iloc[-1] if not last_24h_df.empty else df["price_usd"].iloc[-1]
    # Prev price: цена closest to exactly 24h ago
    exact_24h_ago = now - timedelta(hours=24)
    # Filter to prev period for safety
    prev_candidates = df[(df["timestamp"] >= prev_24h_start) & (df["timestamp"] < prev_24h_end)]
    if not prev_candidates.empty:
        closest_idx = (prev_candidates["timestamp"] - exact_24h_ago).abs().idxmin()
        prev_price = prev_candidates.loc[closest_idx, "price_usd"]
    else:
        prev_price = 0.0
    # Volumes
    volume_24h = last_24h_df["volume"].sum() if not last_24h_df.empty else 0
    prev_volume = prev_candidates["volume"].sum() if not prev_candidates.empty else 0
    # Growth
    price_growth = ((current_price_usd - prev_price) / prev_price * 100) if prev_price > 0 else 0.0
    volume_growth = ((volume_24h - prev_volume) / prev_volume * 100) if prev_volume > 0 else (100.0 if volume_24h > 0 else -100.0)
    return {
        "current_price_usd": current_price_usd,
        "volume_24h": volume_24h,
        "prices": df["price_usd"].tolist(),
        "price_growth": price_growth,
        "volume_growth": volume_growth
    }
# ===================== ПОЛУЧЕНИЕ ИСТОРИИ ЦЕН И ДРУГИХ ДАННЫХ =====================
def get_item_data(market_hash_name: str) -> Dict[str, Any]:
    encoded = quote(market_hash_name, safe='')
    url = f"https://steamcommunity.com/market/listings/{APPID}/{encoded}"
    headers = {"Referer": url}
    r = request_with_retries(url, headers=headers, timeout=20)
    if not r or r.status_code != 200:
        logger.warning(f"Failed to fetch item data for {market_hash_name}")
        return {"history": [], "sell_listings": 0, "buy_orders": 0, "total_listings": 0, "price_usd": 0.0, "image_url": "", "histogram": None}
    soup = BeautifulSoup(r.text, "html.parser")
    # Парсинг истории цен (unchanged)
    scripts = soup.find_all("script")
    candidate = None
    for script in scripts:
        text = script.string
        if not text:
            continue
        m = re.search(r'var\s+line1\s*=\s*(\[\s*\[.*?\]\s*\])\s*;', text, re.DOTALL)
        if not m:
            m = re.search(r'var\s+g_rgHistory\s*=\s*(\[\s*\[.*?\]\s*\])\s*;', text, re.DOTALL)
        if not m:
            m2 = re.search(r'Market_LoadOrderHistogram\(\s*(\{.*?"sell_order_table".*?\})\s*\)', text, re.DOTALL)
            if m2:
                obj = safe_json_loads(m2.group(1))
                if obj:
                    pass
            m = None
        if m:
            candidate = m.group(1)
            break
    if candidate:
        parsed = safe_json_loads(candidate)
        if parsed:
            history = parsed
        else:
            arrs = re.findall(r'\[\s*([^\]]*?)\s*\]', candidate, re.DOTALL)
            history = []
            for arr in arrs:
                parts = [p.strip() for p in re.split(r'\s*,\s*(?=(?:[^"]*"[^"]*")*[^"]*$)', arr)]
                if len(parts) >= 2:
                    date_part = parts[0].strip().strip('"').strip("'")
                    price_part = parts[1].strip().strip('"').strip("'")
                    vol_part = parts[2].strip().strip('"').strip("'") if len(parts) > 2 else "1"
                    vol_val = int(float(vol_part)) if vol_part else 1
                    history.append([date_part, price_part, vol_val])
    else:
        history = []
    # Парсинг total_listings (unchanged)
    total_listings = 0
    paging_summary = soup.find("div", class_="market_paging_summary ellipsis")
    if paging_summary:
        total_text = paging_summary.text.strip()
        total_match = re.search(r'из\s+(\d+)', total_text)
        if total_match:
            total_listings = int(total_match.group(1))
        else:
            total_span = soup.find("span", id="searchResults_total")
            if total_span:
                total_listings = int(total_span.text.strip())
    # Парсинг таблиц ордеров
    sell_graph = parse_order_table(soup, "market_commodity_forsale_table")
    buy_graph = parse_order_table(soup, "market_commodity_buyreqeusts_table")
    sell_listings = float(sell_graph[-1][1]) if sell_graph else 0
    buy_orders = float(buy_graph[-1][1]) if buy_graph else 0
    # Fallback к API
    histogram = None
    if not sell_graph or not buy_graph:
        scripts = soup.find_all("script")
        item_nameid = None
        for script in scripts:
            text = script.string
            if text:
                m = re.search(r'Market_LoadOrderSpread\(\s*(\d+)\s*\)', text)
                if m:
                    item_nameid = m.group(1)
                    break
        if item_nameid:
            histogram_url = f"https://steamcommunity.com/market/itemordershistogram?country=RU&language=russian&currency=5&item_nameid={item_nameid}&two_factor=0&norender=1"
            r_hist = request_with_retries(histogram_url, timeout=20)
            if r_hist and r_hist.status_code == 200:
                j = r_hist.json()
                if 'success' in j and j['success'] == 1:
                    buy_orders = j.get('buy_order_count', buy_orders)
                    sell_listings = j.get('sell_order_count', sell_listings)
                    histogram = j
    else:
        all_prices = [p for p, c in sell_graph + buy_graph]
        all_cumuls = [c for p, c in sell_graph + buy_graph]
        min_x = min(all_prices) if all_prices else 0
        max_x = max(all_prices) if all_prices else 0
        max_y = max(all_cumuls) if all_cumuls else 0
        histogram = {
            "buy_order_graph": buy_graph,
            "sell_order_graph": sell_graph,
            "graph_min_x": min_x,
            "graph_max_x": max_x,
            "graph_max_y": max_y
        }
    # Парсинг lowest price
    price_span = soup.find("span", class_="market_listing_price market_listing_price_with_fee")
    price_usd = 0.0
    if price_span:
        price_text = price_span.text.strip()
        usd_match = re.search(r'\$([\d.]+)', price_text)
        if usd_match:
            price_usd = float(usd_match.group(1))
        else:
            price_usd = parse_price_text(price_text)
    # ИЗМЕНЕНО: Улучшенный парсинг image_url — по классу вместо id, с fallback'ами
    image_url = ""
    # Основной поиск: img в .market_listing_largeimage
    image_elem = soup.select_one(".market_listing_largeimage img")
    if image_elem:
        image_url = image_elem.get('src') or image_elem.get('data-src', '') or image_elem.get('srcset', '').split(',')[0].strip().split(' ')[0]
        logger.debug(f"Found image via .market_listing_largeimage: {image_url}")
    # Fallback 1: img в hover (largeiteminfo_item_icon)
    if not image_url:
        image_elem = soup.find("img", id="largeiteminfo_item_icon")
        if image_elem:
            image_url = image_elem.get('src') or image_elem.get('data-src', '') or image_elem.get('srcset', '').split(',')[0].strip().split(' ')[0]
            logger.debug(f"Found image via largeiteminfo_item_icon: {image_url}")
    # Fallback 2: Старый поиск (на всякий случай)
    if not image_url:
        image_elem = soup.find("img", id="largeItemImage")
        if image_elem:
            image_url = image_elem.get('src') or image_elem.get('data-src', '') or image_elem.get('srcset', '').split(',')[0].strip().split(' ')[0]
            logger.debug(f"Found image via largeItemImage: {image_url}")
    # Нормализация: добавляем протокол/хост если относительный
    if image_url and not image_url.startswith('http'):
        image_url = f"https://steamcommunity.com{image_url}"
    # Финальная проверка и лог
    if image_url:
        logger.info(f"Successfully parsed image URL for {market_hash_name}: {image_url}")
    else:
        logger.warning(f"Failed to parse image URL for {market_hash_name} — will use fallback from JSON")
    # No fallback here, handle in main loop
    return {
        "history": history,
        "sell_listings": sell_listings,
        "buy_orders": buy_orders,
        "total_listings": total_listings,
        "price_usd": price_usd,
        "image_url": image_url,
        "histogram": histogram
    }
# ===================== ФУНКЦИЯ FETCH ИЗОБРАЖЕНИЯ (ОБНОВЛЁННАЯ) =====================
def fetch_item_image(image_url: str, mhn: str, max_retries: int = 5) -> tuple[BytesIO, bool]:
    """Fetch image with retries. Returns (buf, is_placeholder)."""
    is_placeholder = False
    if not image_url:
        logger.warning(f"No image URL for {mhn} — generating placeholder")
        is_placeholder = True
        return create_placeholder_image(mhn), is_placeholder
    cache_key = hashlib.md5(image_url.encode()).hexdigest()
    now_ts = time.time()
    if cache_key in image_cache and (now_ts - image_cache[cache_key].get("timestamp", 0)) < IMAGE_CACHE_TTL:
        # Восстанавливаем из base64
        buf = BytesIO(base64.b64decode(image_cache[cache_key]["data"]))
        logger.info(f"Image cache hit for {mhn}")
        return buf, False # Кэш — это реальная картинка
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://steamcommunity.com/market/",
        "Accept": "image/webp,image/apng,image/*,*/*;q=0.8"
    }
    for attempt in range(max_retries):
        try:
            r_img = session.get(image_url, headers=headers, timeout=15, allow_redirects=True)
            if r_img.status_code == 200:
                buf = BytesIO(r_img.content)
                # Кэшируем как base64
                buf.seek(0)
                img_data = base64.b64encode(buf.read()).decode('utf-8')
                image_cache[cache_key] = {"data": img_data, "timestamp": now_ts}
                if len(image_cache) > 100:
                    oldest = min(image_cache, key=lambda k: image_cache[k]["timestamp"])
                    del image_cache[oldest]
                save_image_cache(image_cache)
                logger.info(f"Real image fetched OK (attempt {attempt+1}) for {mhn}")
                buf.seek(0)
                return buf, False # Реальная картинка
            else:
                logger.warning(f"Image fetch failed {r_img.status_code} (attempt {attempt+1}) for {mhn}: {image_url}")
        except Exception as e:
            logger.warning(f"Image fetch error (attempt {attempt+1}): {e} for {mhn}")
        if attempt < max_retries - 1:
            time.sleep(2 + random.uniform(0, 3))
    # Если все ретраи fail — placeholder
    logger.warning(f"All retries failed for {mhn}, using placeholder")
    is_placeholder = True
    return create_placeholder_image(mhn), is_placeholder
def create_placeholder_image(mhn: str) -> BytesIO:
    """Генерируем placeholder: тёмный фон + текст скина + логотип."""
    try:
        buf = BytesIO()
        img = Image.new('RGB', (360, 360), color='#1b2838') # Steam-размер
        draw = ImageDraw.Draw(img)
        # Текст скина
        try:
            font = ImageFont.truetype("arial.ttf", 20) # Или любой шрифт
        except:
            font = ImageFont.load_default()
        text = mhn[:30] + "..." if len(mhn) > 30 else mhn # Обрезаем
        draw.text((10, 150), text, fill='#ccc', font=font)
        draw.text((10, 180), "No image available", fill='#888', font=font)
        # Логотип (маленький)
        if os.path.exists(LOGO_PATH):
            logo = Image.open(LOGO_PATH).convert("RGBA")
            logo_size = 60
            logo.thumbnail((logo_size, logo_size), Image.Resampling.LANCZOS)
            img.paste(logo, (250, 10), logo) # Top-right
        img.save(buf, format='PNG')
        buf.seek(0)
        logger.info(f"Placeholder created for {mhn}")
        return buf
    except Exception as e:
        logger.error(f"Placeholder error: {e}")
        return create_empty_buf() # Ultimate fallback
# ===================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====================
def parse_price_text(s):
    if isinstance(s, (int, float)):
        return float(s)
    if not isinstance(s, str):
        return 0.0
    s = s.replace("$", "").replace("USD", "").replace("\xa0", "").replace("руб.", "").replace("₽", "").replace(",", ".").replace(" ", "")
    try:
        return float(s)
    except ValueError:
        return 0.0
def format_usd(price: float) -> str:
    if price == 0:
        return "$0.00"
    return f"${price:.2f}"
def format_rub(price: float) -> str:
    if price == 0:
        return "0,00₽"
    int_part = f"{int(price):,}".replace(",", " ")
    dec_part = f"{price:.2f}".split(".")[1]
    return f"{int_part},{dec_part}₽"
def parse_volume(s):
    if isinstance(s, int):
        return s
    if not isinstance(s, str):
        return 0
    s = s.replace(",", "").replace(" ", "").replace("\xa0", "").replace(" ", "") # Добавлен вариант nbsp
    s = re.sub(r'[^\d]', '', s)
    try:
        return int(s) if s else 0
    except ValueError:
        return 0
def df_from_pricehistory(prices_raw, usd_rate: float = USD_RATE):
    rows = []
    now = datetime.now(tz=TZ)
    cutoff_date = now - timedelta(days=HISTORY_DAYS)
    for i in range(len(prices_raw) - 1, -1, -1):
        p = prices_raw[i]
        try:
            date_raw, price_str, volume_str = p
            date_raw = re.sub(r' \+\d+$', '', date_raw).strip()
            if ':' in date_raw:
                date_raw = date_raw.rstrip(':').strip()
            parts = date_raw.split()
            if len(parts) >= 4 and len(parts[-1]) == 2:
                date_raw = ' '.join(parts[:-1]) + ' ' + parts[-1] + ':00'
            price = parse_price_text(price_str)
            volume = parse_volume(volume_str)
            dt = pd.to_datetime(date_raw, utc=True, dayfirst=False, errors='coerce')
            if pd.isna(dt):
                continue
            dt = dt.tz_convert('Europe/Moscow')
            if dt < cutoff_date:
                break
            rows.append({"timestamp": dt, "price_usd": price, "volume": volume})
        except Exception:
            continue
    rows.reverse()
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("timestamp").reset_index(drop=True)
        df["price_rub"] = df["price_usd"] * usd_rate
    return df
def make_serializable(obj):
    if isinstance(obj, dict):
        return {k: make_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_serializable(i) for i in obj]
    elif isinstance(obj, np.generic):
        if np.iscomplexobj(obj):
            return complex(obj)
        else:
            return obj.item()
    return obj
def analyze_dataframe(df: pd.DataFrame, current_median: float, current_volume: int):
    if df.empty or len(df) < 2:
        return {
            "volatility": 0.0,
            "price_growth": 0.0,
            "volume_growth": 0.0,
            "breakout_percentage": 0.0,
            "range_breakout": 0.0,
            "is_sideways": False,
            "range_percent": 0.0
        }
    prices = df["price_usd"].values
    avg_price = float(prices.mean())
    stdev_price = float(prices.std(ddof=0))
    volatility = (stdev_price / avg_price * 100) if avg_price > 0 else 0.0
    now = datetime.now(tz=TZ)
    last_24h_start = now - timedelta(hours=24)
    prev_24h_start = now - timedelta(hours=48)
    prev_24h_end = last_24h_start
    last_24h = df[df["timestamp"] >= last_24h_start]
    # Prev price: цена closest to exactly 24h ago
    exact_24h_ago = now - timedelta(hours=24)
    # Filter to prev period for safety
    prev_candidates = df[(df["timestamp"] >= prev_24h_start) & (df["timestamp"] < prev_24h_end)]
    if not prev_candidates.empty:
        closest_idx = (prev_candidates["timestamp"] - exact_24h_ago).abs().idxmin()
        prev_price = prev_candidates.loc[closest_idx, "price_usd"]
    else:
        prev_price = avg_price
    price_growth = ((current_median - prev_price) / prev_price * 100) if prev_price > 0 else 0.0
    prev_volume = prev_candidates["volume"].sum() if not prev_candidates.empty else 0
    volume_growth = 0.0
    if prev_volume > 0:
        volume_growth = ((current_volume - prev_volume) / prev_volume * 100)
    elif prev_volume == 0 and current_volume > 0:
        volume_growth = 100.0
    elif current_volume == 0:
        volume_growth = -100.0
    week_start = now - timedelta(days=HISTORY_DAYS)
    week_df = df[df["timestamp"] >= week_start]
    max_price_week = week_df["price_usd"].max() if not week_df.empty else 0.0
    min_price_week = week_df["price_usd"].min() if not week_df.empty else 0.0
    breakout_percentage = ((current_median - max_price_week) / max_price_week * 100) if max_price_week > 0 else 0.0
    range_percent = ((max_price_week - min_price_week) / min_price_week * 100) if min_price_week > 0 else 0.0
    is_sideways = bool(range_percent < 20.0)
    range_breakout = 0.0
    if is_sideways and min_price_week > 0:
        upper_bound = max_price_week * 1.10
        lower_bound = min_price_week * 0.90
        if current_median > upper_bound:
            range_breakout = ((current_median - max_price_week) / max_price_week * 100)
        elif current_median < lower_bound:
            range_breakout = ((current_median - min_price_week) / min_price_week * 100) * -1
    return {
        "volatility": round(volatility, 2),
        "price_growth": round(price_growth, 2),
        "volume_growth": round(volume_growth, 2),
        "breakout_percentage": round(breakout_percentage, 2),
        "range_breakout": round(abs(range_breakout), 2),
        "is_sideways": is_sideways,
        "range_percent": round(range_percent, 2)
    }
def clean_tag(n: str) -> str:
    """Очистка строки для хэштега: удаляем все кроме букв, цифр, и приводим к нижнему регистру."""
    cleaned = re.sub(r'[^a-zа-яё0-9]', '', n.lower())
    return cleaned
def extract_skin_type(mhn: str) -> str:
    """Извлекает тип скина (модель оружия/ножа) из market_hash_name."""
    name_lower = mhn.lower()
    parts = name_lower.split('|')
    if not parts:
        return ""
    first_part = parts[0].strip()
    first_part = first_part.replace('★', '').strip()
    words = first_part.split()
    if words:
        candidate = words[-1].strip()
        # Очистка от пунктуации
        candidate = re.sub(r'[^\wа-яё]', '', candidate)
        return candidate
    return ""
def get_item_type_and_hashtags(market_hash_name: str, item: dict) -> tuple[str, str]:
    hashtags = ""
    name_lower = market_hash_name.lower()
    weapon_dict = item.get('weapon', {})
    weapon = weapon_dict.get('name', '') if isinstance(weapon_dict, dict) else str(item.get('weapon', ''))
    category_dict = item.get('category', {})
    cat_name = category_dict.get('name', '').lower() if isinstance(category_dict, dict) else str(item.get('category', '')).lower()
    pattern_dict = item.get('pattern', {})
    skin_name = pattern_dict.get('name', '') if isinstance(pattern_dict, dict) else str(item.get('pattern', ''))
    if weapon:
        cat = 'other'
        for c, keywords in CATEGORY_KEYWORDS.items():
            if any(kw in cat_name for kw in keywords):
                cat = c
                break
        cat_tag = CATEGORY_HASHTAGS.get(cat, '#другое')
        weapon_tag = clean_tag(weapon)
        skin_tag = clean_tag(skin_name)
        hashtags = f"{cat_tag} #{weapon_tag} #{skin_tag}"
        return "weapon", hashtags
    if "sticker" in name_lower or "наклейка" in name_lower:
        hashtags = "#наклейка"
        # Новый подход: парсим market_hash_name по "|"
        parts = [part.strip() for part in market_hash_name.split("|")]
        if len(parts) >= 3:
            # Первая часть обычно "Наклейка" — пропускаем
            team_player_part = parts[1] # "TeSeS (с блёстками)"
            tournament_part = parts[2] # "Шанхай-2024"
     
            # Очистка и хэштегизация для команды/игрока
            team_player_tag = clean_tag(team_player_part)
            # Проверяем маппинг для команды, если возможно
            team_name_lower = team_player_part.lower()
            mapped_team = None
            for eng, tag in TEAM_MAP.items():
                if eng in team_name_lower:
                    mapped_team = tag
                    break
            if mapped_team:
                team_player_tag = mapped_team
            if team_player_tag:
                hashtags += f" #{team_player_tag}"
     
            # Очистка и хэштегизация для турнира
            tour_tag = clean_tag(tournament_part)
            # Проверяем маппинг для турнира
            tour_name_lower = tournament_part.lower()
            mapped_tour = None
            for eng, rus in TOURNAMENT_MAP.items():
                if eng in tour_name_lower:
                    mapped_tour = rus
                    break
            if mapped_tour:
                tour_tag = mapped_tour
            if tour_tag:
                hashtags += f" #{tour_tag}"
        # Fallback: если парсинг не сработал, используем старый метод из item
        else:
            if item.get('player'):
                player_code = item['player'].get('code', '').lower()
                if player_code:
                    hashtags += f" #{player_code}"
            elif item.get('team'):
                team_name_lower = item['team'].get('name', '').lower()
                team_tag = None
                for eng, tag in TEAM_MAP.items():
                    if eng in team_name_lower:
                        team_tag = tag
                        break
                if not team_tag:
                    team_tag = clean_tag(team_name_lower)
                if team_tag:
                    hashtags += f" #{team_tag}"
   
            tournament = item.get('tournament', {})
            if tournament:
                name_lower = tournament.get('name', '').lower()
                tour_tag = None
                for eng, rus in TOURNAMENT_MAP.items():
                    if eng in name_lower:
                        tour_tag = rus
                        break
                if not tour_tag:
                    tour_tag = clean_tag(name_lower)
                if tour_tag:
                    hashtags += f" #{tour_tag}"
        return "sticker", hashtags
    other_types = {
        "container": "#контейнер",
        "case": "#кейс",
        "agent": "#агент",
        "glove": "#перчатки",
        "charm": "#брелок",
        "music": "#музыка",
        "patch": "#патч",
        "graffiti": "#граффити"
    }
    name_lower = market_hash_name.lower()
    for otype, tag in other_types.items():
        if otype in name_lower:
            return otype, tag
    return "other", "#другое"
def is_similar_to_recently_posted(mhn: str, posted_log: List[str], posted_history: List[Dict], similarity_threshold: int = 3):
    item_type, _ = get_item_type_and_hashtags(mhn, {})
    skin = extract_skin_type(mhn)
    similar_count = 0
    for post in posted_history[-similarity_threshold:]:
        if not isinstance(post, dict) or post.get("type") != item_type:
            continue
        post_skin = post.get("skin", "")
        if skin and post_skin == skin:
            similar_count += 1
        # Удален elif, так как логика на основе skin_type теперь покрывает типы (включая ножи)
    return similar_count >= 1
def item_passes_criteria(item: dict, posted_log: List[str], posted_history: List[Dict]) -> tuple[bool, str]:
    if item.get("price_usd", 0) < MIN_PRICE:
        return False, f"price < {MIN_PRICE}"
    if item.get("volume_24h", 0) < MIN_VOLUME_24H:
        return False, f"volume < {MIN_VOLUME_24H}"
    growth = item.get("growth", 0)
    if abs(growth) < PRICE_CHANGE_THRESHOLD:
        return False, f"price change < {PRICE_CHANGE_THRESHOLD}%"
    mhn = item.get("market_hash_name", "")
    if mhn in posted_log or is_similar_to_recently_posted(mhn, posted_log, posted_history):
        return False, "similar to recently posted"
    if item.get("is_sideways", False) and item.get("range_breakout", 0) >= 10.0:
        return True, "range breakout from sideways"
    if item.get("breakout_percentage", 0) >= BREAKOUT_THRESHOLD:
        return True, "breakout threshold"
    if item.get("volatility", 0) > VOLATILITY_THRESHOLD or abs(growth) >= PRICE_CHANGE_THRESHOLD:
        return True, "volatility or price change"
    return False, "no criteria met"
def create_empty_buf():
    buf = BytesIO()
    try:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.set_facecolor('#1b2838')
        fig.patch.set_facecolor('#1b2838')
        ax.grid(True, linestyle='--', alpha=0.2, color='#555')
        ax.tick_params(axis='x', colors='#ccc', labelrotation=45)
        ax.tick_params(axis='y', colors='#ccc')
        fig.savefig(buf, format="png", dpi=80, bbox_inches="tight", facecolor='#1b2838')
        plt.close(fig)
    except Exception as e:
        logger.warning(f"Error creating empty buf: {e}")
    buf.seek(0)
    return buf
def russian_month_formatter(x, pos):
    dt = mdates.num2date(x)
    months = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
    return f"{dt.day:02d} {months[dt.month - 1]}"
def overlay_logo_on_image(buf: BytesIO, position: str = "top_right", opacity: float = 1.0, is_graph: bool = False):
    try:
        img = Image.open(buf).convert("RGBA")
        if not os.path.exists(LOGO_PATH):
            new_buf = BytesIO()
            img.convert("RGB").save(new_buf, format='PNG')
            new_buf.seek(0)
            return new_buf
        logo = Image.open(LOGO_PATH).convert("RGBA")
        logo_size = int(img.width * 0.1)
        logo.thumbnail((logo_size, logo_size), Image.Resampling.LANCZOS)
        alpha = logo.split()[-1]
        alpha = ImageEnhance.Brightness(alpha).enhance(opacity)
        logo.putalpha(alpha)
        if is_graph:
            pos_x = (img.width - logo.width) // 2
            pos_y = (img.height - logo.height) // 2
        else:
            pos_x = img.width - logo.width - 10
            pos_y = 10
        img.paste(logo, (pos_x, pos_y), logo)
        new_buf = BytesIO()
        img.convert("RGB").save(new_buf, format='PNG')
        new_buf.seek(0)
        return new_buf
    except Exception as e:
        logger.warning(f"Error overlaying logo: {e}")
        new_buf = BytesIO()
        img = Image.open(buf).convert("RGB")
        img.save(new_buf, format='PNG')
        new_buf.seek(0)
        return new_buf
def plot_price_week(df: pd.DataFrame, title: str):
    if df.empty:
        return create_empty_buf()
    now = datetime.now(tz=TZ)
    week_df = df[df["timestamp"] >= (now - timedelta(days=HISTORY_DAYS))]
    if week_df.empty:
        week_df = df.copy()
    try:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.plot(week_df["timestamp"], week_df["price_rub"], linestyle='-', color='#00a1d6', linewidth=1.5)
        ax.set_title(title, fontsize=10, color='#fff', pad=10)
        ax.set_xlabel("Дата", fontsize=8, color='#ccc')
        ax.set_ylabel("Цена (₽)", fontsize=8, color='#ccc')
        ax.grid(True, linestyle='--', alpha=0.2, color='#555')
        ax.tick_params(axis='x', colors='#ccc', labelrotation=45)
        ax.tick_params(axis='y', colors='#ccc')
        ax.xaxis.set_major_formatter(FuncFormatter(russian_month_formatter))
        fig.patch.set_facecolor('#1b2838')
        ax.set_facecolor('#1b2838')
        plt.tight_layout()
        buf = BytesIO()
        fig.savefig(buf, format="png", dpi=80, facecolor='#1b2838', edgecolor='none')
        plt.close(fig)
        buf.seek(0)
        return overlay_logo_on_image(buf, "center", LOGO_OPACITY, is_graph=True)
    except Exception as e:
        logger.warning(f"Error plotting price: {e}")
        return create_empty_buf()
def plot_volume_week(df: pd.DataFrame, title: str):
    if df.empty:
        return create_empty_buf()
    now = datetime.now(tz=TZ)
    week_df = df[df["timestamp"] >= (now - timedelta(days=HISTORY_DAYS))]
    if week_df.empty:
        week_df = df.copy()
    try:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.plot(week_df["timestamp"], week_df["volume"], linestyle='-', color='#00a1d6', linewidth=1.5)
        ax.set_title(title, fontsize=10, color='#fff', pad=10)
        ax.set_xlabel("Дата", fontsize=8, color='#ccc')
        ax.set_ylabel("Объём", fontsize=8, color='#ccc')
        ax.grid(True, linestyle='--', alpha=0.2, color='#555')
        ax.tick_params(axis='x', colors='#ccc', labelrotation=45)
        ax.tick_params(axis='y', colors='#ccc')
        ax.xaxis.set_major_formatter(FuncFormatter(russian_month_formatter))
        fig.patch.set_facecolor('#1b2838')
        ax.set_facecolor('#1b2838')
        plt.tight_layout()
        buf = BytesIO()
        fig.savefig(buf, format="png", dpi=80, facecolor='#1b2838', edgecolor='none')
        plt.close(fig)
        buf.seek(0)
        return overlay_logo_on_image(buf, "center", LOGO_OPACITY, is_graph=True)
    except Exception as e:
        logger.warning(f"Error plotting volume: {e}")
        return create_empty_buf()
def build_plots(item: dict, days: int = HISTORY_DAYS) -> tuple[BytesIO, BytesIO, BytesIO]:
    df = df_from_pricehistory(item.get("history_raw", []), item.get("usd_rate", USD_RATE))
    price_buf = plot_price_week(df, f"Динамика цены за {days} дней — {item['name']}")
    volume_buf = plot_volume_week(df, f"Объём продаж за {days} дней — {item['name']}")
    histogram = item.get("histogram")
    if not histogram or not histogram.get("buy_order_graph") or not histogram.get("sell_order_graph"):
        order_buf = create_empty_buf()
    else:
        try:
            fig_order, ax_order = plt.subplots(figsize=(8, 3))
            fig_order.patch.set_facecolor('#1b2838')
            ax_order.set_facecolor('#1b2838')
            ax_order.set_title(f"Книга ордеров — {item['name']}", fontsize=10, color='#fff', pad=10)
   
            buy_graph = histogram["buy_order_graph"]
            buy_prices = [row[0] for row in buy_graph]
            buy_cumuls = [row[1] for row in buy_graph]
            ax_order.step(buy_prices, buy_cumuls, where='post', color='#00FF00', linewidth=1.5, label='Запросов на покупку (Зелёный)')
   
            sell_graph = histogram["sell_order_graph"]
            sell_prices = [row[0] for row in sell_graph]
            sell_cumuls = [row[1] for row in sell_graph]
            ax_order.step(sell_prices, sell_cumuls, where='post', color='#FF0000', linewidth=1.5, label='Лотов на продажу (Красный)')
   
            ax_order.set_ylabel("Количество", fontsize=8, color='#ccc')
            ax_order.set_xlabel("Цена (₽)", fontsize=8, color='#ccc')
            ax_order.grid(True, linestyle="--", alpha=0.2, color='#555')
            ax_order.tick_params(colors='#ccc')
            ax_order.tick_params(axis='x', labelrotation=45)
            if "graph_min_x" in histogram and "graph_max_x" in histogram:
                ax_order.set_xlim(histogram["graph_min_x"], histogram["graph_max_x"])
            if "graph_max_y" in histogram:
                ax_order.set_ylim(0, histogram["graph_max_y"] * 1.2)
            ax_order.legend(loc='upper left', fontsize=8, facecolor='#1b2838', edgecolor='#ccc', labelcolor='#ccc')
            order_buf = BytesIO()
            fig_order.savefig(order_buf, format="png", dpi=80, facecolor='#1b2838', edgecolor='none')
            plt.close(fig_order)
            order_buf.seek(0)
            order_buf = overlay_logo_on_image(order_buf, "center", LOGO_OPACITY, is_graph=True)
        except Exception as e:
            logger.warning(f"Error building order plot: {e}")
            order_buf = create_empty_buf()
    return price_buf, volume_buf, order_buf
def extract_steam_url_from_text(text: str) -> Optional[str]:
    match = re.search(r'<a href="([^"]+)"', text)
    if match and "steamcommunity.com/market" in match.group(1):
        return match.group(1)
    return None
def send_media_group_telegram(media_files, caption="", message_id=None):
    if not media_files:
        return None
    send_url = f"https://api.telegram.org/bot{TOKEN}/sendMediaGroup"
    media_list = []
    group_files = {}
    for i, (file_type, file_data) in enumerate(media_files):
        group_files[f"{file_type}{i}"] = file_data
        media_item = {"type": "photo", "media": f"attach://{file_type}{i}"}
        if i == 0 and caption:
            media_item["caption"] = caption
            media_item["parse_mode"] = "HTML"
        media_list.append(media_item)
    group_payload = {
        "chat_id": CHAT_ID,
        "media": json.dumps(media_list)
    }
    max_retries = 2
    for retry in range(max_retries + 1):
        try:
            r_group = session.post(send_url, data=group_payload, files=group_files, timeout=20)
            if r_group.status_code == 200 and r_group.json().get("ok"):
                result = r_group.json()["result"]
                if isinstance(result, list) and len(result) > 0:
                    return result[0]["message_id"]
            elif r_group.status_code == 429:
                try:
                    retry_after = r_group.json()["parameters"]["retry_after"]
                    print(f"[WARN] Telegram 429, sleeping {retry_after + 1}s (retry {retry + 1}/{max_retries})")
                    time.sleep(retry_after + random.uniform(1, 3))
                except KeyError:
                    time.sleep(10) # Default backoff
                continue
            else:
                logger.warning(f"sendMediaGroup failed: {r_group.text}")
                break
        except Exception as e:
            logger.warning(f"Error in sendMediaGroup: {e}")
            time.sleep(5)
            continue
    return None
def send_message_telegram(text: str, message_id=None) -> Optional[int]:
    send_url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true"
    }
    try:
        r = session.post(send_url, data=payload, timeout=12)
        if r.status_code == 200 and r.json().get("ok"):
            return r.json()["result"]["message_id"]
        else:
            logger.warning(f"sendMessage failed: {r.text if r else 'No response'}")
            return None
    except Exception as e:
        logger.error(f"Error in sendMessage: {e}")
        return None
def send_maintenance_message():
    send_message_telegram(MAINTENANCE_MESSAGE)
def load_posted_history() -> List[Dict]:
    if os.path.exists(POSTED_HISTORY_FILE):
        try:
            with open(POSTED_HISTORY_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Валидация: фильтр валидных записей
                valid_history = [h for h in data if isinstance(h, dict) and "message_id" in h and "mhn" in h]
                return valid_history
        except Exception as e:
            logger.error(f"Error loading posted history: {e}")
            pass
    return []
def save_posted_history(message_id: int, data: Dict):
    try:
        history = load_posted_history()
        mhn = data.get("mhn", "")
        item_type, _ = get_item_type_and_hashtags(mhn, {})
        skin = extract_skin_type(mhn)
        history.append({
            "message_id": message_id,
            "mhn": mhn,
            "type": item_type,
            "skin": skin,
            "timestamp": data.get("timestamp", time.time())
        })
        now_ts = time.time()
        history = [h for h in history if now_ts - h["timestamp"] < 86400]
        with open(POSTED_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error saving posted history: {e}")
def get_similar_posts(mhn: str) -> List[str]:
    history = load_posted_history()
    item_type, _ = get_item_type_and_hashtags(mhn, {})
    skin = extract_skin_type(mhn)
    similar = []
    for post in history[-10:]:
        if len(similar) >= 3:
            break
        if post.get("type") != item_type:
            continue
        post_skin = post.get("skin", "")
        if skin and post_skin == skin:
            similar.append(f'<a href="https://t.me/c/{CHAT_ID[4:]}/{post["message_id"]}">{html.escape(post["mhn"])}</a>')
    return similar
def load_posted_log() -> List[str]:
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
                else:
                    return []
        except Exception as e:
            logger.error(f"Error loading posted log: {e}")
            pass
    return []
def save_posted_log(log: List[str]):
    try:
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            json.dump(log, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error saving posted log: {e}")
def unpin_and_pin_summary(summary_message_id: int):
    try:
        unpin_url = f"https://api.telegram.org/bot{TOKEN}/unpinChatMessage"
        unpin_payload = {"chat_id": CHAT_ID}
        session.post(unpin_url, data=unpin_payload, timeout=10)
        time.sleep(1)
        pin_url = f"https://api.telegram.org/bot{TOKEN}/pinChatMessage"
        pin_payload = {"chat_id": CHAT_ID, "message_id": summary_message_id, "disable_notification": True}
        session.post(pin_url, data=pin_payload, timeout=10)
    except Exception as e:
        logger.warning(f"Error pinning summary: {e}")
# ИЗМЕНЕНО: Улучшена функция для summary — добавлено больше логирования, fallback для пустых изображений, водяной знак с opacity=0.8
# ИЗМЕНЕНО: Для итогов только текст, без изображений
def generate_daily_summary(posted_items: List[Dict], posted_log: List[str]):
    logger.info(f"Generating summary with {len(posted_items)} posted items from last 24h")
    if not posted_items:
        logger.warning("No posted items found for summary - check if publications occurred in last 24h")
        combined_caption = "🕐 ИТОГИ за 24 часа:\n\n🟢 ТОП-5 предметов по росту:\n\nНет предметов с ростом цены за последние 24 часа.\n\n🔴 ТОП-5 предметов по падению:\n\nНет предметов с падением цены за последние 24 часа.\n"
        return combined_caption, [] # Только текст
    top_growth = sorted(
        [item for item in posted_items if item.get("growth", 0) > 0],
        key=lambda x: x.get("growth", 0),
        reverse=True
    )[:5]
    top_decline = sorted(
        [item for item in posted_items if item.get("growth", 0) < 0],
        key=lambda x: x.get("growth", 0)
    )[:5]
    logger.info(f"Top growth items: {len(top_growth)}, Top decline: {len(top_decline)}")
    growth_text = "🟢 ТОП-5 предметов по росту:\n\n"
    for i, item in enumerate(top_growth, 1):
        steam_url = f"https://steamcommunity.com/market/listings/{APPID}/{quote(item.get('market_hash_name', ''), safe='')}"
        growth_text += (
            f"{i}️⃣ <a href=\"{steam_url}\">{html.escape(item.get('name', ''))}</a>\n"
            f" ﹂Цена: {format_rub(item.get('price_rub', 0))} | Волатильность: {item.get('volatility', 0):.2f}% | Изменение цены: +{item.get('growth', 0):.2f}%\n\n"
        )
    if not top_growth:
        growth_text += "Нет предметов с ростом цены за последние 24 часа.\n"
    decline_text = "🔴 ТОП-5 предметов по падению:\n\n"
    for i, item in enumerate(top_decline, 1):
        steam_url = f"https://steamcommunity.com/market/listings/{APPID}/{quote(item.get('market_hash_name', ''), safe='')}"
        decline_text += (
            f"{i}️⃣ <a href=\"{steam_url}\">{html.escape(item.get('name', ''))}</a>\n"
            f" ﹂Цена: {format_rub(item.get('price_rub', 0))} | Волатильность: {item.get('volatility', 0):.2f}% | Изменение цены: {item.get('growth', 0):.2f}%\n\n"
        )
    if not top_decline:
        decline_text += "Нет предметов с падением цены за последние 24 часа.\n"
    combined_caption = f"🕐 ИТОГИ за 24 часа:\n\n{growth_text}\n{decline_text}"
    # ИЗМЕНЕНО: Только текст, без media
    return combined_caption, []
def send_summary_parts(combined_caption, combined_media):
    sent = False
    sent_id = None
    # ИЗМЕНЕНО: Только текст для итогов
    sent_id = send_message_telegram(combined_caption)
    if sent_id:
        sent = True
        notify_monitor_bot("summary_sent", f"Summary posted at {datetime.now().isoformat()}")
        unpin_and_pin_summary(sent_id)
        print(f"[INFO] Summary pinned: message_id={sent_id}")
        logger.info(f"Summary sent successfully: message_id={sent_id}")
    else:
        print("[WARN] Summary send failed")
        logger.warning("Summary send failed")
    return sent, sent_id
# ===================== MAIN =====================
def main():
    global USD_RATE, USE_PROXY, image_cache
    parser = argparse.ArgumentParser(description="CSGO Market Analyzer")
    parser.add_argument('--send-summary', action='store_true', help="Send daily summary of top growth and decline items")
    parser.add_argument('--summary-time', type=str, default=DEFAULT_SUMMARY_TIME, help="Time to send summary in HH:MM format")
    args = parser.parse_args()
    if args.send_summary:
        posted_log = load_posted_log()
        posted_items = load_posted_items(for_summary=True)
        combined_caption, combined_media = generate_daily_summary(posted_items, posted_log)
        sent, sent_id = send_summary_parts(combined_caption, combined_media)
        if sent:
            now_local = datetime.now(tz=TZ)
            save_last_summary(now_local)
            posted_log = []
            save_posted_log(posted_log)
            with open(POSTED_ITEMS_FILE, "w", encoding="utf-8") as f:
                json.dump([], f)
            with open(POSTED_HISTORY_FILE, "w", encoding="utf-8") as f:
                json.dump([], f)
            if os.path.exists(OUT_DIR):
                shutil.rmtree(OUT_DIR, ignore_errors=True)
            os.makedirs(OUT_DIR, exist_ok=True)
            clear_cache() # ИЗМЕНЕНО: Добавлена очистка кэша
            clear_image_cache() # Очистка кэша изображений
            logger.info("Manual summary sent and cleanup completed")
        return
    log_event("startup", "Bot started")
    USD_RATE = get_usd_to_rub_rate()
    try:
        summary_time = datetime.strptime(args.summary_time, "%H:%M").time()
    except ValueError:
        summary_time = datetime.strptime(DEFAULT_SUMMARY_TIME, "%H:%M").time()
    last_summary_sent = load_last_summary()
    if last_summary_sent:
        log_event("summary_load", f"Last summary sent on: {last_summary_sent}")
    else:
        log_event("summary_load", "No previous summary found")
    if USE_PROXY:
        try:
            r = session.get("https://api.ipify.org?format=json", timeout=10)
            if r.status_code != 200:
                raise Exception("Proxy test failed")
            print(f"Proxy IP: {r.json()['ip']}")
            r2 = session.get(BYMYKEL_URL, timeout=10)
            if r2.status_code != 200:
                raise Exception("Proxy can't reach API")
        except Exception as e:
            print(f"Proxy failed ({e}), disabling...")
            USE_PROXY = False
            disable_proxy()
    else:
        disable_proxy()
    items_raw = load_items()
    valid_items = get_valid_items(items_raw)
    total_items = len(valid_items)
    log_event("items_loaded", f"Loaded {total_items} items")
    if total_items == 0:
        log_event("no_items", "No valid items loaded. Skipping scan cycle.")
        time.sleep(3600)
        return main()
    random.shuffle(valid_items)
    log_event("shuffle_done", "Shuffled items list")
    posted_log = load_posted_log()
    posted_history = load_posted_history()
    item_cache = load_cache()
    image_cache = load_image_cache() # Загрузка кэша изображений
    consecutive_errors = 0
    while True:
        now_local = datetime.now(tz=TZ)
        target_time = now_local.replace(hour=summary_time.hour, minute=summary_time.minute, second=0, microsecond=0)
        if target_time <= now_local:
            target_time += timedelta(days=1)
        time_to_target = (target_time - now_local).total_seconds()
        minutes_to_target = time_to_target / 60
        print(f"[INFO] Time until top items summary post: {minutes_to_target:.1f} minutes ({time_to_target:.0f} seconds)")
        log_event("time_to_summary", f"Remaining time to top items post: {minutes_to_target:.1f} min")
        summary_sent_in_24h = False
        if last_summary_sent:
            time_diff = (now_local - last_summary_sent).total_seconds()
            try:
                with open(SUMMARY_LOG, "r", encoding="utf-8") as f:
                    data = json.load(f)
                saved_time_str = data.get('summary_time', DEFAULT_SUMMARY_TIME)
                saved_time = datetime.strptime(saved_time_str, "%H:%M").time()
                if saved_time == summary_time and time_diff < 24 * 3600:
                    summary_sent_in_24h = True
                    print(f"[INFO] Summary already sent in last 24h, skipping until next cycle")
                    log_event("summary_skipped_24h", "Summary skipped due to 24h limit")
            except Exception as e:
                logger.warning(f"Error checking summary 24h: {e}")
        prep_time_to_target = time_to_target - (SUMMARY_PREP_MINUTES * 60)
        is_prep_mode = (prep_time_to_target <= 0 and time_to_target > 0)
        if is_prep_mode:
            prep_minutes = time_to_target / 60
            print(f"[INFO] Entering prep mode for top items summary (remaining: {prep_minutes:.1f} min)")
            log_event("prep_mode", f"Entering prep mode for top items summary (remaining: {prep_minutes:.1f} min)")
            # Incremental sleep: check every 30s
            while time_to_target > 0:
                time.sleep(min(30, time_to_target))
                now_local = datetime.now(tz=TZ)
                target_time = now_local.replace(hour=summary_time.hour, minute=summary_time.minute, second=0, microsecond=0)
                if target_time <= now_local:
                    target_time += timedelta(days=1)
                time_to_target = (target_time - now_local).total_seconds()
            is_prep_mode = False
        if time_to_target <= 0 and not summary_sent_in_24h:
            print("[INFO] Time reached! Generating and sending top items summary")
            log_event("summary_time_reached", "Exact time for top items summary reached")
            posted_items = load_posted_items(for_summary=True)
            combined_caption, combined_media = generate_daily_summary(posted_items, posted_log)
   
            sent, sent_id = send_summary_parts(combined_caption, combined_media)
   
            if sent:
                print("[INFO] Full cleanup after top items summary sent")
                log_event("summary_sent_success", "Top items summary sent successfully")
                save_last_summary(now_local)
                try:
                    with open(SUMMARY_LOG, "r+", encoding="utf-8") as f:
                        data = json.load(f)
                        data['summary_time'] = args.summary_time
                        f.seek(0)
                        json.dump(data, f, ensure_ascii=False, indent=2)
                        f.truncate()
                except Exception as e:
                    logger.warning(f"Error updating summary time: {e}")
                last_summary_sent = now_local
                posted_log = []
                save_posted_log(posted_log)
                with open(POSTED_ITEMS_FILE, "w", encoding="utf-8") as f:
                    json.dump([], f)
                with open(POSTED_HISTORY_FILE, "w", encoding="utf-8") as f:
                    json.dump([], f)
                if os.path.exists(OUT_DIR):
                    shutil.rmtree(OUT_DIR, ignore_errors=True)
                os.makedirs(OUT_DIR, exist_ok=True)
                clear_cache() # ИЗМЕНЕНО: Очистка кэша после summary
                clear_image_cache() # Очистка кэша изображений
                print("[INFO] Cleanup done: logs, posted_items, history, OUT_DIR, cache")
                log_event("cleanup_done", "Cleanup after top items summary completed")
            else:
                print("[WARN] Top items summary send failed, no cleanup")
                log_event("summary_send_failed", "Top items summary send failed, no cleanup")
            time.sleep(REQUEST_DELAY + random.random() * JITTER)
            continue
        log_event("scan_cycle_start", "Starting new scan cycle")
        item_counter = 0
        for item in valid_items:
            # Recalculate time inside loop to catch summary during scan
            now_local = datetime.now(tz=TZ)
            target_time = now_local.replace(hour=summary_time.hour, minute=summary_time.minute, second=0, microsecond=0)
            if target_time <= now_local:
                target_time += timedelta(days=1)
            time_to_target = (target_time - now_local).total_seconds()
            print(f"[DEBUG] time_to_target inside loop: {time_to_target:.0f}s") # Remove in prod
   
            if time_to_target <= 60 and not summary_sent_in_24h:
                print("[INFO] Summary time hit during scan! Interrupting for summary.")
                log_event("summary_interrupt", "Summary time during scan, interrupting")
                # Send summary here (code same as above)
                posted_items = load_posted_items(for_summary=True)
                combined_caption, combined_media = generate_daily_summary(posted_items, posted_log)
                sent, sent_id = send_summary_parts(combined_caption, combined_media)
                if sent:
                    # Cleanup (same as above)
                    save_last_summary(now_local)
                    try:
                        with open(SUMMARY_LOG, "r+", encoding="utf-8") as f:
                            data = json.load(f)
                            data['summary_time'] = args.summary_time
                            f.seek(0)
                            json.dump(data, f, ensure_ascii=False, indent=2)
                            f.truncate()
                    except Exception as e:
                        logger.warning(f"Error updating summary time: {e}")
                    last_summary_sent = now_local
                    posted_log = []
                    save_posted_log(posted_log)
                    with open(POSTED_ITEMS_FILE, "w", encoding="utf-8") as f:
                        json.dump([], f)
                    with open(POSTED_HISTORY_FILE, "w", encoding="utf-8") as f:
                        json.dump([], f)
                    if os.path.exists(OUT_DIR):
                        shutil.rmtree(OUT_DIR, ignore_errors=True)
                    os.makedirs(OUT_DIR, exist_ok=True)
                    clear_cache()
                    clear_image_cache()
                    print("[INFO] Cleanup done after interrupt")
                    log_event("cleanup_after_interrupt", "Cleanup after summary interrupt")
                break
   
            # Prep mode check inside loop
            prep_time_to_target = time_to_target - (SUMMARY_PREP_MINUTES * 60)
            if prep_time_to_target <= 0 and time_to_target > 0:
                print(f"[INFO] Prep mode hit during scan (remaining: {time_to_target/60:.1f} min). Pausing scan.")
                log_event("prep_interrupt", "Prep mode during scan, pausing")
                print(f"[DEBUG] Sleeping {time_to_target:.0f}s until summary ({time_to_target/60:.1f} min)")
                log_event("prep_sleep", f"Sleeping {time_to_target/60:.1f} min until summary")
                time.sleep(time_to_target)
                break
   
            if is_prep_mode:
                log_event("prep_interrupt", "Interrupting analysis for top items summary prep")
                break
   
            try:
                item_counter += 1
                if item_counter % PROGRESS_INTERVAL == 0:
                    print_progress(item_counter, total_items)
                if item_counter % MONITOR_INTERVAL == 0:
                    print_resource_usage(item_counter, total_items)
                mhn = build_market_hash_name(item)
                if not mhn:
                    continue
                item["market_hash_name"] = mhn
                if mhn in posted_log:
                    continue
                log_event("item_processing", f"Processing item: {mhn}", mhn)
                time.sleep(REQUEST_DELAY + random.random() * JITTER)
                cache_key = hashlib.md5(mhn.encode()).hexdigest()
                now_ts = time.time()
                if cache_key in item_cache and (now_ts - item_cache[cache_key].get("timestamp", 0)) < CACHE_TTL:
                    data = item_cache[cache_key]["data"]
                    log_event("cache_hit", "Cache hit", mhn)
                else:
                    data = get_item_data(mhn)
                    if data:
                        item_cache[cache_key] = {"data": data, "timestamp": now_ts}
                        if len(item_cache) > 500:
                            oldest_key = min(item_cache, key=lambda k: item_cache[k]["timestamp"])
                            del item_cache[oldest_key]
                        save_cache(item_cache)
                        log_event("cache_miss", "Cache miss and fetched", mhn)
                    else:
                        log_event("data_fetch_failed", "Failed to fetch data", mhn)
                        continue
                raw_history = data["history"]
                if not raw_history:
                    log_event("no_history", "No history data", mhn)
                    continue
                quick_data = quick_parse_history(raw_history, USD_RATE)
                if not quick_data["prices"]:
                    log_event("empty_quick_data", "Empty quick parse data", mhn)
                    continue
                item["price_usd"] = data["price_usd"] if data["price_usd"] > 0 else quick_data["current_price_usd"]
                item["price_rub"] = item["price_usd"] * USD_RATE
                item["volume_24h"] = quick_data["volume_24h"]
                item["growth"] = quick_data["price_growth"]
                item["volume_change"] = quick_data["volume_growth"]
                item["history_raw"] = raw_history
                item["histogram"] = data.get("histogram")
                item["usd_rate"] = USD_RATE
                # === НОВАЯ ЛОГИКА ПОЛУЧЕНИЯ ИЗОБРАЖЕНИЯ ===
                # 1. Приоритет №1: прямая ссылка из локального JSON (item["image"]) — она всегда правильная и высокого качества
                primary_image_url = item.get("image", "").strip()
                if primary_image_url:
                    if primary_image_url.startswith("http"):
                        item["image_url"] = primary_image_url
                    else:
                        # Если это относительный путь (как в вашем JSON), добавляем базовый URL Steam
                        item["image_url"] = f"https://community.akamai.steamstatic.com/economy/image/{primary_image_url}"
                    logger.info(f"Using primary image from JSON for {mhn}: {item['image_url']}")
                else:
                    # 2. Fallback: изображение, спаршенное со страницы Steam
                    item["image_url"] = data.get("image_url", "") or ""
                    if not item["image_url"]:
                        logger.warning(f"No image URL from page for {mhn}, will try placeholder later")
                
                # === КОНЕЦ НОВОЙ ЛОГИКИ ===
                item["name"] = item.get("name", mhn)
                item["publications"] = 1
                item["sell_listings"] = data["sell_listings"]
                item["buy_orders"] = data["buy_orders"]
                item["total_listings"] = data["total_listings"]
                prices_np = np.array(quick_data["prices"])
                volatility_approx = (prices_np.std() / prices_np.mean() * 100) if prices_np.mean() > 0 else 0.0
                item["volatility"] = volatility_approx
                passed, reason = item_passes_criteria(item, posted_log, posted_history)
                if passed:
                    df = df_from_pricehistory(raw_history, USD_RATE)
                    analysis = analyze_dataframe(df, item["price_usd"], item["volume_24h"])
                    item.update({k: v for k, v in analysis.items() if k not in item})
                    log_event("criteria_passed", f"Item passed criteria: {reason}", mhn)
                    # Move CSV saving inside if passed
                    safe_name = re.sub(r"[^\w\-_.() ]", "_", mhn)[:120]
                    csv_name = os.path.join(OUT_DIR, f"prices_{safe_name}.csv")
                    try:
                        df.to_csv(csv_name, index=False)
                        log_event("csv_saved", f"CSV saved for item", mhn)
                    except Exception as e:
                        logger.warning(f"Error saving CSV: {e}")
                    price_buf, volume_buf, order_buf = build_plots(item, HISTORY_DAYS)
                    log_event("plots_generated", "Plots generated", mhn)
                    steam_url = f"https://steamcommunity.com/market/listings/{APPID}/{quote(mhn, safe='')}"
                    growth_sign = "+" if item["growth"] >= 0 else ""
                    volume_sign = "+" if item["volume_change"] >= 0 else ""
                    color_emoji = "🟢" if item["growth"] >= 0 else "🔴"
           
                    _, hashtags = get_item_type_and_hashtags(mhn, item)
           
                    similar_posts = get_similar_posts(mhn)
                    similar_text = " | ".join(similar_posts) if similar_posts else "Нет"
           
                    caption = (
                        f"<a href=\"{steam_url}\">{html.escape(item['name'])}</a>\n\n"
                        f"{color_emoji} Стоимость: {format_rub(item['price_rub'])} ({format_usd(item['price_usd'])}) (24 часа: {growth_sign}{item['growth']:.2f}%)\n"
                        f"🔘 Объем продаж: {item['volume_24h']} (24 часа: {volume_sign}{item['volume_change']:.2f}%)\n"
                        f"🔘 Волатильность: {item['volatility']:.2f}%\n\n"
                        f"📤 Лотов на продажу: {item['sell_listings']}\n"
                        f"📥 Запросов на покупку: {item['buy_orders']}\n\n"
                        f"🔗 Схожие публикации за сутки: {similar_text}\n\n"
                        f"{hashtags}"
                    )
                    media_files = []
                    # НОВОЕ: Fetch image и проверка на placeholder
                    img_buf, is_placeholder = fetch_item_image(item.get("image_url"), mhn)
                    if is_placeholder:
                        log_event("skip_no_image", "Skipping post due to no real image available", mhn)
                        continue # Пропускаем весь пост!
                    img_buf = overlay_logo_on_image(img_buf, "top_right", 1.0, is_graph=False)
                    media_files.append(('photo', img_buf.getvalue()))
                    logger.info(f"Real image prepared for post: {mhn}")
                    media_files.append(('photo', price_buf.getvalue()))
                    media_files.append(('photo', volume_buf.getvalue()))
                    media_files.append(('photo', order_buf.getvalue()))
                    sent_id = send_media_group_telegram(media_files, caption)
           
                    if sent_id:
                        posted_log.append(mhn)
                        save_posted_log(posted_log)
                        save_posted_history(sent_id, {"mhn": mhn, "timestamp": time.time()})
                        save_posted_item(item)
                        notify_monitor_bot("post_sent", f"{mhn} at {datetime.now().isoformat()}")
                        # НОВОЕ: Очистка кэша изображения сразу после поста
                        cache_key = hashlib.md5(item.get("image_url", "").encode()).hexdigest()
                        if cache_key in image_cache:
                            del image_cache[cache_key]
                            save_image_cache(image_cache) # Пересохраняем кэш без этого изображения
                            logger.info(f"Image cache cleared for {mhn} after post")
                        log_event("post_sent", "Media group sent successfully", mhn)
                        consecutive_errors = 0
                    else:
                        sent_text_id = send_message_telegram(caption)
                        if sent_text_id:
                            posted_log.append(mhn)
                            save_posted_log(posted_log)
                            save_posted_history(sent_text_id, {"mhn": mhn, "timestamp": time.time()})
                            save_posted_item(item)
                            notify_monitor_bot("post_sent", f"{mhn} (text fallback) at {datetime.now().isoformat()}")
                            log_event("post_sent_fallback", "Text message sent as fallback", mhn)
                            consecutive_errors = 0
                        else:
                            consecutive_errors += 1
                            if consecutive_errors >= 5:
                                send_maintenance_message()
                                log_event("maintenance_sent", "Maintenance message sent due to errors")
                                time.sleep(3600)
                                consecutive_errors = 0
                else:
                    log_event("criteria_failed", f"Item failed criteria: {reason}", mhn)
            except Exception as e:
                consecutive_errors += 1
                mhn = item.get('market_hash_name', 'unknown')
                log_event("item_skip_error", f"Skipping item due to error: {e}", mhn)
                if consecutive_errors >= 5:
                    send_maintenance_message()
                    time.sleep(3600)
                    consecutive_errors = 0
                time.sleep(REQUEST_DELAY + random.random() * JITTER)
                continue
        # НОВЫЙ БЛОК: Проверка summary после for-цикла (для prep_interrupt)
        now_local = datetime.now(tz=TZ)
        target_time = now_local.replace(hour=summary_time.hour, minute=summary_time.minute, second=0, microsecond=0)
        if target_time <= now_local:
            target_time += timedelta(days=1)
        time_to_target = (target_time - now_local).total_seconds()
        if time_to_target <= 0 and not summary_sent_in_24h:
            print("[INFO] Summary time hit after scan interruption! Generating and sending top items summary")
            log_event("summary_after_interrupt", "Summary after prep_interrupt in scan")
            posted_items = load_posted_items(for_summary=True)
            combined_caption, combined_media = generate_daily_summary(posted_items, posted_log)
            sent, sent_id = send_summary_parts(combined_caption, combined_media)
            if sent:
                print("[INFO] Full cleanup after top items summary sent")
                log_event("summary_sent_success", "Top items summary sent successfully")
                save_last_summary(now_local)
                try:
                    with open(SUMMARY_LOG, "r+", encoding="utf-8") as f:
                        data = json.load(f)
                        data['summary_time'] = args.summary_time
                        f.seek(0)
                        json.dump(data, f, ensure_ascii=False, indent=2)
                        f.truncate()
                except Exception as e:
                    logger.warning(f"Error updating summary time: {e}")
                last_summary_sent = now_local
                posted_log = []
                save_posted_log(posted_log)
                with open(POSTED_ITEMS_FILE, "w", encoding="utf-8") as f:
                    json.dump([], f)
                with open(POSTED_HISTORY_FILE, "w", encoding="utf-8") as f:
                    json.dump([], f)
                if os.path.exists(OUT_DIR):
                    shutil.rmtree(OUT_DIR, ignore_errors=True)
                os.makedirs(OUT_DIR, exist_ok=True)
                clear_cache() # ИЗМЕНЕНО: Очистка кэша
                clear_image_cache()
                print("[INFO] Cleanup done: logs, posted_items, history, OUT_DIR, cache")
                log_event("cleanup_done", "Cleanup after top items summary completed")
            else:
                print("[WARN] Top items summary send failed, no cleanup")
                log_event("summary_send_failed", "Top items summary send failed, no cleanup")
            time.sleep(REQUEST_DELAY + random.random() * JITTER)
            continue # Выходим из while, чтобы не спать до next minute
        print_resource_usage(item_counter, total_items)
        print_progress(item_counter, total_items)
        log_event("scan_cycle_end", "Scan cycle completed")
        now_eest = datetime.now(tz=EEST_TZ)
        seconds_to_next_minute = 60 - now_eest.second
        time.sleep(seconds_to_next_minute)
    save_cache(item_cache)
    save_image_cache(image_cache)
if __name__ == "__main__":
    main()
