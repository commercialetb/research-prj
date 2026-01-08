"""
OSINT Smart Scraper - Core Module
Unified scraper con rotating proxies e stealth (senza playwright-stealth)
"""

import asyncio
import re
import time
import random
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse, urldefrag
from urllib import robotparser
from itertools import cycle
from typing import List, Dict, Optional

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from dateutil import parser as dtparser


# ==================== STEALTH NATIVO ====================

async def apply_stealth(page):
    """
    Implementazione stealth nativa - maschera automazione browser
    """
    await page.add_init_script("""
        // Override navigator.webdriver
        Object.defineProperty(navigator, 'webdriver', {
            get: () => false
        });
        
        // Override permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );
        
        // Override plugins
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });
        
        // Override languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['it-IT', 'it', 'en-US', 'en']
        });
        
        // Chrome runtime
        window.chrome = {
            runtime: {}
        };
        
        // Override toString per nascondere override
        const originalToString = Function.prototype.toString;
        Function.prototype.toString = function() {
            if (this === window.navigator.permissions.query) {
                return 'function query() { [native code] }';
            }
            return originalToString.call(this);
        };
        
        // Override screen resolution
        Object.defineProperty(screen, 'width', {
            get: () => 1920
        });
        Object.defineProperty(screen, 'height', {
            get: () => 1080
        });
    """)


# ==================== PDF EXTRACTION ====================

def extract_pdf_text_bytes(pdf_bytes: bytes) -> str:
    """Best-effort PDF text extraction"""
    try:
        from pypdf import PdfReader
        import io
        reader = PdfReader(io.BytesIO(pdf_bytes))
        parts = []
        for p in reader.pages[:50]:
            t = p.extract_text() or ""
            if t:
                parts.append(t)
        return "\n".join(parts)
    except Exception:
        return ""


# ==================== REGEX PATTERNS ====================

RE_CIG_CTX = re.compile(r"\bCIG\b[:\s]*([A-Z0-9]{10})\b", re.IGNORECASE)
RE_CUP_CTX = re.compile(r"\bCUP\b[:\s]*([A-Z0-9]{15})\b", re.IGNORECASE)
RE_EURO = re.compile(r"\b€\s?([0-9\.\,]+)\b|\b([0-9\.\,]+)\s?€\b")
RE_DATE_HINT = re.compile(r"\b(\d{1,2}[\/\.-]\d{1,2}[\/\.-]\d{2,4})\b")

PORTAL_HINTS = {
    "albo_pretorio": ["albo pretorio", "albo online", "atti", "pubblicazioni"],
    "trasparenza": ["amministrazione trasparente", "trasparenza", "bandi di gara"],
    "gare": ["gare", "bandi", "avvisi", "eprocurement", "tender", "affidamenti"],
    "news": ["news", "notizie", "comunicati", "avvisi"],
}

DEFAULT_SEED_PATHS = [
    "/albo-pretorio", "/albo-pretorio-online", "/albo",
    "/amministrazione-trasparente", "/trasparenza",
    "/bandi", "/bandi-gare", "/gare", "/avvisi",
    "/news", "/notizie", "/comunicati"
]

BINARY_EXT = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
              ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
              ".zip", ".rar", ".7z", ".tar", ".gz",
              ".mp4", ".mp3", ".avi", ".mov")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


# ==================== PROXY POOL ====================

class ProxyPool:
    """Gestisce pool di proxy con rotazione intelligente"""
    
    def __init__(self, proxies: List[Dict]):
        self.proxies = proxies if proxies else [{}]
        self.proxy_cycle = cycle(self.proxies)
        self.failed_proxies = set()
        self.proxy_stats = {i: {"success": 0, "failed": 0, "last_used": 0} 
                           for i in range(len(self.proxies))}
        self.current_index = 0
    
    def get_next_proxy(self) -> Optional[Dict]:
        if not self.proxies or self.proxies == [{}]:
            return {}
        
        max_attempts = len(self.proxies)
        attempts = 0
        
        while attempts < max_attempts:
            proxy = next(self.proxy_cycle)
            proxy_id = self.proxies.index(proxy)
            
            if proxy_id in self.failed_proxies:
                attempts += 1
                continue
            
            last_used = self.proxy_stats[proxy_id]["last_used"]
            if time.time() - last_used < 3:
                attempts += 1
                continue
            
            self.proxy_stats[proxy_id]["last_used"] = time.time()
            self.current_index = proxy_id
            return proxy
        
        if self.failed_proxies:
            self.failed_proxies.clear()
            return self.get_next_proxy()
        
        return {}
    
    def mark_success(self, proxy_id: int):
        if proxy_id in self.proxy_stats:
            self.proxy_stats[proxy_id]["success"] += 1
            if proxy_id in self.failed_proxies:
                self.failed_proxies.remove(proxy_id)
    
    def mark_failed(self, proxy_id: int):
        if proxy_id in self.proxy_stats:
            self.proxy_stats[proxy_id]["failed"] += 1
            self.failed_proxies.add(proxy_id)
    
    def get_stats(self):
        return self.proxy_stats


# ==================== CONFIGURAZIONE ====================

@dataclass
class CrawlConfig:
    """Configurazione crawling"""
    proxy_list: List[Dict] = None
    use_proxies: bool = False
    headless: bool = True
    timeout_s: int = 30
    max_pages: int = 50
    max_concurrent: int = 2
    base_delay_s: float = 2.0
    jitter_s: float = 1.0
    include_keywords: List[str] = None
    exclude_keywords: List[str] = None
    regex_pattern: str = ""
    date_from: str = None
    date_to: str = None
    only_same_domain: bool = True
    enable_pdf: bool = True
    respect_robots: bool = True
    
    def __post_init__(self):
        if self.proxy_list is None:
            self.proxy_list = []
        if self.include_keywords is None:
            self.include_keywords = []
        if self.exclude_keywords is None:
            self.exclude_keywords = []


# ==================== HELPER FUNCTIONS ====================

def normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    return u

def same_domain(a: str, b: str) -> bool:
    try:
        return urlparse(a).netloc == urlparse(b).netloc
    except Exception:
        return False

def looks_like_binary(url: str) -> bool:
    path = urlparse(url).path.lower()
    return path.endswith(BINARY_EXT)

def clean_text(s: str) -> str:
    s = re.sub(r"\s+", " ", s or "").strip()
    return s

async def random_delay(min_sec: float = 1, max_sec: float = 3):
    await asyncio.sleep(random.uniform(min_sec, max_sec))

def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

async def human_like_scroll(page):
    """Simula scroll umano"""
    try:
        scroll_points = [0.2, 0.5, 0.7, 1.0]
        for point in scroll_points:
            await page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {point})")
            await random_delay(0.3, 0.8)
    except Exception:
        pass

def portal_type_guess(url: str, html_text: str) -> str:
    txt = (url + " " + (html_text or "")).lower()
    scores = {}
    for k, hints in PORTAL_HINTS.items():
        scores[k] = sum(1 for h in hints if h in txt)
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "generic"

def extract_date_from_soup(soup: BeautifulSoup) -> str:
    for sel in [
        ('meta[property="article:published_time"]', "content"),
        ('meta[property="og:updated_time"]', "content"),
        ('meta[name="date"]', "content"),
        ('time[datetime]', "datetime"),
    ]:
        el = soup.select_one(sel[0])
        if el and el.get(sel[1]):
            return el.get(sel[1]).strip()
    t = clean_text(soup.get_text(" "))
    m = RE_DATE_HINT.search(t)
    return m.group(1) if m else ""

def parse_date_maybe(s: str):
    if not s:
        return None
    try:
        return dtparser.parse(s, fuzzy=True, dayfirst=True)
    except Exception:
        return None

def keyword_filter(text: str, include_kw: list, exclude_kw: list) -> tuple:
    t = (text or "").lower()
    matched = []
    if include_kw:
        for k in include_kw:
            kk = k.lower().strip()
            if kk and kk in t:
                matched.append(k)
        if not matched:
            return False, []
    if exclude_kw:
        for k in exclude_kw:
            kk = k.lower().strip()
            if kk and kk in t:
                return False, matched
    return True, matched

def extract_fields(text: str) -> dict:
    cig = ""
    cup = ""
    m = RE_CIG_CTX.search(text or "")
    if m:
        cig = m.group(1)
    m = RE_CUP_CTX.search(text or "")
    if m:
        cup = m.group(1)
    imp = ""
    m = RE_EURO.search(text or "")
    if m:
        imp = (m.group(1) or m.group(2) or "").strip()
    return {"cig": cig, "cup": cup, "importo_euro_raw": imp}

def extract_listing_items(soup: BeautifulSoup, base_url: str) -> list:
    items = []
    
    for table in soup.find_all("table"):
        for tr in table.find_a
