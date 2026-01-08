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
from playwright_stealth import stealth_async
from bs4 import BeautifulSoup
from dateutil import parser as dtparser


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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
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
        for tr in table.find_all("tr"):
            a = tr.find("a", href=True)
            if not a:
                continue
            url = urljoin(base_url, a["href"])
            title = clean_text(a.get_text(" ")) or clean_text(tr.get_text(" "))
            row_text = clean_text(tr.get_text(" "))
            date = ""
            m = RE_DATE_HINT.search(row_text)
            if m:
                date = m.group(1)
            items.append({"url": url, "title": title, "date": date, "from": "table"})
    
    for a in soup.select("a[href]"):
        href = a.get("href")
        txt = clean_text(a.get_text(" "))
        if not href or len(txt) < 12:
            continue
        url = urljoin(base_url, href)
        items.append({"url": url, "title": txt, "date": "", "from": "link"})
    
    out = []
    seen = set()
    for it in items:
        u, _ = urldefrag(it["url"])
        if looks_like_binary(u):
            continue
        if u not in seen:
            seen.add(u)
            it["url"] = u
            out.append(it)
    return out


# ==================== STEALTH FETCHER ====================

class StealthFetcher:
    """Fetcher con Playwright e rotating proxies"""
    
    def __init__(self, proxy_pool: ProxyPool, config: CrawlConfig):
        self.proxy_pool = proxy_pool
        self.config = config
        self._robots_cache = {}
    
    def _get_root(self, url: str) -> str:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}"
    
    def allowed_by_robots(self, url: str) -> bool:
        if not self.config.respect_robots:
            return True
        root = self._get_root(url)
        if root not in self._robots_cache:
            rp = robotparser.RobotFileParser()
            rp.set_url(urljoin(root, "/robots.txt"))
            try:
                rp.read()
            except Exception:
                pass
            self._robots_cache[root] = rp
        rp = self._robots_cache[root]
        try:
            return rp.can_fetch("*", url)
        except Exception:
            return True
    
    async def fetch(self, url: str, max_retries: int = 3) -> Dict:
        url = normalize_url(url)
        
        if not self.allowed_by_robots(url):
            return {"url": url, "success": False, "error": "Blocked by robots.txt"}
        
        for attempt in range(max_retries):
            proxy = self.proxy_pool.get_next_proxy() if self.config.use_proxies else {}
            proxy_id = self.proxy_pool.current_index if proxy and proxy != {} else -1
            user_agent = get_random_user_agent()
            
            async with async_playwright() as p:
                try:
                    browser_args = {
                        "headless": self.config.headless,
                        "args": [
                            "--disable-blink-features=AutomationControlled",
                            "--disable-dev-shm-usage",
                            "--no-sandbox",
                        ]
                    }
                    
                    if proxy and proxy.get("server"):
                        browser_args["proxy"] = proxy
                    
                    browser = await p.chromium.launch(**browser_args)
                    
                    context = await browser.new_context(
                        user_agent=user_agent,
                        viewport={
                            "width": random.randint(1366, 1920),
                            "height": random.randint(768, 1080)
                        },
                        locale="it-IT",
                        timezone_id="Europe/Rome",
                    )
                    
                    page = await context.new_page()
                    await stealth_async(page)
                    
                    await page.goto(url, wait_until="domcontentloaded", timeout=self.config.timeout_s * 1000)
                    await random_delay(1, 2)
                    
                    content = await page.content()
                    blocked_keywords = ["access denied", "blocked", "captcha", "cloudflare"]
                    if any(kw in content.lower() for kw in blocked_keywords):
                        raise Exception("Anti-bot block detected")
                    
                    await human_like_scroll(page)
                    
                    html = await page.content()
                    final_url = page.url
                    
                    await context.close()
                    await browser.close()
                    
                    if proxy_id >= 0:
                        self.proxy_pool.mark_success(proxy_id)
                    
                    return {
                        "url": final_url,
                        "html": html,
                        "success": True,
                        "proxy_id": proxy_id
                    }
                    
                except Exception as e:
                    if proxy_id >= 0:
                        self.proxy_pool.mark_failed(proxy_id)
                    
                    try:
                        await context.close()
                        await browser.close()
                    except:
                        pass
                    
                    if attempt == max_retries - 1:
                        return {
                            "url": url,
                            "error": str(e),
                            "success": False
                        }
                    
                    await random_delay(2, 5)
                    continue
        
        return {"url": url, "success": False, "error": "Max retries exceeded"}


# ==================== CRAWLER ====================

async def crawl_site_intelligent(
    site_url: str,
    config: CrawlConfig,
    proxy_pool: ProxyPool
) -> List[Dict]:
    """Crawler intelligente con stealth"""
    
    site_url = normalize_url(site_url)
    root = f"{urlparse(site_url).scheme}://{urlparse(site_url).netloc}"
    
    fetcher = StealthFetcher(proxy_pool, config)
    visited = set()
    results = []
    queue = [site_url] + [urljoin(root, p) for p in DEFAULT_SEED_PATHS]
    
    regex = re.compile(config.regex_pattern, re.IGNORECASE) if config.regex_pattern.strip() else None
    
    def in_date_range(dt):
        if not dt:
            return True if not (config.date_from or config.date_to) else False
        date_from = parse_date_maybe(config.date_from) if config.date_from else None
        date_to = parse_date_maybe(config.date_to) if config.date_to else None
        if date_from and dt < date_from:
            return False
        if date_to and dt > date_to:
            return False
        return True
    
    print(f"🚀 Crawling: {site_url}")
    
    while queue and len(visited) < config.max_pages:
        url = queue.pop(0)
        url, _ = urldefrag(url)
        
        if url in visited:
            continue
        if config.only_same_domain and not same_domain(root, url):
            continue
        if looks_like_binary(url) and not (config.enable_pdf and url.lower().endswith(".pdf")):
            continue
        
        visited.add(url)
        print(f"📄 [{len(visited)}/{config.max_pages}] {url[:80]}")
        
        response = await fetcher.fetch(url)
        
        if not response.get("success"):
            continue
        
        html = response.get("html", "")
        soup = BeautifulSoup(html, "html.parser")
        
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        
        title = clean_text(soup.title.get_text(" ")) if soup.title else ""
        page_text = clean_text(soup.get_text(" "))
        ptype = portal_type_guess(url, page_text)
        
        date_str = extract_date_from_soup(soup)
        dt = parse_date_maybe(date_str)
        
        items = extract_listing_items(soup, url)
        if len(items) >= 8:
            for it in items[:100]:
                u = it["url"]
                if config.only_same_domain and not same_domain(root, u):
                    continue
                if u not in visited and len(visited) < config.max_pages:
                    queue.append(u)
            continue
        
        ok, matched = keyword_filter(page_text, config.include_keywords, config.exclude_keywords)
        rx_ok = True if not regex else bool(regex.search(page_text))
        date_ok = in_date_range(dt)
        
        doc_urls = []
        if config.enable_pdf:
            for a in soup.select('a[href]'):
                href = a.get("href")
                if not href:
                    continue
                u = urljoin(url, href)
                u, _ = urldefrag(u)
                if u.lower().endswith(".pdf") and same_domain(root, u):
                    doc_urls.append(u)
        
        if ok and rx_ok and date_ok:
            fields = extract_fields(page_text)
            results.append({
                "site": site_url,
                "portal_type": ptype,
                "url": url,
                "title": title,
                "date": dt.isoformat() if dt else (date_str or ""),
                "matched_keywords": "; ".join(matched),
                "snippet": page_text[:400],
                "cig": fields["cig"],
                "cup": fields["cup"],
                "importo_euro_raw": fields["importo_euro_raw"],
                "doc_urls": "; ".join(doc_urls[:20]),
                "proxy_used": response.get("proxy_id", -1)
            })
            print(f"   ✅ Match: {title[:60]}")
        
        if config.enable_pdf:
            for u in doc_urls[:15]:
                if u not in visited and len(visited) < config.max_pages:
                    queue.append(u)
        
        await random_delay(config.base_delay_s, config.base_delay_s + config.jitter_s)
    
    return results


async def crawl_multiple_sites(
    site_urls: List[str],
    config: CrawlConfig
) -> List[Dict]:
    """Crawl multipli siti"""
    
    proxy_pool = ProxyPool(config.proxy_list) if config.proxy_list else ProxyPool([{}])
    semaphore = asyncio.Semaphore(config.max_concurrent)
    all_results = []
    
    async def limited_crawl(url):
        async with semaphore:
            results = await crawl_site_intelligent(url, config, proxy_pool)
            return results
    
    tasks = [limited_crawl(url) for url in site_urls]
    results_list = await asyncio.gather(*tasks)
    
    for results in results_list:
        all_results.extend(results)
    
    print(f"\n{'='*60}")
    print(f"✨ Completato: {len(all_results)} risultati")
    print(f"📊 Statistiche Proxy:")
    for proxy_id, stats in proxy_pool.get_stats().items():
        print(f"   Proxy #{proxy_id}: ✅ {stats['success']} | ❌ {stats['failed']}")
    
    return all_results
