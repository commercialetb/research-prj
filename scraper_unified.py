"""
OSINT Smart Scraper - Core Module
"""

import asyncio
import re
import time
import random
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse, urldefrag
from urllib import robotparser
from itertools import cycle
from typing import List, Dict, Optional

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from dateutil import parser as dtparser


async def apply_stealth(page):
    """Implementazione stealth nativa"""
    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => false});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
        Object.defineProperty(navigator, 'languages', {get: () => ['it-IT', 'it']});
        window.chrome = {runtime: {}};
    """)


RE_CIG_CTX = re.compile(r"\bCIG\b[:\s]*([A-Z0-9]{10})\b", re.IGNORECASE)
RE_CUP_CTX = re.compile(r"\bCUP\b[:\s]*([A-Z0-9]{15})\b", re.IGNORECASE)
RE_EURO = re.compile(r"\b€\s?([0-9\.\,]+)\b|\b([0-9\.\,]+)\s?€\b")
RE_DATE_HINT = re.compile(r"\b(\d{1,2}[\/\.-]\d{1,2}[\/\.-]\d{2,4})\b")

PORTAL_HINTS = {
    "albo_pretorio": ["albo pretorio", "albo online", "atti"],
    "trasparenza": ["amministrazione trasparente", "trasparenza"],
    "gare": ["gare", "bandi", "avvisi", "affidamenti"],
    "news": ["news", "notizie", "comunicati"],
}

DEFAULT_SEED_PATHS = [
    "/albo-pretorio", "/albo", "/trasparenza",
    "/bandi", "/gare", "/news"
]

BINARY_EXT = (".jpg", ".jpeg", ".png", ".gif", ".css", ".js", 
              ".zip", ".rar", ".mp4", ".mp3")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
]


class ProxyPool:
    def __init__(self, proxies: List[Dict]):
        self.proxies = proxies if proxies else [{}]
        self.proxy_cycle = cycle(self.proxies)
        self.failed_proxies = set()
        self.proxy_stats = {i: {"success": 0, "failed": 0, "last_used": 0} 
                           for i in range(len(self.proxies))}
        self.current_index = 0
    
    def get_next_proxy(self) -> Dict:
        if not self.proxies or self.proxies == [{}]:
            return {}
        
        for _ in range(len(self.proxies)):
            proxy = next(self.proxy_cycle)
            proxy_id = self.proxies.index(proxy)
            
            if proxy_id in self.failed_proxies:
                continue
            
            if time.time() - self.proxy_stats[proxy_id]["last_used"] < 3:
                continue
            
            self.proxy_stats[proxy_id]["last_used"] = time.time()
            self.current_index = proxy_id
            return proxy
        
        self.failed_proxies.clear()
        return self.get_next_proxy()
    
    def mark_success(self, proxy_id: int):
        if proxy_id in self.proxy_stats:
            self.proxy_stats[proxy_id]["success"] += 1
    
    def mark_failed(self, proxy_id: int):
        if proxy_id in self.proxy_stats:
            self.proxy_stats[proxy_id]["failed"] += 1
            self.failed_proxies.add(proxy_id)
    
    def get_stats(self):
        return self.proxy_stats


@dataclass
class CrawlConfig:
    proxy_list: List[Dict] = field(default_factory=list)
    use_proxies: bool = False
    headless: bool = True
    timeout_s: int = 30
    max_pages: int = 50
    max_concurrent: int = 2
    base_delay_s: float = 2.0
    jitter_s: float = 1.0
    include_keywords: List[str] = field(default_factory=list)
    exclude_keywords: List[str] = field(default_factory=list)
    regex_pattern: str = ""
    date_from: str = None
    date_to: str = None
    only_same_domain: bool = True
    enable_pdf: bool = True
    respect_robots: bool = True


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
    except:
        return False


def looks_like_binary(url: str) -> bool:
    return urlparse(url).path.lower().endswith(BINARY_EXT)


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


async def random_delay(min_sec: float = 1, max_sec: float = 3):
    await asyncio.sleep(random.uniform(min_sec, max_sec))


def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)


async def human_like_scroll(page):
    try:
        for point in [0.2, 0.5, 0.7, 1.0]:
            await page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {point})")
            await asyncio.sleep(random.uniform(0.3, 0.8))
    except:
        pass


def portal_type_guess(url: str, html_text: str) -> str:
    txt = (url + " " + (html_text or "")).lower()
    scores = {k: sum(1 for h in hints if h in txt) for k, hints in PORTAL_HINTS.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "generic"


def extract_date_from_soup(soup: BeautifulSoup) -> str:
    for sel, attr in [
        ('meta[property="article:published_time"]', "content"),
        ('meta[name="date"]', "content"),
        ('time[datetime]', "datetime"),
    ]:
        el = soup.select_one(sel)
        if el and el.get(attr):
            return el.get(attr).strip()
    
    m = RE_DATE_HINT.search(clean_text(soup.get_text(" ")))
    return m.group(1) if m else ""


def parse_date_maybe(s: str):
    if not s:
        return None
    try:
        return dtparser.parse(s, fuzzy=True, dayfirst=True)
    except:
        return None


def keyword_filter(text: str, include_kw: list, exclude_kw: list):
    t = (text or "").lower()
    matched = []
    
    if include_kw:
        for k in include_kw:
            if k.lower().strip() in t:
                matched.append(k)
        if not matched:
            return False, []
    
    if exclude_kw:
        for k in exclude_kw:
            if k.lower().strip() in t:
                return False, matched
    
    return True, matched


def extract_fields(text: str) -> dict:
    cig = ""
    cup = ""
    imp = ""
    
    m = RE_CIG_CTX.search(text or "")
    if m:
        cig = m.group(1)
    
    m = RE_CUP_CTX.search(text or "")
    if m:
        cup = m.group(1)
    
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
            title = clean_text(a.get_text(" "))
            items.append({"url": url, "title": title})
    
    for a in soup.select("a[href]"):
        href = a.get("href")
        txt = clean_text(a.get_text(" "))
        if href and len(txt) >= 12:
            items.append({"url": urljoin(base_url, href), "title": txt})
    
    seen = set()
    result = []
    for it in items:
        u, _ = urldefrag(it["url"])
        if not looks_like_binary(u) and u not in seen:
            seen.add(u)
            it["url"] = u
            result.append(it)
    
    return result


class StealthFetcher:
    def __init__(self, proxy_pool: ProxyPool, config: CrawlConfig):
        self.proxy_pool = proxy_pool
        self.config = config
        self._robots_cache = {}
    
    def allowed_by_robots(self, url: str) -> bool:
        if not self.config.respect_robots:
            return True
        
        root = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        if root not in self._robots_cache:
            rp = robotparser.RobotFileParser()
            rp.set_url(urljoin(root, "/robots.txt"))
            try:
                rp.read()
            except:
                pass
            self._robots_cache[root] = rp
        
        try:
            return self._robots_cache[root].can_fetch("*", url)
        except:
            return True
    
    async def fetch(self, url: str, max_retries: int = 3) -> Dict:
        url = normalize_url(url)
        
        if not self.allowed_by_robots(url):
            return {"url": url, "success": False, "error": "Blocked by robots.txt"}
        
        for attempt in range(max_retries):
            proxy = self.proxy_pool.get_next_proxy() if self.config.use_proxies else {}
            proxy_id = self.proxy_pool.current_index if proxy and proxy != {} else -1
            
            async with async_playwright() as p:
                try:
                    browser_args = {
                        "headless": self.config.headless,
                        "args": [
                            "--disable-blink-features=AutomationControlled",
                            "--no-sandbox",
                        ]
                    }
                    
                    if proxy and proxy.get("server"):
                        browser_args["proxy"] = proxy
                    
                    browser = await p.chromium.launch(**browser_args)
                    context = await browser.new_context(
                        user_agent=get_random_user_agent(),
                        viewport={"width": random.randint(1366, 1920), "height": random.randint(768, 1080)},
                        locale="it-IT",
                    )
                    
                    page = await context.new_page()
                    await apply_stealth(page)
                    
                    await page.goto(url, wait_until="domcontentloaded", timeout=self.config.timeout_s * 1000)
                    await random_delay(1, 2)
                    
                    html = await page.content()
                    final_url = page.url
                    
                    await context.close()
                    await browser.close()
                    
                    if proxy_id >= 0:
                        self.proxy_pool.mark_success(proxy_id)
                    
                    return {"url": final_url, "html": html, "success": True, "proxy_id": proxy_id}
                
                except Exception as e:
                    if proxy_id >= 0:
                        self.proxy_pool.mark_failed(proxy_id)
                    
                    try:
                        await context.close()
                        await browser.close()
                    except:
                        pass
                    
                    if attempt == max_retries - 1:
                        return {"url": url, "error": str(e), "success": False}
                    
                    await random_delay(2, 5)
        
        return {"url": url, "success": False, "error": "Max retries"}


async def crawl_site_intelligent(site_url: str, config: CrawlConfig, proxy_pool: ProxyPool) -> List[Dict]:
    site_url = normalize_url(site_url)
    root = f"{urlparse(site_url).scheme}://{urlparse(site_url).netloc}"
    
    fetcher = StealthFetcher(proxy_pool, config)
    visited = set()
    results = []
    queue = [site_url] + [urljoin(root, p) for p in DEFAULT_SEED_PATHS]
    
    regex = re.compile(config.regex_pattern, re.IGNORECASE) if config.regex_pattern else None
    
    def in_date_range(dt):
        if not dt:
            return not (config.date_from or config.date_to)
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
        if looks_like_binary(url):
            continue
        
        visited.add(url)
        print(f"📄 [{len(visited)}/{config.max_pages}] {url[:80]}")
        
        response = await fetcher.fetch(url)
        
        if not response.get("success"):
            continue
        
        html = response.get("html", "")
        soup = BeautifulSoup(html, "html.parser")
        
        for tag in soup(["script", "style"]):
            tag.decompose()
        
        title = clean_text(soup.title.get_text(" ")) if soup.title else ""
        page_text = clean_text(soup.get_text(" "))
        ptype = portal_type_guess(url, page_text)
        
        date_str = extract_date_from_soup(soup)
        dt = parse_date_maybe(date_str)
        
        items = extract_listing_items(soup, url)
        if len(items) >= 8:
            for it in items[:50]:
                u = it["url"]
                if config.only_same_domain and not same_domain(root, u):
                    continue
                if u not in visited:
                    queue.append(u)
            continue
        
        ok, matched = keyword_filter(page_text, config.include_keywords, config.exclude_keywords)
        rx_ok = not regex or bool(regex.search(page_text))
        date_ok = in_date_range(dt)
        
        if ok and rx_ok and date_ok:
            fields = extract_fields(page_text)
            results.append({
                "site": site_url,
                "portal_type": ptype,
                "url": url,
                "title": title,
                "date": dt.isoformat() if dt else date_str,
                "matched_keywords": "; ".join(matched),
                "snippet": page_text[:400],
                "cig": fields["cig"],
                "cup": fields["cup"],
                "importo_euro_raw": fields["importo_euro_raw"],
                "doc_urls": "",
                "proxy_used": response.get("proxy_id", -1)
            })
            print(f"   ✅ {title[:60]}")
        
        await random_delay(config.base_delay_s, config.base_delay_s + config.jitter_s)
    
    return results


async def crawl_multiple_sites(site_urls: List[str], config: CrawlConfig) -> List[Dict]:
    proxy_pool = ProxyPool(config.proxy_list if config.proxy_list else [{}])
    semaphore = asyncio.Semaphore(config.max_concurrent)
    all_results = []
    
    async def limited_crawl(url):
        async with semaphore:
            return await crawl_site_intelligent(url, config, proxy_pool)
    
    tasks = [limited_crawl(url) for url in site_urls]
    results_list = await asyncio.gather(*tasks)
    
    for results in results_list:
        all_results.extend(results)
    
    print(f"✨ Completato: {len(all_results)} risultati")
    return all_results
