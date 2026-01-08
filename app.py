"""
OSINT Smart Scraper - Streamlit App
Web interface per crawling PA italiane con rotating proxies
"""

import asyncio
import pandas as pd
import streamlit as st
from dateutil import parser as dtparser
from io import BytesIO

from scraper_unified import (
    CrawlConfig, ProxyPool, crawl_multiple_sites
)

# Configurazione pagina
st.set_page_config(
    page_title="OSINT Stealth Scraper", 
    layout="wide", 
    page_icon="🔍",
    initial_sidebar_state="expanded"
)

# CSS custom
st.markdown("""
<style>
    .stAlert {border-radius: 10px;}
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.title("🔍 OSINT Smart Scraper")
st.caption("⚡ Stealth mode: Playwright + Rotating Proxies + Anti-Bot Bypass")

# ==================== SIDEBAR ====================

with st.sidebar:
    st.header("⚙️ Configurazione")
    
    # Proxy Settings
    st.subheader("🔄 Proxy Settings")
    use_proxies = st.checkbox(
        "Usa Rotating Proxies", 
        value=False,
        help="Attiva rotazione automatica proxy"
    )
    
    # Carica proxy da secrets se disponibili
    proxy_from_secrets = []
    if "proxies" in st.secrets:
        try:
            for key, value in st.secrets["proxies"].items():
                proxy_from_secrets.append({"server": value})
            st.success(f"✅ {len(proxy_from_secrets)} proxy da secrets")
        except:
            pass
    
    proxy_input = st.text_area(
        "Lista Proxy (uno per riga)",
        height=120,
        placeholder="http://proxy1.example.com:8080\nhttp://user:pass@proxy2.com:8080",
        help="Formato: http://host:port o http://user:pass@host:port"
    )
    
    # Parsing proxy
    proxy_list = proxy_from_secrets.copy()
    if use_proxies and proxy_input:
        for line in proxy_input.strip().splitlines():
            line = line.strip()
            if line and line.startswith("http"):
                proxy_list.append({"server": line})
    
    if use_proxies and proxy_list:
        st.info(f"🔄 {len(proxy_list)} proxy configurati")
    
    st.divider()
    
    # Crawling Settings
    st.subheader("🕷️ Crawling")
    max_pages = st.number_input("Max pagine/sito", 10, 500, 50, 10)
    max_concurrent = st.slider("Richieste simultanee", 1, 5, 2)
    base_delay = st.slider("Delay base (sec)", 0.5, 5.0, 2.0, 0.5)
    timeout_s = st.number_input("Timeout (sec)", 10, 60, 30, 5)
    
    st.divider()
    
    # Advanced
    with st.expander("🔧 Avanzate"):
        headless = st.checkbox("Headless", True)
        respect_robots = st.checkbox("Rispetta robots.txt", True)
        only_same_domain = st.checkbox("Solo stesso dominio", True)
        enable_pdf = st.checkbox("Analizza PDF", True)
        jitter = st.slider("Jitter", 0.0, 2.0, 1.0, 0.1)

# ==================== MAIN ====================

st.subheader("1️⃣ Sorgenti")

# Session state
if "sources" not in st.session_state:
    st.session_state.sources = []

col1, col2 = st.columns([3, 1])

with col1:
    bulk_input = st.text_area(
        "URL siti da crawlare (uno per riga)",
        height=150,
        placeholder="https://www.comune.torino.it/albo-pretorio\nhttps://www.regione.piemonte.it/trasparenza",
        help="Portali PA: Albo Pretorio, Trasparenza, Bandi"
    )

with col2:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("➕ Aggiungi", use_container_width=True):
        for line in (bulk_input or "").splitlines():
            url = line.strip()
            if url and url.startswith(("http://", "https://")):
                if url not in [s["url"] for s in st.session_state.sources]:
                    st.session_state.sources.append({
                        "url": url,
                        "label": url.split("//")[1].split("/")[0]
                    })
        st.rerun()
    
    if st.button("🗑️ Svuota", use_container_width=True):
        st.session_state.sources = []
        st.rerun()

# Mostra sorgenti
if st.session_state.sources:
    st.success(f"✅ {len(st.session_state.sources)} sorgenti")
    for idx, src in enumerate(st.session_state.sources):
        col_a, col_b = st.columns([5, 1])
        with col_a:
            st.text(f"🌐 {src['url']}")
        with col_b:
            if st.button("❌", key=f"del_{idx}"):
                st.session_state.sources.pop(idx)
                st.rerun()
else:
    st.info("ℹ️ Aggiungi almeno una sorgente")

# ==================== FILTRI ====================

st.divider()
st.subheader("2️⃣ Filtri")

col_f1, col_f2, col_f3 = st.columns(3)

with col_f1:
    include_kw = st.text_area(
        "INCLUDE (una per riga)",
        height=150,
        placeholder="affidamento\nincarico\nprogettazione\nbando\ngara",
        help="Almeno UNA deve essere presente"
    )

with col_f2:
    exclude_kw = st.text_area(
        "ESCLUDE (una per riga)",
        height=150,
        placeholder="rettifica\nannullamento\narchiviato",
        help="Scarta se presente"
    )

with col_f3:
    regex_pattern = st.text_input(
        "Regex",
        placeholder=r"\bCIG\b|\bCUP\b",
        help="Pattern regex opzionale"
    )
    
    enable_date = st.checkbox("Filtra date", False)
    
    if enable_date:
        from_date = st.date_input("Da", value=None)
        to_date = st.date_input("A", value=None)
    else:
        from_date = None
        to_date = None

# ==================== ESECUZIONE ====================

st.divider()
st.subheader("3️⃣ Esecuzione")

# Prepara config
inc_list = [x.strip() for x in (include_kw or "").splitlines() if x.strip()]
exc_list = [x.strip() for x in (exclude_kw or "").splitlines() if x.strip()]

# Riepilogo
with st.expander("📋 Riepilogo"):
    st.json({
        "sorgenti": len(st.session_state.sources),
        "proxy": len(proxy_list) if use_proxies else 0,
        "max_pagine": max_pages,
        "include_kw": len(inc_list),
        "exclude_kw": len(exc_list),
        "regex": bool(regex_pattern),
        "date_filter": enable_date
    })

# Bottone avvio
can_run = bool(st.session_state.sources)
run_btn = st.button(
    "🚀 Avvia Crawling", 
    type="primary", 
    disabled=not can_run, 
    use_container_width=True
)

if run_btn:
    
    config = CrawlConfig(
        proxy_list=proxy_list if use_proxies else [],
        use_proxies=use_proxies,
        headless=headless,
        timeout_s=timeout_s,
        max_pages=max_pages,
        max_concurrent=max_concurrent,
        base_delay_s=base_delay,
        jitter_s=jitter,
        include_keywords=inc_list,
        exclude_keywords=exc_list,
        regex_pattern=regex_pattern or "",
        date_from=str(from_date) if from_date else None,
        date_to=str(to_date) if to_date else None,
        only_same_domain=only_same_domain,
        enable_pdf=enable_pdf,
        respect_robots=respect_robots
    )
    
    site_urls = [s["url"] for s in st.session_state.sources]
    
    progress_bar = st.progress(0, text="Inizializzazione...")
    
    try:
        with st.spinner("🔍 Crawling in corso..."):
            
            async def run_crawl():
                return await crawl_multiple_sites(site_urls, config)
            
            results = asyncio.run(run_crawl())
        
        progress_bar.progress(100, text="✅ Completato!")
        
        # ==================== RISULTATI ====================
        
        st.divider()
        st.header("📊 Risultati")
        
        if not results:
            st.warning("⚠️ Nessun risultato")
        else:
            df = pd.DataFrame(results)
            
            # Metriche
            col_s1, col_s2, col_s3, col_s4 = st.columns(4)
            with col_s1:
                st.metric("📄 Pagine", len(df))
            with col_s2:
                st.metric("🏛️ Siti", df["site"].nunique())
            with col_s3:
                cig_count = df["cig"].astype(str).str.strip().ne("").sum()
                st.metric("🔢 CIG", cig_count)
            with col_s4:
                cup_count = df["cup"].astype(str).str.strip().ne("").sum()
                st.metric("🏗️ CUP", cup_count)
            
            # Filtri
            st.subheader("🔍 Esplora")
            
            filter_col1, filter_col2 = st.columns(2)
            with filter_col1:
                portal_filter = st.multiselect(
                    "Tipo portale",
                    options=df["portal_type"].unique().tolist(),
                    default=df["portal_type"].unique().tolist()
                )
            with filter_col2:
                site_filter = st.multiselect(
                    "Sito",
                    options=df["site"].unique().tolist(),
                    default=df["site"].unique().tolist()
                )
            
            filtered_df = df[
                (df["portal_type"].isin(portal_filter)) &
                (df["site"].isin(site_filter))
            ].copy()
            
            # Tabella
            st.dataframe(
                filtered_df[[
                    "site", "portal_type", "title", "date", 
                    "cig", "cup", "importo_euro_raw", 
                    "matched_keywords", "url"
                ]],
                use_container_width=True,
                height=400
            )
            
            # Dettagli
            with st.expander("📋 Dettagli"):
                for idx, row in filtered_df.iterrows():
                    st.markdown(f"""
**{row['title']}**  
🔗 {row['url']}  
📅 {row['date']} | 🏷️ {row['portal_type']}  
🔑 {row['matched_keywords']}  
💰 {row['importo_euro_raw']} | CIG: {row['cig']} | CUP: {row['cup']}  
📄 {row['snippet'][:200]}...
                    """)
                    if row.get('doc_urls'):
                        st.markdown(f"📎 PDF: {row['doc_urls']}")
                    st.divider()
            
            # ==================== EXPORT ====================
            
            st.divider()
            st.subheader("💾 Export")
            
            col_e1, col_e2, col_e3 = st.columns(3)
            
            with col_e1:
                csv_data = filtered_df.to_csv(index=False, encoding="utf-8-sig")
                st.download_button(
                    "⬇️ CSV",
                    data=csv_data,
                    file_name=f"osint_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            with col_e2:
                buffer = BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    filtered_df.to_excel(writer, index=False, sheet_name='Risultati')
                excel_data = buffer.getvalue()
                
                st.download_button(
                    "⬇️ Excel",
                    data=excel_data,
                    file_name=f"osint_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            
            with col_e3:
                json_data = filtered_df.to_json(orient='records', force_ascii=False, indent=2)
                st.download_button(
                    "⬇️ JSON",
                    data=json_data,
                    file_name=f"osint_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json",
                    use_container_width=True
                )
    
    except Exception as e:
        st.error(f"❌ Errore: {str(e)}")
        st.exception(e)

# ==================== FOOTER ====================

st.divider()
st.markdown("""
<div style='text-align: center; color: #888; padding: 20px;'>
    <p>🔒 <strong>OSINT Smart Scraper</strong> v1.0</p>
    <p style='font-size: 0.9em;'>⚠️ Solo dati pubblici - Rispetta TOS</p>
</div>
""", unsafe_allow_html=True)

# Sidebar info
with st.sidebar:
    st.divider()
    with st.expander("ℹ️ Info"):
        st.markdown("""
**Features:**
- ✅ Rotating proxies
- ✅ Stealth mode
- ✅ CIG/CUP extraction
- ✅ PDF support
        """)
