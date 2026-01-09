"""
OSINT Smart Scraper - Streamlit App
"""

import pandas as pd
import streamlit as st
from io import BytesIO

from scraper_unified import CrawlConfig, ProxyPool, crawl_multiple_sites

st.set_page_config(page_title="OSINT Scraper", layout="wide", page_icon="🔍")

st.title("🔍 OSINT Smart Scraper")
st.caption("⚡ Web Crawler con Filtering Avanzato")

# ==================== SIDEBAR ====================

with st.sidebar:
    st.header("⚙️ Config")

    st.subheader("🔄 Proxy")
    use_proxies = st.checkbox("Usa Proxies", value=False)

    proxy_from_secrets = []
    if "proxies" in st.secrets:
        try:
            for key, value in st.secrets["proxies"].items():
                proxy_from_secrets.append({"server": value})
            st.success(f"✅ {len(proxy_from_secrets)} proxy")
        except:
            pass

    proxy_input = st.text_area("Proxy (uno per riga)", height=80, placeholder="http://proxy:8080")

    proxy_list = proxy_from_secrets.copy()
    if use_proxies and proxy_input:
        for line in proxy_input.strip().splitlines():
            if line.strip().startswith("http"):
                proxy_list.append({"server": line.strip()})

    if use_proxies and proxy_list:
        st.info(f"🔄 {len(proxy_list)} proxy attivi")

    st.divider()

    st.subheader("🕷️ Crawling")
    max_pages = st.number_input("Max pagine/sito", 10, 100, 30, 10)
    base_delay = st.slider("Delay (sec)", 0.5, 3.0, 1.0, 0.5)
    timeout_s = st.number_input("Timeout (sec)", 10, 60, 15, 5)

# ==================== SORGENTI ====================

st.subheader("1️⃣ Sorgenti")

if "sources" not in st.session_state:
    st.session_state.sources = []

col1, col2 = st.columns([3, 1])

with col1:
    bulk_input = st.text_area("URL siti (uno per riga)", height=100,
        placeholder="https://www.comune.torino.it/albo-pretorio\nhttps://www.regione.piemonte.it")

with col2:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("➕ Aggiungi", use_container_width=True):
        for line in (bulk_input or "").splitlines():
            url = line.strip()
            if url and url.startswith(("http://", "https://")):
                if url not in [s["url"] for s in st.session_state.sources]:
                    st.session_state.sources.append({"url": url})
        st.rerun()

    if st.button("🗑️ Svuota", use_container_width=True):
        st.session_state.sources = []
        st.rerun()

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

col_f1, col_f2 = st.columns(2)

with col_f1:
    include_kw = st.text_area("INCLUDE keywords", height=100,
        placeholder="affidamento\nincarico\nbando")

with col_f2:
    exclude_kw = st.text_area("ESCLUDE keywords", height=100,
        placeholder="rettifica\nannullamento")

# ==================== ESECUZIONE ====================

st.divider()
st.subheader("3️⃣ Esecuzione")

inc_list = [x.strip() for x in (include_kw or "").splitlines() if x.strip()]
exc_list = [x.strip() for x in (exclude_kw or "").splitlines() if x.strip()]

with st.expander("📋 Riepilogo"):
    st.json({
        "sorgenti": len(st.session_state.sources),
        "proxy": len(proxy_list) if use_proxies else 0,
        "max_pagine": max_pages,
        "include_kw": len(inc_list),
        "exclude_kw": len(exc_list)
    })

can_run = bool(st.session_state.sources)
run_btn = st.button("🚀 Avvia Crawling", type="primary", disabled=not can_run, use_container_width=True)

if run_btn:

    config = CrawlConfig(
        proxy_list=proxy_list if use_proxies else [],
        use_proxies=use_proxies,
        timeout_s=timeout_s,
        max_pages=max_pages,
        base_delay_s=base_delay,
        jitter_s=0.3,
        include_keywords=inc_list,
        exclude_keywords=exc_list
    )

    site_urls = [s["url"] for s in st.session_state.sources]

    progress_bar = st.progress(0, text="Inizializzazione...")

    try:
        with st.spinner("🔍 Crawling in corso..."):
            results = crawl_multiple_sites(site_urls, config)

        progress_bar.progress(100, text="✅ Completato!")

        st.divider()
        st.header("📊 Risultati")

        if not results:
            st.warning("⚠️ Nessun risultato trovato")
        else:
            df = pd.DataFrame(results)

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

            st.dataframe(df[["site", "portal_type", "title", "date", "cig", "cup", "url"]], 
                        use_container_width=True, height=400)

            with st.expander("📋 Dettagli completi"):
                for _, row in df.iterrows():
                    st.markdown(f"**{row['title']}**")
                    st.text(f"🔗 {row['url']}")
                    st.text(f"📅 {row['date']} | 🏷️ {row['portal_type']}")
                    st.text(f"💰 {row['importo_euro_raw']} | CIG: {row['cig']} | CUP: {row['cup']}")
                    st.text(f"📄 {row['snippet'][:150]}...")
                    st.divider()

            st.divider()
            st.subheader("💾 Export")

            col_e1, col_e2, col_e3 = st.columns(3)

            with col_e1:
                csv_data = df.to_csv(index=False, encoding="utf-8-sig")
                st.download_button("⬇️ CSV", data=csv_data,
                    file_name=f"osint_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv", use_container_width=True)

            with col_e2:
                buffer = BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Risultati')
                st.download_button("⬇️ Excel", data=buffer.getvalue(),
                    file_name=f"osint_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.ms-excel", use_container_width=True)

            with col_e3:
                json_data = df.to_json(orient='records', force_ascii=False, indent=2)
                st.download_button("⬇️ JSON", data=json_data,
                    file_name=f"osint_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.json",
                    mime="application/json", use_container_width=True)

    except Exception as e:
        st.error(f"❌ Errore: {str(e)}")
        with st.expander("🐛 Debug info"):
            st.exception(e)

st.divider()
st.markdown("""
<div style='text-align: center; color: #888; padding: 20px;'>
    <p>🔒 <strong>OSINT Smart Scraper</strong> v1.0</p>
    <p style='font-size: 0.9em;'>⚠️ Solo dati pubblici - Rispetta TOS</p>
</div>
""", unsafe_allow_html=True)
