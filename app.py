import os

# Comando per installare i binari di Playwright se mancano
if not os.path.exists("/home/adminuser/.cache/ms-playwright"):
    os.system("playwright install chromium")

import streamlit as st
import pandas as pd
import asyncio
import re
import io
import requests
import pdfplumber
from playwright.async_api import async_playwright

# Configurazione per iPad/Mobile
st.set_page_config(page_title="Ricerca Profonda Progettisti", layout="wide")

class DeepNavigator:
    def __init__(self):
        # Regex per identificare Studi, Professionisti e Società
        self.pattern = r"(?i)(?:Studio|Arch\.|Ing\.|S\.r\.l\.|S\.p\.A\.|S\.T\.P\.|R\.T\.P\.|Associati)\s+([A-Z][A-Z\s\.]{3,50})"
        self.keywords = ['esito', 'aggiudica', 'verbale', 'determina', 'affidamento', 'incarico']

    async def extract_pdf(self, url):
        """Scarica e analizza il PDF in memoria"""
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            r = requests.get(url, timeout=12, verify=False, headers=headers)
            if r.status_code == 200:
                with pdfplumber.open(io.BytesIO(r.content)) as pdf:
                    testo = "\n".join([p.extract_text() for p in pdf.pages[:4] if p.extract_text()])
                    match = re.search(self.pattern, testo)
                    return match.group(0).strip() if match else None
        except: return None
        return None

    async def scrape_ente(self, url):
        """Naviga il portale e cerca i documenti di aggiudicazione"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0")
            page = await context.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(3) # Tempo per caricamento JS
                
                # Clic automatici per portali comuni
                if "portaleappalti" in url.lower():
                    btn = await page.query_selector("text='Avvisi ed esiti'")
                    if btn: await btn.click(); await asyncio.sleep(2)

                links = await page.query_selector_all("a")
                for link in links:
                    href = await link.get_attribute("href")
                    text = (await link.inner_text() or "").lower()
                    if href and (any(k in text for k in self.keywords) or ".pdf" in href.lower()):
                        full_url = href if href.startswith('http') else url.rstrip('/') + '/' + href.lstrip('/')
                        res = await self.extract_pdf(full_url)
                        if res: return res
            except: pass
            finally: await browser.close()
        return "Non individuato"

# INTERFACCIA UTENTE
st.title("🔍 Ricerca Profonda Progettisti PA")
st.markdown("Strumento di analisi automatizzata portali gare e documenti PDF.")

uploaded_file = st.sidebar.file_uploader("Carica MASTER_SA_gare_link.csv", type="csv")

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    provincia = st.sidebar.selectbox("Seleziona Provincia", sorted(df['provincia'].unique()))
    
    if st.sidebar.button("Avvia Analisi Profonda"):
        batch = df[df['provincia'] == provincia]
        st.write(f"### Analisi in corso per {len(batch)} enti in {provincia}")
        
        results = []
        scanner = DeepNavigator()
        progress = st.progress(0)
        table_placeholder = st.empty()

        for i, (idx, row) in enumerate(batch.iterrows()):
            with st.spinner(f"Analisi: {row['denominazione_sa']}..."):
                progettista = asyncio.run(scanner.scrape_ente(row['link_gare']))
                results.append({
                    "Stazione Appaltante": row['denominazione_sa'],
                    "Progettista/Studio": progettista,
                    "Link Portale": row['link_gare']
                })
                table_placeholder.table(pd.DataFrame(results))
                progress.progress((i + 1) / len(batch))

        st.success("Analisi completata!")
        df_out = pd.DataFrame(results)
        st.download_button("Scarica Risultati (CSV)", df_out.to_csv(index=False), "report.csv", "text/csv")
