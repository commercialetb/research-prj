import streamlit as st
import pandas as pd
import asyncio
import re
import io
import requests
import pdfplumber
import os
from playwright.async_api import async_playwright

# FORZA L'INSTALLAZIONE DEI BINARI ALL'AVVIO
if not os.environ.get("PLAYWRIGHT_BROWSERS_PATH"):
    os.system("playwright install chromium")

st.set_page_config(page_title="Deep Designer Finder", layout="wide")

class DeepNavigator:
    def __init__(self):
        self.pattern = r"(?i)(?:Studio|Arch\.|Ing\.|S\.r\.l\.|S\.p\.A\.|S\.T\.P\.|R\.T\.P\.|Associati)\s+([A-Z][A-Z\s\.]{3,50})"
        self.keywords = ['esito', 'aggiudica', 'verbale', 'determina', 'affidamento']

    async def extract_pdf(self, url):
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            r = requests.get(url, timeout=10, verify=False, headers=headers)
            with pdfplumber.open(io.BytesIO(r.content)) as pdf:
                text = "\n".join([p.extract_text() for p in pdf.pages[:3] if p.extract_text()])
                match = re.search(self.pattern, text)
                return match.group(0).strip() if match else None
        except: return None

    async def scrape_ente(self, url):
        # Aggiungiamo argomenti per evitare il crash su server Linux (no-sandbox)
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
                )
                context = await browser.new_context(user_agent="Mozilla/5.0")
                page = await context.new_page()
                
                await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(2)
                
                # Cerca link PDF
                links = await page.query_selector_all("a")
                for link in links:
                    href = await link.get_attribute("href")
                    if href and (".pdf" in href.lower() or any(k in href.lower() for k in self.keywords)):
                        full_url = href if href.startswith('http') else url.rstrip('/') + '/' + href.lstrip('/')
                        res = await self.extract_pdf(full_url)
                        if res: 
                            await browser.close()
                            return res
                await browser.close()
            except Exception as e:
                return f"Errore: {str(e)[:30]}"
        return "Non trovato"

# INTERFACCIA
st.title("🔍 Ricerca Profonda Progettisti")

uploaded_file = st.sidebar.file_uploader("Carica MASTER_SA_gare_link.csv", type="csv")

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    provincia = st.sidebar.selectbox("Seleziona Provincia", sorted(df['provincia'].unique()))
    
    if st.sidebar.button("Avvia Scansione"):
        batch = df[df['provincia'] == provincia].head(10)
        results = []
        scanner = DeepNavigator()
        
        table_placeholder = st.empty()

        for i, (idx, row) in enumerate(batch.iterrows()):
            st.write(f"⏳ Analizzando: {row['denominazione_sa']}")
            # FIX PER ASYNCIO SU STREAMLIT
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                progettista = loop.run_until_complete(scanner.scrape_ente(row['link_gare']))
                loop.close()
            except:
                progettista = "Errore sessione"

            results.append({"Ente": row['denominazione_sa'], "Progettista": progettista})
            table_placeholder.table(pd.DataFrame(results))

        st.success("Fine Test")
