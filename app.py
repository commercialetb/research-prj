import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import pdfplumber
import re
import io

st.set_page_config(page_title="Progettisti PA - iPad Edition", layout="wide")

class SolidScanner:
    def __init__(self):
        # Pattern per trovare i progettisti
        self.pattern = r"(?i)(?:Studio|Arch\.|Ing\.|S\.r\.l\.|S\.p\.A\.|S\.T\.P\.|R\.T\.P\.|Associati)\s+([A-Z][A-Z\s\.]{3,50})"
        self.keywords = ['esito', 'aggiudica', 'verbale', 'determina', 'affidamento', 'progettazione']

    def get_progettista(self, url):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (iPad; CPU OS 16_0 like Mac OS X) AppleWebKit/605.1.15'}
            response = requests.get(url, timeout=15, verify=False, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 1. Cerca tutti i link nella pagina
            links = soup.find_all('a', href=True)
            for link in links:
                href = link['href']
                text = link.get_text().lower()
                
                # 2. Se il link sembra un documento di aggiudicazione o un PDF
                if any(k in text for k in self.keywords) or href.endswith('.pdf'):
                    full_url = href if href.startswith('http') else url.rstrip('/') + '/' + href.lstrip('/')
                    
                    # 3. Se è un PDF, lo analizziamo
                    if '.pdf' in full_url.lower():
                        progettista = self.leggi_pdf(full_url)
                        if progettista: return progettista
            
            return "Nessun documento utile in Home"
        except Exception as e:
            return f"Errore connessione: {str(e)[:20]}"

    def leggi_pdf(self, pdf_url):
        try:
            r = requests.get(pdf_url, timeout=10, verify=False)
            with pdfplumber.open(io.BytesIO(r.content)) as pdf:
                # Leggiamo le prime 3 pagine
                text = ""
                for page in pdf.pages[:3]:
                    text += (page.extract_text() or "") + "\n"
                
                match = re.search(self.pattern, text)
                return match.group(0).strip() if match else None
        except:
            return None

# --- INTERFACCIA STREAMLIT ---
st.title("🔍 Ricerca Progettisti (Versione iPad)")
st.info("Questa versione utilizza una connessione sicura e stabile per evitare crash del server.")

uploaded_file = st.sidebar.file_uploader("Carica MASTER_SA_gare_link.csv", type="csv")

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    provincia = st.sidebar.selectbox("Seleziona Provincia", sorted(df['provincia'].unique()))
    
    if st.sidebar.button("Avvia Scansione"):
        batch = df[df['provincia'] == provincia].head(20) # Test su 20 enti
        results = []
        scanner = SolidScanner()
        
        table_placeholder = st.empty()

        for i, (idx, row) in enumerate(batch.iterrows()):
            st.write(f"🕵️ Analisi: {row['denominazione_sa']}")
            risultato = scanner.get_progettista(row['link_gare'])
            
            results.append({
                "Ente": row['denominazione_sa'],
                "Risultato": risultato
            })
            table_placeholder.table(pd.DataFrame(results))

        st.success("Analisi Completata")
        st.download_button("Scarica Risultati", pd.DataFrame(results).to_csv(index=False), "risultati.csv")
