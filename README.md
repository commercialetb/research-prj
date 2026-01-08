# 🔍 OSINT Smart Scraper

**Web scraper intelligente per database pubblici PA italiane con rotating proxies e stealth mode.**

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://your-app.streamlit.app)

## ✨ Features

- 🔄 **Rotating Proxies** - Rotazione automatica per evitare blocchi
- 🥷 **Stealth Mode** - Playwright + fingerprint randomization
- 🎯 **Smart Crawling** - Discovery automatica sitemap e RSS feeds
- 📊 **Estrazione Strutturata** - CIG, CUP, importi, date
- 📄 **PDF Support** - Text extraction da documenti PDF
- 💾 **Multi-Export** - CSV, Excel, JSON
- 🌐 **Web Interface** - Streamlit dashboard intuitiva

## 🚀 Deploy su Streamlit Cloud

### Quick Deploy

1. **Fork questo repository**
2. Vai su [share.streamlit.io](https://share.streamlit.io)
3. Clicca su "New app"
4. Seleziona il tuo fork
5. Main file: `app.py`
6. Clicca "Deploy"

### Configurazione Proxy (Opzionale)

Nel pannello **Secrets** di Streamlit Cloud, aggiungi:

```toml
[proxies]
proxy1 = "http://proxy1.example.com:8080"
proxy2 = "http://user:pass@proxy2.example.com:8080"
proxy3 = "http://proxy3.example.com:8080"
