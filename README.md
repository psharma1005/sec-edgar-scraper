# SEC EDGAR M&A Scraper

This project automated the extraction of **Merger and Acquisition (M&A)** announcement dates from **SEC EDGAR 8-K filings** using Python, BeautifulSoup, and Selenium. It stores structured results in a **Snowflake** warehouse for downstream analysis.


## Features
- Fully automated retrieval of 8-K filing announcement dates from SEC EDGAR
- Integration with Snowflake for persistent storage
- Modular, headless browser automation using Selenium
- Optimized for scaling across 30+ companies


## Setup

### 1️⃣ Install Dependencies
```bash
pip install selenium beautifulsoup4 pandas snowflake-connector-python
