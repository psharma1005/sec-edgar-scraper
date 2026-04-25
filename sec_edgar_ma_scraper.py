import time
import snowflake.connector
import pandas as pd
import configparser
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

companies = [
    "Armada I", "Athena Technology", "26 Capital", "Adit EdTech",
    "Reinvent Technology Partners Y", "Blue Safari Group", "Digital World",
    "NextGen II", "MCAP", "GigCapital4", "dMY Technology Group IV",
    "890 5th Avenue Partners", "ION 2", "Poema", "Tailwind Two",
    "Bridgetown 2", "Supernova II", "Roth CH III", "Novus Capital II",
    "FS II", "Decarbonization Plus II", "Angel Pond", "Crescent Cove",
    "Digital Health", "KludeIn I", "Artemis Strategic", "InterPrivate II",
    "Data Knights", "Goldenbridge", "Mount Rainier"
]

def get_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=options)

def extract_dates(driver, company):
    url = f"https://www.sec.gov/edgar/search/#/q={company}%208-K&dateRange=all&category=custom&forms=8-K"
    driver.get(url)
    WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, "//tr")))
    soup = BeautifulSoup(driver.page_source, "html.parser")
    rows = soup.select("tr")
    data = []
    for r in rows:
        link_tag = r.select_one("a")
        date_tag = r.select_one("td:nth-of-type(4)")
        if link_tag and date_tag:
            href = link_tag.get("href", "")
            if "Documents" in link_tag.text:
                data.append((company, "https://www.sec.gov" + href, date_tag.text.strip()))
    return data

def store_in_snowflake(df):
    config = configparser.ConfigParser()
    config.read("config.ini")
    conn = snowflake.connector.connect(
        user=config["snowflake"]["user"],
        password=config["snowflake"]["password"],
        account=config["snowflake"]["account"],
        warehouse=config["snowflake"]["warehouse"],
        database=config["snowflake"]["database"],
        schema=config["snowflake"]["schema"]
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS MA_Announcement_Dates (
            COMPANY_NAME STRING,
            FILING_URL STRING,
            ANNOUNCEMENT_DATE STRING
        )
    """)
    insert_sql = """
        INSERT INTO MA_Announcement_Dates (COMPANY_NAME, FILING_URL, ANNOUNCEMENT_DATE)
        VALUES (%(Company)s, %(Filing_URL)s, %(Date)s)
    """
    for _, row in df.iterrows():
        cursor.execute(insert_sql, row.to_dict())
    conn.commit()
    cursor.close()
    conn.close()

def main():
    driver = get_driver()
    all_data = []
    for c in companies:
        try:
            results = extract_dates(driver, c)
            all_data.extend(results)
        except Exception:
            pass
        time.sleep(1)
    driver.quit()
    df = pd.DataFrame(all_data, columns=["Company", "Filing_URL", "Date"])
    store_in_snowflake(df)

if __name__ == "__main__":
    main()
