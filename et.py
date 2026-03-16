import requests
from bs4 import BeautifulSoup
from datetime import datetime

# import your existing google sheets module
from google_sheets import update_google_sheet_by_name, append_footer

BASE = "https://economictimes.indiatimes.com"
URL = "https://economictimes.indiatimes.com/markets/stocks/news"

headers = {"User-Agent": "Mozilla/5.0"}

res = requests.get(URL, headers=headers)
soup = BeautifulSoup(res.text, "html.parser")

rows = []

articles = soup.select("h3 a")

for a in articles[:20]:

    subject = a.text.strip()

    symbol = ""

    for word in subject.split():
        if word.isupper() and len(word) <= 10:
            symbol = word
            break

    rows.append([symbol, subject])

# Sheet info
SHEET_ID = "1le7tQxVkznMvphgOB2T0tGyzb_ByeaOHJ4R9E5piY_A"
WORKSHEET = "et"

headers = ["SYMBOL", "SUBJECT"]

# push data
update_google_sheet_by_name(SHEET_ID, WORKSHEET, headers, rows)

# add timestamp footer
append_footer(SHEET_ID, WORKSHEET, ["Updated:", datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
