import requests
from bs4 import BeautifulSoup
from datetime import datetime

# import your existing module
from google_sheets import update_google_sheet_by_name, append_footer

BASE_URL = "https://www.moneycontrol.com/news/business/stocks/"
HEADERS = {"User-Agent": "Mozilla/5.0"}

pages = 3

rows = []
count = 1

for page in range(1, pages + 1):

    if page == 1:
        url = BASE_URL
    else:
        url = f"{BASE_URL}page-{page}/"

    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    news_items = soup.find_all("li", class_="clearfix")

    for item in news_items:

        h2 = item.find("h2")
        a = item.find("a")

        if h2 and a:

            title = h2.text.strip()
       

            rows.append([title, link])

            count += 1


# Google Sheet info
SHEET_ID = "1le7tQxVkznMvphgOB2T0tGyzb_ByeaOHJ4R9E5piY_A"
WORKSHEET = "monc"

headers = ["TITLE"]

# push to Google Sheets
update_google_sheet_by_name(SHEET_ID, WORKSHEET, headers, rows)

# timestamp
append_footer(
    SHEET_ID,
    WORKSHEET,
    ["Updated:", datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
)
