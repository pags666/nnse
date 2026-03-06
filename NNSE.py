import requests
import time
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# -------------------------
# GOOGLE SHEETS LOGIN
# -------------------------

SERVICE_ACCOUNT_FILE = "service_account.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES
)

client = gspread.authorize(creds)

sheet_url = "https://docs.google.com/spreadsheets/d/1le7tQxVkznMvphgOB2T0tGyzb_ByeaOHJ4R9E5piY_A/edit"
sheet = client.open_by_url(sheet_url).sheet1


# -------------------------
# NSE REQUEST
# -------------------------

session = requests.Session()

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "X-Requested-With": "XMLHttpRequest"
}

# get cookies
session.get("https://www.nseindia.com", headers=headers)
time.sleep(3)

url = "https://www.nseindia.com/api/corporate-announcements?index=equities"

response = session.get(url, headers=headers)

data = response.json()

rows = []

for item in data:

    symbol = item.get("symbol", "")
    company = item.get("sm_name", "")
    subject = item.get("desc", "")
    details = item.get("attchmntText", "")

    rows.append([symbol, company, subject, details])


df = pd.DataFrame(rows, columns=[
    "SYMBOL",
    "COMPANY NAME",
    "SUBJECT",
    "DETAILS"
])


# -------------------------
# UPLOAD TO GSHEET
# -------------------------

sheet.clear()

sheet.update(
    [df.columns.values.tolist()] + df.values.tolist()
)

print("Uploaded to Google Sheet successfully")
