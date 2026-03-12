import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz

# ---------------------------
# GOOGLE SHEET CONNECTION
# ---------------------------

SERVICE_FILE = "service_account.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file(SERVICE_FILE, scopes=SCOPES)

client = gspread.authorize(creds)

sheet = client.open_by_url(
    "https://docs.google.com/spreadsheets/d/1le7tQxVkznMvphgOB2T0tGyzb_ByeaOHJ4R9E5piY_A/edit"
).worksheet("bse")


# ---------------------------
# FETCH BSE DATA
# ---------------------------

url = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"

params = {
    "pageno": 1,
    "strCat": -1,
    "strPrevDate": "",
    "strScrip": "",
    "strSearch": "P",
    "strToDate": "",
    "strType": "C",
}

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://www.bseindia.com/corporates/ann.html"
}

response = requests.get(url, params=params, headers=headers)

data = response.json()

rows = []

for item in data["Table"]:

    company = item["SLONGNAME"]
    code = item["SCRIP_CD"]
    title = item["HEADLINE"]
    category = item["CATEGORYNAME"]

    rows.append([
        code,
        company,
        title,
        category
    ])


# ---------------------------
# UPDATE GOOGLE SHEET
# ---------------------------

sheet.clear()

sheet.append_row([
    "SYMBOL",
    "COMPANY NAME",
    "ANNOUNCEMENT",
    "CATEGORY"
])

sheet.append_rows(rows)

# ---------------------------
# ADD LAST UPDATED TIME
# ---------------------------

ist = pytz.timezone("Asia/Kolkata")
now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

sheet.append_row([])
sheet.append_row(["Last Updated:", now])

print("BSE announcements updated:", len(rows))
