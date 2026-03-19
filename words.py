import re
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

# ---------------- AUTH ---------------- #

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

def get_client():
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "service_account.json", scope
    )
    return gspread.authorize(creds)

client = get_client()

SHEET_ID = "1le7tQxVkznMvphgOB2T0tGyzb_ByeaOHJ4R9E5piY_A"

# ---------------- SOURCE WEIGHT ---------------- #

SOURCE_WEIGHT = {
    "nse": 5,
    "bse": 5,
    "monc": 3,
    "et": 1
}

# ---------------- SCORING ---------------- #

def event_score(text):
    text = text.lower()

    if any(x in text for x in ["order","contract","deal","wins","secured"]):
        return 5

    if any(x in text for x in ["approval","launch","expansion","acquisition"]):
        return 3

    if any(x in text for x in ["allotment","subsidiary","investment","agreement"]):
        return 2

    if any(x in text for x in ["fraud","default"]):
        return -5

    if any(x in text for x in ["litigation","penalty","downgrade"]):
        return -4

    if "resignation" in text:
        return -2

    return 0


def money_score(text):
    nums = re.findall(r'\d+', text)
    if not nums:
        return 0

    val = max([int(n) for n in nums])

    if val > 1000:
        return 3
    elif val > 100:
        return 2
    elif val > 10:
        return 1
    return 0


# ---------------- SYMBOL ---------------- #

def extract_symbol(row, sheet_name, text):
    text = text.upper()

    if "BEL" in text or "BHARAT ELECTRONICS" in text:
        return "BEL"
    if "SUBEX" in text:
        return "SUBEX"

    if sheet_name in ["nse", "bse"]:
        return row[0]

    return "UNKNOWN"


# ---------------- CLASSIFY ---------------- #

def process_sheet(sheet_name, col_index):
    sh = client.open_by_key(SHEET_ID)
    ws = sh.worksheet(sheet_name)

    data = ws.get_all_values()

    header = data[0]

    buy = []
    sell = []

    for row in data[1:]:

        if len(row) < col_index:
            continue

        text = row[col_index - 1]

        symbol = extract_symbol(row, sheet_name, text)

        e = event_score(text)

        # 🚀 BSE → ignore negative
        if sheet_name == "bse" and e < 0:
            continue

        m = money_score(text)
        w = SOURCE_WEIGHT.get(sheet_name, 1)

        score = (e + m) * w

        prob = max(0, min(100, int((score + 20) * 2)))

        # ---------------- SIGNAL ---------------- #

        if prob >= 70:
            signal = "STRONG BUY 🟢🟢"
            buy.append([symbol, text, score, prob, signal])

        elif prob >= 60:
            signal = "BUY 🟢"
            buy.append([symbol, text, score, prob, signal])

        elif prob <= 30:
            signal = "STRONG SELL 🔴🔴"
            sell.append([symbol, text, score, prob, signal])

        elif prob <= 40:
            signal = "SELL 🔴"
            sell.append([symbol, text, score, prob, signal])

    return buy, sell


# ---------------- PROCESS ALL ---------------- #

all_buy = []
all_sell = []

# NSE
b, s = process_sheet("nse", 4)
all_buy.extend(b)
all_sell.extend(s)

# BSE
b, s = process_sheet("bse", 3)
all_buy.extend(b)
# ❌ no sell from BSE

# MONC
b, s = process_sheet("monc", 1)
all_buy.extend(b)
all_sell.extend(s)

# ET
b, s = process_sheet("et", 1)
all_buy.extend(b)
all_sell.extend(s)

# ---------------- UPDATE ---------------- #

def update_sheet(name, rows):
    sh = client.open_by_key(SHEET_ID)

    try:
        ws = sh.worksheet(name)
        ws.clear()
    except:
        ws = sh.add_worksheet(title=name, rows="1000", cols="10")

    header = ["Stock", "News", "Score", "Probability", "Signal"]

    ws.append_row(header)

    if rows:
        ws.append_rows(rows)


# ---------------- PUSH ---------------- #

update_sheet("good", all_buy)
update_sheet("bad", all_sell)

print("✅ DONE")
print(f"BUY: {len(all_buy)}")
print(f"SELL: {len(all_sell)}")
