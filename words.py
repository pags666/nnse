import gspread
import json
import os
from oauth2client.service_account import ServiceAccountCredentials

# ---------------- GOOGLE SHEETS SETUP ---------------- #

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

def get_client():
    raw_json = os.environ.get("NEW")
    creds_dict = json.loads(raw_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

client = get_client()

SHEET_ID = "1le7tQxVkznMvphgOB2T0tGyzb_ByeaOHJ4R9E5piY_A"

# ---------------- KEYWORDS ---------------- #

GOOD_KEYWORDS = [
    "acquisition","amalgamation","anda approval","asset quality improvement",
    "block deal","bonus","bulk deal","buyback","capacity expansion","capex",
    "commercial production","contract win","debt free","debt reduction","deleverage",
    "demerger","dii buying","dividend","drug approval","drug launch","earnings beat",
    "ebitda growth","fda approval","fii buying","approval","fresh order","government order",
    "guidance upgrade","highest ever profit","market share","infrastructure order",
    "insider buying","institutional buying","ipo subscribed","l1 bidder","large order",
    "letter of award","listing gains","margin expansion","profit","revenue growth",
    "order","plant","production","promoter buying","rating upgrade","record profit",
    "return to profit","robust demand","strong demand","strong earnings","takeover",
    "tax benefit","tender","turnaround","usfda","value unlocking"
]

BAD_KEYWORDS = [
    "accounting irregularities","asset quality stress","auditor resignation",
    "audit qualification","bankruptcy","below estimates","block deal exit",
    "cash crunch","credit rating downgrade","debt default","debt restructuring",
    "default","delisting","downgrade","earnings miss","fii outflow","fraud",
    "governance issues","guidance cut","income tax raid","insolvency",
    "investigation","liquidity crisis","liquidation","loss","margin compression",
    "market share loss","negative guidance","npa","plant shutdown","pledge",
    "production halt","profit warning","promoter selling","stake sale",
    "regulatory","revenue decline","sebi","share dilution","supply overhang",
    "weak earnings","weak guidance"
]

# ---------------- FUNCTION ---------------- #

def classify_and_push(sheet_name, col_index):
    sh = client.open_by_key(SHEET_ID)
    ws = sh.worksheet(sheet_name)

    data = ws.get_all_values()

    good_rows = []
    bad_rows = []

    for row in data[1:]:  # skip header

        if len(row) < col_index:
            continue

        text = row[col_index - 1].lower()

        if any(k in text for k in GOOD_KEYWORDS):
            good_rows.append(row)

        elif any(k in text for k in BAD_KEYWORDS):
            bad_rows.append(row)

    return good_rows, bad_rows

# ---------------- PROCESS ALL SHEETS ---------------- #

all_good = []
all_bad = []

# nse -> column D (4)
g, b = classify_and_push("nse", 4)
all_good.extend(g)
all_bad.extend(b)

# bse -> column C (3)
g, b = classify_and_push("bse", 3)
all_good.extend(g)
all_bad.extend(b)

# monc -> column A (1)
g, b = classify_and_push("monc", 1)
all_good.extend(g)
all_bad.extend(b)

# et -> column A (1)
g, b = classify_and_push("et", 1)
all_good.extend(g)
all_bad.extend(b)

# ---------------- PUSH TO OUTPUT SHEETS ---------------- #

def update_sheet(name, rows):
    sh = client.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(name)
        ws.clear()
    except:
        ws = sh.add_worksheet(title=name, rows="1000", cols="20")

    if rows:
        ws.update("A1", rows)

# update good & bad sheets
update_sheet("good", all_good)
update_sheet("bad", all_bad)

print("DONE ✅")
