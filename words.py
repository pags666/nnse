import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------- AUTH ---------------- #

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

def get_google_credentials():
    return ServiceAccountCredentials.from_json_keyfile_name(
        "service_account.json", scope
    )

def get_client():
    creds = get_google_credentials()
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

def classify(sheet_name, col_index):
    sh = client.open_by_key(SHEET_ID)
    ws = sh.worksheet(sheet_name)

    data = ws.get_all_values()

    good = []
    bad = []

    header = data[0]

    for row in data[1:]:

        if len(row) < col_index:
            continue

        text = row[col_index - 1].lower()

        if any(k in text for k in GOOD_KEYWORDS):
            good.append(row)

        elif any(k in text for k in BAD_KEYWORDS):
            bad.append(row)

    return header, good, bad

# ---------------- PROCESS ---------------- #

all_good = []
all_bad = []
final_header = None

# NSE (col D = 4)
h, g, b = classify("nse", 4)
final_header = h
all_good.extend(g)
all_bad.extend(b)

# BSE (col C = 3)
h, g, b = classify("bse", 3)
all_good.extend(g)
all_bad.extend(b)

# MONC (col A = 1)
h, g, b = classify("monc", 1)
all_good.extend(g)
all_bad.extend(b)

# ET (col A = 1)
h, g, b = classify("et", 1)
all_good.extend(g)
all_bad.extend(b)

# ---------------- UPDATE FUNCTION ---------------- #

def update_sheet(name, header, rows):
    sh = client.open_by_key(SHEET_ID)

    try:
        ws = sh.worksheet(name)
        ws.clear()
    except:
        ws = sh.add_worksheet(title=name, rows="1000", cols="20")

    ws.append_row(header)
    if rows:
        ws.append_rows(rows)

# ---------------- PUSH ---------------- #

update_sheet("good", final_header, all_good)
update_sheet("bad", final_header, all_bad)

print("✅ DONE")
print(f"GOOD: {len(all_good)} rows")
print(f"BAD: {len(all_bad)} rows")
