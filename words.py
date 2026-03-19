import re
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import pytz

# =============================
# GOOGLE AUTH
# =============================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

def get_client():
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "service_account.json", scope
    )
    return gspread.authorize(creds)

# =============================
# IST TIME
# =============================
def get_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

# =============================
# CONFIG
# =============================
SHEET_ID = "1le7tQxVkznMvphgOB2T0tGyzb_ByeaOHJ4R9E5piY_A"

# =============================
# SOURCE WEIGHTS
# =============================
SOURCE_WEIGHT = {
    "nse": 5,
    "bse": 5,
    "monc": 3,
    "et": 1
}

# =============================
# 🔥 ADVANCED EVENT WORDS
# =============================
STRONG_BUY = [
"l1 bidder","loa","letter of award","contract secured","large order","order book",
"buyback","bonus","stock split","record profit","all time high profit",
"debt free","deleveraging","promoter buying","value unlocking","turnaround"
]

MEDIUM_BUY = [
"capacity expansion","partnership","joint venture","acquisition",
"margin expansion","earnings beat","revenue growth","order inflow"
]

LIGHT_BUY = [
"agreement","mou","investment","launch","expansion"
]

STRONG_SELL = [
"forensic audit","auditor resignation","default","insolvency","nclt",
"sebi action","fraud","accounting irregularities","pledge invoked"
]

MEDIUM_SELL = [
"rating downgrade","loss widens","earnings miss",
"production halt","governance issue"
]

LIGHT_SELL = [
"stake sale","promoter selling","margin pressure",
"guidance cut","penalty","litigation"
]

IGNORE = [
"board meeting","postal ballot","agm","investor meet",
"trading window","clarification","newspaper"
]

# =============================
# EVENT SCORE (MULTI HIT)
# =============================
def event_score(text):
    text = text.lower()

    if any(x in text for x in IGNORE):
        return 0

    score = 0

    for w in STRONG_BUY:
        if w in text:
            score += 6

    for w in MEDIUM_BUY:
        if w in text:
            score += 3

    for w in LIGHT_BUY:
        if w in text:
            score += 1

    for w in STRONG_SELL:
        if w in text:
            score -= 6

    for w in MEDIUM_SELL:
        if w in text:
            score -= 3

    for w in LIGHT_SELL:
        if w in text:
            score -= 1

    return score

# =============================
# MONEY SCORE
# =============================
def money_score(text):
    nums = re.findall(r'\d+', text)
    if not nums:
        return 0

    val = max([int(n) for n in nums])

    if val > 1000: return 3
    elif val > 100: return 2
    elif val > 10: return 1
    return 0

# =============================
# SYMBOL NORMALIZATION
# =============================
def normalize_symbol(source, row, text):
    text_upper = text.upper()

    if "BEL" in text_upper or "BHARAT ELECTRONICS" in text_upper:
        return "BEL"
    if "SUBEX" in text_upper:
        return "SUBEX"
    if "DC INFOTECH" in text_upper:
        return "DCI"

    if source == "nse":
        return row[0]

    if source == "bse":
        if len(row) < 2:
            return None

        company = row[1].upper()

        if "BHARAT ELECTRONICS" in company:
            return "BEL"
        if "SUBEX" in company:
            return "SUBEX"

        return None

    return None

# =============================
# READ SHEETS
# =============================
def read_sheet(ws, source):
    data = ws.get_all_values()[1:]
    result = []

    for r in data:
        if len(r) < 1:
            continue

        if source in ["nse","bse"]:
            text = r[-1]
            symbol = normalize_symbol(source, r, text)

        elif source == "et":
            text = r[0]
            symbol = "MARKET"

        elif source == "monc":
            text = r[0]
            symbol = normalize_symbol(source, r, text)

        else:
            continue

        if symbol:
            result.append((source, symbol, text))

    return result

# =============================
# MAIN ENGINE
# =============================
def run():
    client = get_client()
    sheet = client.open_by_key(SHEET_ID)

    all_data = []

    for name in ["nse","bse","et","monc"]:
        try:
            ws = sheet.worksheet(name)
            all_data += read_sheet(ws, name)
        except Exception as e:
            print(f"Skipping {name}: {e}")

    stock_scores = {}

    for source, symbol, text in all_data:

        if symbol in ["", "MARKET", None]:
            continue

        e = event_score(text)

        # 🚀 BSE SELL BLOCK
        if source == "bse" and e < 0:
            continue

        m = money_score(text)
        w = SOURCE_WEIGHT.get(source, 1)

        total = (e + m) * w

        if symbol not in stock_scores:
            stock_scores[symbol] = 0

        stock_scores[symbol] += total

    # =============================
    # FINAL OUTPUT
    # =============================
    output = []

    print("\n======= FINAL HIGH PROBABILITY SIGNALS =======\n")

    for stock, score in stock_scores.items():

        prob = max(0, min(100, int((score + 20) * 2)))

        if prob >= 70:
            signal = "STRONG BUY 🟢🟢"

        elif prob >= 60:
            signal = "BUY 🟢"

        elif prob <= 30:
            signal = "STRONG SELL 🔴🔴"

        elif prob <= 40:
            signal = "SELL 🔴"

        else:
            continue

        print(f"{stock} | Score: {score} | {prob}% | {signal}")

        output.append([
            datetime.now().strftime("%Y-%m-%d %H:%M"),
            stock,
            score,
            prob,
            signal
        ])

    print(f"\nTotal Signals: {len(output)}\n")

    # =============================
    # WRITE TO SHEET (APPEND MODE)
    # =============================
    try:
        ws = sheet.worksheet("FINAL")
    except:
        ws = sheet.add_worksheet(title="FINAL", rows="1000", cols="10")

    if not ws.get_all_values():
        ws.append_row(["Time","Stock","Score","Probability","Signal"])

    output.sort(key=lambda x: x[3], reverse=True)

    if output:
        ws.append_rows(output)

    ws.append_row(["Last Updated (IST):", get_ist_time()])

# =============================
# RUN
# =============================
if __name__ == "__main__":
    run()
