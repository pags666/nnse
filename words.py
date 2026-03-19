import re
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import pytz   # ✅ ADDED

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
# IST TIME FUNCTION (ADDED)
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
# EVENT SCORE
# =============================
def event_score(text):
    text = text.lower()

    if any(x in text for x in ["order","orders","contract","deal","wins","secured","receives","received"]):
        return 5

    if any(x in text for x in ["approval","launch","expansion","acquisition"]):
        return 3

    if any(x in text for x in ["allotment","subsidiary","investment","agreement","partnership"]):
        return 2

    if any(x in text for x in ["fraud","default"]):
        return -5

    if any(x in text for x in ["litigation","penalty"]):
        return -4

    if "resignation" in text:
        return -2

    return 0

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
    text = text.upper()

    if "BEL" in text or "BHARAT ELECTRONICS" in text:
        return "BEL"
    if "SUBEX" in text:
        return "SUBEX"
    if "DC INFOTECH" in text:
        return "DCI"

    if source in ["nse","bse"]:
        return row[0]

    return "UNKNOWN"

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

        if symbol in ["", "MARKET"]:
            continue

        e = event_score(text)

        # 🚀 BSE → REMOVE NEGATIVE IMPACT
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
    # ✅ SORT HERE
    output.sort(key=lambda x: x[3], reverse=True)

    # =============================
    # WRITE TO SHEET
    # =============================
    try:
        ws = sheet.worksheet("FINAL")
    except:
        ws = sheet.add_worksheet(title="FINAL", rows="100", cols="10")

    ws.clear()
    ws.append_row(["Time","Stock","Score","Probability","Signal"])

    if output:
        ws.append_rows(output)

    # ✅ FOOTER (ONLY ADDITION)
    ws.append_row([])
    ws.append_row(["Last Updated (IST):", get_ist_time()])

# =============================
# RUN
# =============================
if __name__ == "__main__":
    run()
