'''import re
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
# KEYWORDS
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
# EVENT SCORE
# =============================
def event_score(text):
    text = text.lower()

    if any(x in text for x in IGNORE):
        return 0, []

    score = 0
    reasons = []

    for w in STRONG_BUY:
        if w in text:
            score += 6
            reasons.append(w)

    for w in MEDIUM_BUY:
        if w in text:
            score += 3
            reasons.append(w)

    for w in LIGHT_BUY:
        if w in text:
            score += 1
            reasons.append(w)

    for w in STRONG_SELL:
        if w in text:
            score -= 6
            reasons.append(w)

    for w in MEDIUM_SELL:
        if w in text:
            score -= 3
            reasons.append(w)

    for w in LIGHT_SELL:
        if w in text:
            score -= 1
            reasons.append(w)

    return score, reasons

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
# SYMBOL NORMALIZATION (FIXED)
# =============================
def normalize_symbol(source, row, text):

    if source == "nse":
        return row[0]

    if source == "bse":
        return None

    # ✅ allow monc + et
    if source in ["monc", "et"]:
        return "GENERIC"

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
            symbol = "GENERIC"

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

        if symbol in ["", None]:
            continue

        e, reasons = event_score(text)

        if source == "bse" and e < 0:
            continue

        m = money_score(text)
        w = SOURCE_WEIGHT.get(source, 1)

        total = (e + m) * w

        if symbol not in stock_scores:
            stock_scores[symbol] = {"score":0, "reasons":[]}

        stock_scores[symbol]["score"] += total
        stock_scores[symbol]["reasons"].extend(reasons)

    # =============================
    # FINAL OUTPUT
    # =============================
    output = []

    print("\n======= FINAL HIGH PROBABILITY SIGNALS =======\n")

    for stock, data in stock_scores.items():

        score = data["score"]
        reasons = list(dict.fromkeys(data["reasons"]))

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
            datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M"),
            stock,
            score,
            prob,
            signal,
            ", ".join(reasons[:3])
        ])

    print(f"\nTotal Signals: {len(output)}\n")

    # =============================
    # WRITE TO SHEET
    # =============================
    try:
        ws = sheet.worksheet("wordf")
    except:
        ws = sheet.add_worksheet(title="wordf", rows="1000", cols="10")

    if not ws.get_all_values():
        ws.append_row(["Time","Stock","Score","Probability","Signal","Reason"])

    output.sort(key=lambda x: x[3], reverse=True)

    if output:
        ws.append_rows(output)

    ws.append_row(["Last Updated (IST):", get_ist_time()])

# =============================
# RUN
# =============================
if __name__ == "__main__":
    run()
'''


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

# NSE and BSE are both direct exchange filings — equal weight
SOURCE_WEIGHT = {
    "nse": 5,
    "bse": 5,
}

# =============================
# CONFIDENCE THRESHOLDS
# Signals only shown when confidence strictly exceeds these values
# =============================
BUY_CONF_THRESHOLD  = 80
SELL_CONF_THRESHOLD = 80

# =============================
# SCORE CALIBRATION
# Raw score needed to reach 80% and 100% confidence on each side.
#
# How to read:
#   One STRONG_BUY hit  = +6 keywords × weight 5 = 30 raw  → ~50% conf
#   Two STRONG_BUY hits = 60 raw                          → 100% conf
#   One STRONG_BUY + large deal value (money_score=3)     → (6+3)*5=45 → ~75% conf
#   Two STRONG_BUY + medium deal value                    → (12+2)*5=70 → ~100% conf
#
# This ensures a single keyword never crosses 80% alone.
# =============================
BUY_80_SCORE   = 40    # raw score needed for 80% buy confidence
BUY_100_SCORE  = 60    # raw score for 100% buy confidence

SELL_80_SCORE  = -40   # raw score needed for 80% sell confidence (negative)
SELL_100_SCORE = -60   # raw score for 100% sell confidence (more negative)

# =============================
# KEYWORD CATEGORIES
# =============================

# --- STRONG BUY (+6 each) ---
STRONG_BUY = [
    # Orders & Contracts
    "l1 bidder",
    "lowest bidder",
    "letter of award",
    "loa received",
    "loa issued",
    "work order received",
    "work order awarded",
    "contract awarded",
    "contract secured",
    "order secured",
    "order received",
    "large order",
    "significant order",
    "mega order",
    "repeat order",
    "orders worth",
    "order value of",
    # Financial Milestones
    "record profit",
    "highest ever profit",
    "all time high profit",
    "all-time high revenue",
    "highest ever revenue",
    "profit doubles",
    "profit triples",
    "net profit surges",
    "ebitda surges",
    "beat estimates",
    "beat expectations",
    # Corporate Actions
    "buyback",
    "share buyback",
    "bonus issue",
    "bonus shares",
    "stock split",
    "rights issue",
    # Debt & Promoter
    "debt free",
    "zero debt",
    "deleveraging",
    "debt fully repaid",
    "promoter increases stake",
    "promoter buys shares",
    "promoter acquires shares",
    "open market purchase",
    # Turnaround
    "turnaround",
    "returns to profit",
    "back in black",
    "value unlocking",
    "strategic divestment",
    "slump sale",
    "resolution plan approved",
    "nclt approval",
]

# --- MEDIUM BUY (+3 each) ---
MEDIUM_BUY = [
    # Capacity & Capex
    "capacity expansion",
    "brownfield expansion",
    "greenfield expansion",
    "new manufacturing plant",
    "capex of",
    "capital expenditure of",
    "capacity addition",
    # Deals & Alliances
    "strategic partnership",
    "joint venture",
    "collaboration agreement",
    "technology transfer agreement",
    "licensing agreement",
    "definitive agreement signed",
    "merger agreement",
    "acquisition completed",
    "takeover offer",
    # Financial Performance
    "earnings beat",
    "revenue growth",
    "margin expansion",
    "order inflow",
    "order book grows",
    "strong order book",
    "order pipeline",
    # Fundraising
    "qip",
    "qualified institutional placement",
    "preferential allotment",
    "ipo opens",
    "ncd issue",
    "rights entitlement",
    "fund raise",
    "private placement",
]

# --- LIGHT BUY (+1 each) ---
LIGHT_BUY = [
    "memorandum of understanding",
    "mou signed",
    "letter of intent",
    "loi signed",
    "new product launch",
    "product launch",
    "new vertical",
    "market expansion",
    "export order",
    "distribution agreement",
    "tie-up",
    "empanelled",
    "registered vendor",
]

# --- STRONG SELL (-6 each) ---
STRONG_SELL = [
    # SEBI / Regulatory
    "sebi order against",
    "sebi action against",
    "sebi show cause notice",
    "sebi investigation",
    "sebi ban",
    "sebi penalty",
    "sebi restraint order",
    # Fraud & Accounting
    "fraud detected",
    "fraud alleged",
    "accounting irregularities",
    "forensic audit initiated",
    "forensic investigation",
    "misappropriation",
    "embezzlement",
    "falsification of accounts",
    # Insolvency & Default
    "nclt admits",
    "insolvency petition admitted",
    "corporate insolvency resolution",
    "ipa filed",
    "default on ncd",
    "default on debenture",
    "default on loan",
    "loan default",
    "wilful defaulter",
    "account classified npa",
    "declared npa",
    # Auditor Red Flags
    "auditor resignation",
    "auditor quits",
    "auditor expresses concern",
    "going concern doubt",
    "going concern qualification",
    "qualified audit opinion",
    "adverse audit opinion",
    "disclaimer of opinion",
    # Pledging
    "pledge invoked",
    "pledged shares invoked",
    "margin call triggered",
    "promoter pledge rises sharply",
]

# --- MEDIUM SELL (-3 each) ---
MEDIUM_SELL = [
    # Rating & Outlook
    "credit rating downgrade",
    "rating downgraded",
    "outlook revised to negative",
    "placed on watch negative",
    "rating withdrawn",
    # Financial Deterioration
    "loss widens",
    "net loss reported",
    "quarterly loss",
    "earnings miss",
    "profit falls sharply",
    "revenue declines",
    "revenue falls",
    "margin contracts",
    "ebitda declines",
    # Operations
    "production shutdown",
    "production halt",
    "plant shut down",
    "factory fire",
    "force majeure declared",
    "operations suspended",
    # Governance
    "governance concern",
    "promoter conflict",
    "board dispute",
    "ceo resigns",
    "md resigns",
    "cfo resigns",
    "key management resignation",
    "mass resignation",
    "independent director resigns",
    # Raids
    "ed raid",
    "cbi raid",
    "income tax raid",
    "search and seizure",
    "attachment order",
    "assets attached",
]

# --- LIGHT SELL (-1 each) ---
LIGHT_SELL = [
    "promoter sells shares",
    "promoter stake sale",
    "promoter reduces stake",
    "bulk deal sold",
    "margin pressure",
    "guidance cut",
    "revised guidance downward",
    "penalty imposed",
    "fine imposed by",
    "litigation pending",
    "legal notice received",
    "regulatory notice received",
    "show cause notice received",
    "demand notice",
    "tax demand raised",
    "contingent liability",
]

# --- IGNORE — routine filings, zero score ---
IGNORE = [
    "board meeting intimation",
    "board meeting on",
    "board meeting scheduled",
    "postal ballot",
    "agm notice",
    "agm on",
    "egm notice",
    "investor meet",
    "analyst meet",
    "trading window closure",
    "trading window opens",
    "trading window shall",
    "clarification sought by exchange",
    "clarification submitted",
    "newspaper publication",
    "change in directorate",
    "appointment of additional director",
    "change of registered address",
    "book closure",
    "record date for dividend",
    "dividend payment",
    "interim dividend",
    "final dividend",
    "intimation of dividend",
    "loss of share certificate",
    "duplicate share certificate",
    "transfer of shares",
    "transmission of shares",
    "submission of certificate",
    "reg 74",
    "reg 40",
    "reg 7",
]

# =============================
# KEYWORD MATCH ENGINE
# Returns (buy_score, sell_score, reasons)
# buy_score  >= 0 always
# sell_score <= 0 always
# Kept separate so confidence is computed independently per side
# =============================
def event_score(text):
    t = text.lower()

    if any(phrase in t for phrase in IGNORE):
        return 0, 0, []

    buy_score  = 0
    sell_score = 0
    reasons    = []

    for phrase in STRONG_BUY:
        if phrase in t:
            buy_score += 6
            reasons.append(f"[STRONG BUY] {phrase}")

    for phrase in MEDIUM_BUY:
        if phrase in t:
            buy_score += 3
            reasons.append(f"[MED BUY] {phrase}")

    for phrase in LIGHT_BUY:
        if phrase in t:
            buy_score += 1
            reasons.append(f"[LIGHT BUY] {phrase}")

    for phrase in STRONG_SELL:
        if phrase in t:
            sell_score -= 6
            reasons.append(f"[STRONG SELL] {phrase}")

    for phrase in MEDIUM_SELL:
        if phrase in t:
            sell_score -= 3
            reasons.append(f"[MED SELL] {phrase}")

    for phrase in LIGHT_SELL:
        if phrase in t:
            sell_score -= 1
            reasons.append(f"[LIGHT SELL] {phrase}")

    return buy_score, sell_score, reasons

# =============================
# MONEY SCORE
# Only amplifies BUY side — large deal size strengthens positive signals.
# Not applied to SELL (loss amount does not worsen the sell signal linearly).
# =============================
def money_score(text):
    clean  = text.replace(",", "")
    nums   = re.findall(r'\b(\d+)\b', clean)
    if not nums:
        return 0
    val = max(int(n) for n in nums)
    if val >= 10000: return 5
    elif val >= 1000: return 3
    elif val >= 100:  return 2
    elif val >= 10:   return 1
    return 0

# =============================
# CONFIDENCE CALCULATOR
# Maps raw scores linearly to 0–100% confidence.
# BUY:  [0  → BUY_100_SCORE]  maps to [0% → 100%]
# SELL: [0  → SELL_100_SCORE] maps to [0% → 100%]  (both negative)
# =============================
def compute_confidence(buy_raw, sell_raw):
    buy_conf = 0
    if buy_raw > 0:
        buy_conf = min(100, int((buy_raw / BUY_100_SCORE) * 100))

    sell_conf = 0
    if sell_raw < 0:
        sell_conf = min(100, int((sell_raw / SELL_100_SCORE) * 100))

    return buy_conf, sell_conf

# =============================
# SIGNAL LABEL
# Strict: confidence must be strictly greater than threshold (not equal)
# =============================
def get_signal_label(conf, direction):
    if conf <= BUY_CONF_THRESHOLD:    # same threshold for both sides
        return None

    if direction == "BUY":
        return "STRONG BUY 🟢🟢" if conf >= 95 else "BUY 🟢"

    if direction == "SELL":
        return "STRONG SELL 🔴🔴" if conf >= 95 else "SELL 🔴"

    return None

# =============================
# SYMBOL EXTRACTION
# =============================
def extract_symbol(source, row):
    if source in ["nse", "bse"]:
        if len(row) > 0 and row[0].strip():
            return row[0].strip().upper()
    return None

# =============================
# READ SHEETS
# =============================
def read_sheet(ws, source):
    rows = ws.get_all_values()
    if len(rows) < 2:
        return []
    result = []
    for row in rows[1:]:
        if not row:
            continue
        text = row[-1].strip()
        if not text:
            continue
        symbol = extract_symbol(source, row)
        if symbol:
            result.append((source, symbol, text))
    return result

# =============================
# MAIN ENGINE
# =============================
def run():
    client = get_client()
    sheet  = client.open_by_key(SHEET_ID)

    all_data = []
    for source in ["nse", "bse"]:
        try:
            ws   = sheet.worksheet(source)
            rows = read_sheet(ws, source)
            print(f"[{source.upper()}] Loaded {len(rows)} rows")
            all_data += rows
        except Exception as e:
            print(f"[{source.upper()}] Skipped: {e}")

    if not all_data:
        print("No data loaded. Check sheet names and permissions.")
        return

    # =============================
    # SCORE AGGREGATION
    # buy_score and sell_score accumulated separately per stock symbol
    # =============================
    stock_scores = {}

    for source, symbol, text in all_data:

        b, s, reasons = event_score(text)

        # Money score only amplifies BUY side
        m = money_score(text) if b > 0 else 0

        weight  = SOURCE_WEIGHT.get(source, 1)
        b_total = (b + m) * weight
        s_total = s * weight

        if symbol not in stock_scores:
            stock_scores[symbol] = {
                "buy_score":  0,
                "sell_score": 0,
                "reasons":    [],
                "sources":    set()
            }

        stock_scores[symbol]["buy_score"]  += b_total
        stock_scores[symbol]["sell_score"] += s_total
        stock_scores[symbol]["reasons"].extend(reasons)
        stock_scores[symbol]["sources"].add(source.upper())

    # =============================
    # EVALUATE & DISPLAY SIGNALS
    # =============================
    buy_output  = []
    sell_output = []

    W = 72
    print(f"\n{'='*W}")
    print(f"  HIGH CONFIDENCE SIGNALS  |  Threshold: >{BUY_CONF_THRESHOLD}%")
    print(f"{'='*W}")
    print(f"{'STOCK':<16} {'BUY_RAW':>8} {'SELL_RAW':>9} {'BUY%':>6} {'SELL%':>6}  SIGNAL")
    print(f"{'-'*W}")

    for stock, data in sorted(stock_scores.items()):

        buy_raw  = data["buy_score"]
        sell_raw = data["sell_score"]
        reasons  = list(dict.fromkeys(data["reasons"]))
        sources  = ", ".join(sorted(data["sources"]))

        buy_conf, sell_conf = compute_confidence(buy_raw, sell_raw)

        has_buy_signal  = buy_conf  > BUY_CONF_THRESHOLD
        has_sell_signal = sell_conf > SELL_CONF_THRESHOLD

        # Mixed signal guard — conflicting signals on same stock → suppress both
        if has_buy_signal and has_sell_signal:
            print(f"{stock:<16} {buy_raw:>8} {sell_raw:>9} {buy_conf:>5}% {sell_conf:>5}%  ⚠️  MIXED SIGNALS — SUPPRESSED")
            continue

        now_str = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M")

        if has_buy_signal:
            signal = get_signal_label(buy_conf, "BUY")
            if signal:
                buy_reasons = [r for r in reasons if "BUY" in r]
                reason_str  = " | ".join(buy_reasons[:5])
                print(f"{stock:<16} {buy_raw:>8} {sell_raw:>9} {buy_conf:>5}% {'—':>5}   {signal}  [{sources}]")
                buy_output.append([
                    now_str, stock, buy_raw, sell_raw,
                    buy_conf, signal, sources, reason_str
                ])

        elif has_sell_signal:
            signal = get_signal_label(sell_conf, "SELL")
            if signal:
                sell_reasons = [r for r in reasons if "SELL" in r]
                reason_str   = " | ".join(sell_reasons[:5])
                print(f"{stock:<16} {buy_raw:>8} {sell_raw:>9} {'—':>5}  {sell_conf:>5}%  {signal}  [{sources}]")
                sell_output.append([
                    now_str, stock, buy_raw, sell_raw,
                    sell_conf, signal, sources, reason_str
                ])

    total = len(buy_output) + len(sell_output)
    print(f"\n  BUY Signals: {len(buy_output)}  |  SELL Signals: {len(sell_output)}  |  Total: {total}")
    print(f"{'='*W}\n")

    # =============================
    # WRITE TO GOOGLE SHEET
    # =============================
    try:
        ws_out = sheet.worksheet("signals")
    except Exception:
        ws_out = sheet.add_worksheet(title="signals", rows="2000", cols="10")

    if not ws_out.get_all_values():
        ws_out.append_row([
            "Time", "Stock", "Buy Raw", "Sell Raw",
            "Confidence (%)", "Signal", "Sources", "Matched Reasons"
        ])

    all_output = buy_output + sell_output
    all_output.sort(key=lambda x: x[4], reverse=True)   # sort by confidence desc

    if all_output:
        ws_out.append_rows(all_output)

    ws_out.append_row(["---", "Last Updated (IST):", get_ist_time(), "", "", "", "", ""])
    print(f"Results written to 'signals' sheet.")

# =============================
# ENTRY POINT
# =============================
if __name__ == "__main__":
    run()
