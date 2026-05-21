import os
import re
from datetime import datetime
import pytz
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# =============================
# GOOGLE AUTH & CONFIG
# =============================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

def get_client():
    creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
    return gspread.authorize(creds)

def get_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

SHEET_ID = "1le7tQxVkznMvphgOB2T0tGyzb_ByeaOHJ4R9E5piY_A"

# Weights and Math
SOURCE_WEIGHT = {"nse": 5, "bse": 5, "monc": 4, "et": 4}
BUY_CONF_THRESHOLD  = 60
SELL_CONF_THRESHOLD = 60

# Lowered from 60 to 45. A +6 keyword * weight 5 = 30.
# 30 / 45 = 66% (Clears threshold immediately).
BUY_100_SCORE  = 45
SELL_100_SCORE = -45

# =============================
# ADVANCED PATTERNS (MASSIVELY EXPANDED)
# =============================
NEGATION_PATTERNS = re.compile(
    r'\b(no\s+to|reject(s|ed)?|cancel(s|led)?|shelv(e|es|ed)?|tumble(s|d)?|crash(es|ed)?|'
    r'plunge(s|d)?|withdraw(s|n)?|call(s)?\s*off|abandon(s|ed)?|deny|denies|denied|fail(s|ed)?)\b', 
    re.IGNORECASE
)

BUY_PATTERNS = [
    # Earnings & Financials
    (r'\b(net\s+)?profit\s+(surges?|jumps?|soars?|rises?|grows?|doubles?|triples?|up\b)\b', 6, "profit surge"),
    (r'\brevenue(s)?\s+(surges?|jumps?|soars?|rises?|grows?|up\b)\b', 6, "revenue growth"),
    (r'\bebitda\s+(surges?|jumps?|rises?|grows?|margin\s+expands?)\b', 6, "ebitda/margin expansion"),
    (r'\brecord\s+(profit|revenue|sales|earnings|ebitda)\b', 6, "record financials"),
    (r'\bhighest\s+ever\s+(profit|revenue|sales|quarter)\b', 6, "highest ever financials"),
    (r'\bbeat(s|ing)?\s+(estimates?|expectations?|street)\b', 6, "beat estimates"),
    (r'\bturnaround\b|\breturns?\s+to\s+profit\b|\bback\s+in\s+black\b', 6, "financial turnaround"),
    (r'\b(yoy|qoq)\s+growth\b', 3, "yoy/qoq growth"),

    # Orders, Contracts & Business Wins
    (r'\bl1\s*bidder\b|\blowest\s+bidder\b', 6, "l1 bidder"),
    (r'\bletter\s+of\s+award\b|\bloa\s+(received|issued|awarded)\b', 6, "loa received"),
    (r'\border(s)?\s+(secured|received|awarded|bagged|won)\b', 6, "order secured"),
    (r'\bcontract(s)?\s+(secured|received|awarded|bagged|won)\b', 6, "contract secured"),
    (r'\border(s)?\s+(worth|valued?\s+at|of\s+rs|of\s+inr)\b', 6, "order worth ₹"),
    (r'\b(large|mega|significant|major)\s+order\b', 6, "large order"),
    (r'\bstrong\s+order\s+book\b', 3, "strong order book"),
    
    # Corporate Actions & Expansion
    (r'\bexecut(e|ed|ing)\s+(a\s+)?share\s+purchase\s+agreement\b', 6, "share purchase agreement"),
    (r'\baquir(e|es|ed|ing)\b|\bacquisition\b', 3, "acquisition"),
    (r'\b49\s*%\s*(equity\s+)?stake\b', 6, "stake acquisition"),
    (r'\bbuy\s*back\b|\bshare\s+buy\s*back\b', 6, "buyback"),
    (r'\bbonus\s+(issue|shares?)\b', 6, "bonus issue"),
    (r'\bstock\s+split\b|\bshare\s+split\b', 6, "stock split"),
    (r'\bdebt[\s-]?free\b|\bzero\s+debt\b', 6, "debt-free"),
    (r'\bpromoter\s+(buys?|acquires?|purchased?)\s+(stake|shares?)\b', 6, "promoter buying"),
    (r'\b(strategic\s+)?partner(ship)?\b|\bcollaborat(e|ion|ing)\b|\btie[- ]?up\b', 3, "strategic partnership"),
    (r'\bproduct\s+launch(es|ed)?\b|\bnew\s+launch\b', 3, "product launch"),
    (r'\bcapacity\s+expan(sion|d)\b|\bcapex\b', 3, "capacity expansion/capex"),
    
    # Market & Analyst Sentiment
    (r'\b(upgrade(d|s)?|raise(d|s)?\s+target)\b', 3, "analyst upgrade"),
    (r'\bbuy\s+rating\b', 3, "buy rating"),
]

SELL_PATTERNS = [
    # Earnings & Financials
    (r'\b(net\s+)?profit\s+(falls?|drops?|declin\w+|plunges?|tumbles?|dips?|shrinks?|slumps?)\b', -6, "profit drop"),
    (r'\brevenue(s)?\s+(falls?|drops?|declin\w+|shrinks?|slumps?)\b', -6, "revenue decline"),
    (r'\b(net\s+)?loss\s+(widen(s|ed|ing)|report(s|ed)|post(s|ed)|surge(s|d)?)\b', -6, "loss widens/reported"),
    (r'\bmargin(s)?\s+(contract(s|ed|ing)|shrink(s|ed)?|compress(ed|ion)?|pressure)\b', -6, "margin contraction"),
    (r'\bmiss(es|ed)?\s+(estimates?|expectations?|street)\b', -6, "missed estimates"),
    (r'\bguidance\s+(cut|lower(ed)?|reduc(e|ed))\b', -6, "guidance cut"),

    # Regulatory, Fraud & Legal
    (r'\bsebi\s+(order|action|notice|penalty|ban|restraint|investigation)\b', -6, "sebi action"),
    (r'\b(ed|cbi|income\s*tax|gst)\s+(raid(s|ed)?|search(es)?|survey)\b', -6, "agency raid/search"),
    (r'\bfraud\s+(detected|alleged|committed|suspected)\b', -6, "fraud detected"),
    (r'\baccounting\s+irregularities?\b', -6, "accounting irregularities"),
    (r'\bforensic\s+(audit|investigation)\b', -6, "forensic audit"),
    (r'\bpenalty\s+(of\s+rs|imposed|levied)\b|\btax\s+demand\b', -3, "penalty/tax demand"),
    (r'\bshow\s*cause\s+notice\b', -3, "show cause notice"),

    # Corporate Disasters
    (r'\bnclt\s+admits?\b|\bcirp\b|\binsolvency\b', -6, "insolvency/cirp"),
    (r'\bdefault\s+on\s+(ncd|loan|repayment|debt)\b', -6, "loan/debt default"),
    (r'\bauditor\s+(resign(s|ed|ation)|quit(s|ting))\b', -6, "auditor resignation"),
    (r'\b(ceo|md|cfo)\s+resign(s|ed|ation|ing)\b', -6, "c-level resignation"),
    (r'\bpledge\s+(invok|trigger)\w+\b', -6, "pledge invoked"),
    (r'\bproduction\s+(halt(ed)?|suspend(ed)?|stop(ped)?)\b', -6, "production halted"),
    (r'\b(plant|factory|facility)\s+(shut\s*down|clos(e|ed))\b', -6, "plant shutdown"),

    # Market Activity
    (r'\bpromoter(s)?\s+(sell(s|ing)?|sold|offload(s|ed)?|dilute(s|d)?)\b', -6, "promoter selling"),
    (r'\b(credit\s+)?rating\s+downgrad(e|ed|es)\b', -6, "rating downgraded"),
    (r'\btarget\s+(cut|slashed|lowered)\b|\bsell\s+rating\b', -3, "analyst downgrade"),
]

IGNORE_PATTERNS = [
    r'\bboard\s+meeting\b', r'\bpostal\s+ballot\b', r'\bagm\s+notice\b', 
    r'\bnewspaper\s+publication\b', r'\btrading\s+window\b', r'\bclarification\b',
    r'\bclosure\s+of\b', r'\btranscript\b', r'\bpresentation\b'
]

_BUY_COMPILED = [(re.compile(p, re.IGNORECASE), sc, lbl) for p, sc, lbl in BUY_PATTERNS]
_SELL_COMPILED = [(re.compile(p, re.IGNORECASE), sc, lbl) for p, sc, lbl in SELL_PATTERNS]
_IGNORE_COMPILED = [re.compile(p, re.IGNORECASE) for p in IGNORE_PATTERNS]

# =============================
# HEURISTIC ENTITY EXTRACTION
# =============================
def extract_symbol_advanced(source, row, full_text):
    if source == "nse":
        return row[0].strip().upper() if row[0].strip() else None
    elif source == "bse":
        return row[1].strip().upper() if len(row) > 1 and row[1].strip() else None
    
    text = full_text.upper()
    text = re.sub(r'^(STOCKS TO WATCH|STOCKS IN FOCUS|BUZZING STOCKS|MARKET UPDATE)[:-]\s*', '', text)
    
    action_verbs = r'\b(APPROVES|ANNOUNCES|REPORTS|POSTS|SHARES|Q[1-4]|SURGES|PLUNGES|TUMBLES|JUMPS|DECLARES|BOARD|ACQUIRES|FALLS|DROPS)\b'
    parts = re.split(action_verbs, text, maxsplit=1)
    
    if len(parts) > 1:
        candidate = parts[0].strip()
        candidate = re.sub(r'\b(LTD|LIMITED|INC|CORP|CORPORATION|SOLUTIONS)\.?$', '', candidate).strip()
        if len(candidate) > 25 or len(candidate) < 2:
            return " ".join(text.split()[:2])
        return candidate
    
    return " ".join(text.split()[:2])

# =============================
# CONTEXT-AWARE SCORING ENGINE
# =============================
def contextual_event_score(text):
    for pat in _IGNORE_COMPILED:
        if pat.search(text):
            return 0, 0, []

    buy_score, sell_score = 0, 0
    reasons = []

    # FIX: Split only by hard stops (period, semicolon, newline). 
    # Do NOT split by commas, to preserve financial grammar.
    clauses = re.split(r'[;\.\n]', text)

    for clause in clauses:
        clause = clause.strip()
        if not clause: continue
        
        is_negated = bool(NEGATION_PATTERNS.search(clause))

        clause_buy, clause_sell = 0, 0
        
        for pat, sc, lbl in _BUY_COMPILED:
            if pat.search(clause):
                if is_negated:
                    clause_sell -= sc 
                    reasons.append(f"[REJECTED BUY -> SELL] {lbl}")
                else:
                    clause_buy += sc
                    reasons.append(f"[BUY] {lbl}")

        for pat, sc, lbl in _SELL_COMPILED:
            if pat.search(clause):
                if is_negated:
                    reasons.append(f"[NEGATED SELL -> IGNORED] {lbl}")
                else:
                    clause_sell += sc
                    reasons.append(f"[SELL] {lbl}")

        buy_score += clause_buy
        sell_score += clause_sell

    return buy_score, sell_score, reasons

# =============================
# MONEY MAGNITUDE ENGINE
# =============================
def money_score(text):
    cleaned = re.sub(r'\b(19|20)\d{2}\b', '', text)
    cleaned = re.sub(r'\d+\s*%', '', cleaned)
    currency_match = re.findall(r'(?:rs\.?\s*|inr\s*|₹\s*)?(\d[\d,]*)\s*(?:cr(?:ore)?s?|lakh|lac|million|bn|mn)', cleaned, re.IGNORECASE)
    
    if currency_match:
        values = [int(n.replace(',', '')) for n in currency_match if n.replace(',', '').isdigit()]
        if values:
            val = max(values)
            if val >= 10000: return 5
            elif val >= 1000: return 4
            elif val >= 500:  return 3
            elif val >= 100:  return 2
            elif val >= 10:   return 1
    return 0

# =============================
# CONFIDENCE CALCULATION
# =============================
def compute_confidence(buy_raw, sell_raw):
    buy_conf = min(100, int((buy_raw / BUY_100_SCORE) * 100)) if buy_raw > 0 else 0
    sell_conf = min(100, int((sell_raw / SELL_100_SCORE) * 100)) if sell_raw < 0 else 0
    return buy_conf, sell_conf

def get_signal_label(conf, direction):
    # Adjusted to >= so it naturally catches hits right on the threshold line
    if conf < BUY_CONF_THRESHOLD: return None
    if direction == "BUY": return "STRONG BUY 🟢🟢" if conf >= 90 else "BUY 🟢"
    if direction == "SELL": return "STRONG SELL 🔴🔴" if conf >= 90 else "SELL 🔴"
    return None

# =============================
# MAIN RUNNER
# =============================
def run():
    client = get_client()
    sheet = client.open_by_key(SHEET_ID)

    all_data = []
    for source in ["nse", "bse", "monc", "et"]:
        try:
            ws = sheet.worksheet(source)
            rows = ws.get_all_values()
            if len(rows) < 2: continue
            
            for row in rows[1:]:
                if not row: continue
                full_text = " ".join([cell.strip() for cell in row if cell.strip()])
                if not full_text: continue
                
                symbol = extract_symbol_advanced(source, row, full_text)
                if symbol:
                    all_data.append((source, symbol, full_text))
            print(f"[{source.upper()}] Parsed successfully.")
        except Exception as e:
            print(f"[{source.upper()}] Skipped: {e}")

    stock_scores = {}

    for source, symbol, text in all_data:
        b, s, reasons = contextual_event_score(text)
        
        # Apply magnitude multiplier to BOTH sides. 
        # A huge loss/fine amplifies sell. A huge deal amplifies buy.
        m = money_score(text)
        
        weight = SOURCE_WEIGHT.get(source, 1)
        
        b_total = (b + m) * weight if b > 0 else 0
        s_total = (s - m) * weight if s < 0 else 0 # Subtract because sell is negative

        if b_total == 0 and s_total == 0:
            continue

        if symbol not in stock_scores:
            stock_scores[symbol] = {"buy": 0, "sell": 0, "reasons": [], "sources": set()}
        
        stock_scores[symbol]["buy"] += b_total
        stock_scores[symbol]["sell"] += s_total
        stock_scores[symbol]["reasons"].extend(reasons)
        stock_scores[symbol]["sources"].add(source.upper())

    buy_output, sell_output = [], []
    now_str = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M")

    for stock, data in stock_scores.items():
        buy_conf, sell_conf = compute_confidence(data["buy"], data["sell"])
        has_buy = buy_conf >= BUY_CONF_THRESHOLD
        has_sell = sell_conf >= SELL_CONF_THRESHOLD

        if has_buy and has_sell:
            continue # Suppress mixed signals (e.g. "Revenue surges but profit drops")

        reasons = list(dict.fromkeys(data["reasons"]))
        sources = ", ".join(data["sources"])

        if has_buy:
            signal = get_signal_label(buy_conf, "BUY")
            buy_output.append([now_str, stock, data["buy"], data["sell"], buy_conf, signal, sources, " | ".join(reasons[:5])])
        elif has_sell:
            signal = get_signal_label(sell_conf, "SELL")
            sell_output.append([now_str, stock, data["buy"], data["sell"], sell_conf, signal, sources, " | ".join(reasons[:5])])

    total = len(buy_output) + len(sell_output)
    print(f"\n✅ Total Actionable Signals: {total} (BUYS: {len(buy_output)} | SELLS: {len(sell_output)})")

    if total > 0:
        try:
            ws_out = sheet.worksheet("wordf")
        except:
            ws_out = sheet.add_worksheet(title="wordf", rows="2000", cols="10")
        
        if not ws_out.get_all_values():
            ws_out.append_row(["Time", "Stock", "Buy Raw", "Sell Raw", "Confidence (%)", "Signal", "Sources", "Matched Reasons"])

        all_output = sorted(buy_output + sell_output, key=lambda x: x[4], reverse=True)
        ws_out.append_rows(all_output)
        ws_out.append_row(["---", "Last Updated (IST):", get_ist_time(), "", "", "", "", ""])
        print("✅ Written to 'wordf' sheet.")

if __name__ == "__main__":
    run()
