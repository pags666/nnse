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

# Baseline for 100% confidence. 
# A +6 Strong score * 5 Source Weight = 30 points (66% conf)
BUY_100_SCORE  = 45
SELL_100_SCORE = -45

# =============================
# NEGATION ENGINE
# =============================
NEGATION_PATTERNS = re.compile(
    r'\b(no\s+to|reject(s|ed)?|cancel(s|led)?|shelv(e|es|ed)?|tumble(s|d)?|crash(es|ed)?|'
    r'plunge(s|d)?|withdraw(s|n)?|call(s)?\s*off|abandon(s|ed)?|deny|denies|denied|fail(s|ed)?)\b', 
    re.IGNORECASE
)

# =============================
# USER'S TIERED DICTIONARIES
# =============================
BUY_PATTERNS = [
    # ── STRONG BUY (+6) ─────────────────────────────────────────────────
    # Orders & Contracts
    (r'\bl1\s*bidder\b',                                                6, "l1 bidder"),
    (r'\blowest\s+bidder\b',                                            6, "lowest bidder"),
    (r'\bletter\s+of\s+award\b',                                        6, "letter of award"),
    (r'\bloa\s+(received|issued|awarded)\b',                            6, "loa received/issued"),
    (r'\bwork\s+order\s+(received|awarded|secured|worth)\b',            6, "work order received"),
    (r'\bcontract\s+(awarded|secured|signed|worth|received)\b',         6, "contract awarded/secured"),
    (r'\border\s+(secured|received|awarded|bagged|won)\b',              6, "order secured/received"),
    (r'\border(s)?\s+(worth|valued?\s+at|of\s+rs|of\s+inr)\b',          6, "order worth ₹"),
    (r'\b(large|mega|significant|major|repeat)\s+order\b',              6, "large/mega/significant order"),
    (r'\border\s+intak(e|es)\b',                                        6, "order intake"),
    (r'\bexecut(e|ed|ing)\s+(a\s+)?share\s+purchase\s+agreement\b',     6, "share purchase agreement"),
    (r'\baquir(e|es|ed|ing)\b|\bacquisition\b',                         6, "acquisition/acquire"),
    (r'\b49\s*%\s*(equity\s+)?stake\b',                                 6, "stake acquisition"),

    # Financial Milestones
    (r'\brecord\s+(profit|revenue|sales|earnings|ebitda)\b',            6, "record profit/revenue"),
    (r'\bhighest\s+ever\s+(profit|revenue|sales)\b',                    6, "highest ever profit"),
    (r'\ball.?time\s+high\s+(profit|revenue|sales)\b',                  6, "all-time high"),
    (r'\bprofit\s+(doubles?|triples?|surges?|jumps?|soars?)\b',         6, "profit doubles/surges"),
    (r'\bnet\s+profit\s+(surges?|jumps?|rises?|up)\b',                   6, "net profit surge"),
    (r'\bebitda\s+(surges?|jumps?|rises?|up|grows?)\b',                  6, "ebitda surge"),
    (r'\bbeat(s|ing)?\s+(estimates?|expectations?|consensus)\b',         6, "beat estimates"),

    # Corporate Actions
    (r'\bbuy\s*back\b|\bshare\s+buy\s*back\b',                           6, "buyback"),
    (r'\bbonus\s+(issue|shares?)\b',                                     6, "bonus issue/shares"),
    (r'\bstock\s+split\b|\bshare\s+split\b',                             6, "stock/share split"),
    (r'\brights?\s+issue\b',                                             6, "rights issue"),

    # Debt & Promoter
    (r'\bdebt[\s-]?free\b|\bzero\s+debt\b',                              6, "debt-free"),
    (r'\bdelevera(ge|ging|ged)\b|\bdebt\s+(repaid|cleared|fully\s+paid)\b', 6, "deleveraging"),
    (r'\bpromoter\s+(increases?|buys?|acquires?|purchased?)\s+(stake|shares?)\b', 6, "promoter buys"),
    (r'\bopen\s+market\s+(purchase|buy)\b',                              6, "open market purchase"),

    # Turnaround
    (r'\bturnaround\b',                                                  6, "turnaround"),
    (r'\breturns?\s+to\s+profit\b|\bback\s+in\s+black\b',                6, "returns to profit"),
    (r'\bvalue\s+unlocking\b|\bstrategic\s+divestment\b',                6, "value unlocking"),
    (r'\bnclt\s+(approval|approves?|order)\b',                           6, "nclt approval"),
    (r'\bresolution\s+plan\s+(approved|accepted)\b',                     6, "resolution plan approved"),

    # ── MEDIUM BUY (+3) ─────────────────────────────────────────────────
    (r'\bcapacity\s+expan(sion|d|ding)\b',                               3, "capacity expansion"),
    (r'\b(brownfield|greenfield)\s+expan(sion|d)\b',                     3, "brownfield/greenfield expansion"),
    (r'\bnew\s+(manufacturing\s+)?plant\b',                              3, "new plant"),
    (r'\bcapex\s+(of|plan|worth|investment)\b',                          3, "capex"),
    (r'\bcapital\s+expenditure\s+(of|plan|worth)\b',                     3, "capital expenditure"),
    (r'\bcapacity\s+addition\b',                                         3, "capacity addition"),
    (r'\bjoint\s+venture\b|\bjv\s+(agreement|formed|signed)\b',          3, "joint venture"),
    (r'\bstrategic\s+partner(ship)?\b',                                  3, "strategic partnership"),
    (r'\bcollabor(ation|ate|ating)\s+(agreement|with)\b',                3, "collaboration"),
    (r'\btechnology\s+(transfer|agreement|licens(e|ing))\b',             3, "technology agreement"),
    (r'\bdefinitive\s+agreement\s+(signed|executed)\b',                  3, "definitive agreement"),
    (r'\bmerger\s+(agreement|approved|completed)\b',                     3, "merger"),
    (r'\btakeover\s+offer\b|\bopen\s+offer\b',                           3, "takeover/open offer"),
    (r'\bearnings?\s+beat\b',                                            3, "earnings beat"),
    (r'\brevenue\s+(growth|grew|rises?|up)\b',                           3, "revenue growth"),
    (r'\bmargin\s+expan(sion|d|ding)\b',                                 3, "margin expansion"),
    (r'\bstrong\s+order\s+book\b|\border\s+(book\s+(grows?|grew)|pipeline)\b', 3, "strong order book"),
    (r'\border\s+inflow\b',                                              3, "order inflow"),
    (r'\bqip\b|\bqualified\s+institutional\s+placement\b',               3, "qip"),
    (r'\bpreferential\s+allotment\b',                                    3, "preferential allotment"),
    (r'\bncd\s+(issue|allotment|raised)\b|\bnon.?convertible\s+(debt|securities|debenture)\s+(issued?|allotted?)\b', 3, "ncd issue"),
    (r'\brights?\s+entitlement\b',                                       3, "rights entitlement"),
    (r'\bfund\s*(raise|raising|raised)\b|\bprivate\s+placement\b',       3, "fundraise/private placement"),
    (r'\bipo\s+(opens?|subscri|listed?)\b',                              3, "ipo"),
    (r'\b(launches?|launched|launching)\s+(india.?s?\s+first|world.?s?\s+first)\b', 3, "launches India's/world's first"),
    (r'\bsingle.?window\s+approval\b',                                   3, "single-window approval system"),

    # ── LIGHT BUY (+1) ──────────────────────────────────────────────────
    (r'\bmemorandum\s+of\s+understanding\b|\bmou\s+(signed|executed|entered)\b', 1, "mou signed"),
    (r'\bletter\s+of\s+intent\b|\bloi\s+(signed|executed)\b',            1, "letter of intent"),
    (r'\bnew\s+product\s+(launch|launched)\b|\bproduct\s+(launch|launched)\b', 1, "product launch"),
    (r'\bnew\s+vertical\b|\bmarket\s+expan(sion|d)\b',                    1, "market expansion"),
    (r'\bexport\s+(order|contract)\b',                                   1, "export order"),
    (r'\bdistribution\s+agreement\b|\btie.?up\b',                        1, "distribution agreement/tie-up"),
    (r'\bempanell?ed\b|\bregistered\s+vendor\b',                         1, "empanelled/registered vendor"),
    (r'\bappointment\s+of\s+(managing|joint\s+managing|executive)\s+director\b', 1, "appointment of md/jmd"),
    (r'\bnew\s+credit\s+rating\b|\bcredit\s+rating.{0,20}(assigned|obtained|received)\b', 1, "new credit rating"),
]

SELL_PATTERNS = [
    # ── STRONG SELL (-6) ────────────────────────────────────────────────
    (r'\bsebi\s+(order|action|notice|penalty|ban|restraint|investigation)\s+(against|on|to)\b', -6, "sebi action against"),
    (r'\bsebi\s+show\s+cause\s+notice\b',                                -6, "sebi show cause"),
    (r'\bsebi\s+investigation\b',                                        -6, "sebi investigation"),
    (r'\bfraud\s+(detected|alleged|committed|found)\b',                  -6, "fraud detected/alleged"),
    (r'\baccounting\s+irregularities?\b',                                -6, "accounting irregularities"),
    (r'\bforensic\s+(audit|investigation)\b',                            -6, "forensic audit/investigation"),
    (r'\bmisappropriat(e|ion|ing)\b|\bembezzl(e|ement|ing)\b',           -6, "misappropriation/embezzlement"),
    (r'\bfalsif(y|ied|ication)\s+of\s+(accounts?|records?|books?)\b',    -6, "falsification of accounts"),
    (r'\bnclt\s+admits?\b|\binsolvency\s+petition\s+admit(ted)?\b',      -6, "nclt admits insolvency"),
    (r'\bcorporate\s+insolvency\s+resolution\b|\bcirp\b',                -6, "cirp/insolvency"),
    (r'\bdefault\s+on\s+(ncd|debenture|loan|bond|repayment)\b',          -6, "default on ncd/loan"),
    (r'\bloan\s+default\b|\bpayment\s+default\b',                        -6, "loan/payment default"),
    (r'\bwilful\s+default(er)?\b',                                       -6, "wilful defaulter"),
    (r'\baccount\s+classified\s+(as\s+)?npa\b|\bdeclared\s+(as\s+)?npa\b|\bnpa\s+classification\b', -6, "npa"),
    (r'\bfirst\s+meeting\s+of\s+committee\s+of\s+creditors\b|\bcoc\s+meeting\b', -6, "committee of creditors"),
    (r'\bauditor\s+(resign(s|ed|ation)|quit(s|ting))\b',                 -6, "auditor resignation"),
    (r'\bgoing\s+concern\s+(doubt|qualif|disclaim)\b',                   -6, "going concern doubt"),
    (r'\b(qualified|adverse|disclaim)\w*\s+(audit\s+)?opinion\b',        -6, "qualified/adverse audit opinion"),
    (r'\bpledge\s+(invok|trigger)\w+\b|\bpledged\s+shares\s+invok\w+\b', -6, "pledge invoked"),
    (r'\bmargin\s+call\s+trigger\w+\b',                                  -6, "margin call triggered"),
    (r'\bpromoter\s+pledge\s+(rises?\s+sharply|increases?\s+significantly)\b', -6, "promoter pledge rises"),

    # ── MEDIUM SELL (-3) ────────────────────────────────────────────────
    (r'\b(credit\s+)?rating\s+downgrad\w+\b',                            -3, "rating downgraded"),
    (r'\boutlook\s+revis\w+\s+to\s+(negative|watch)\b',                  -3, "outlook revised negative"),
    (r'\bplaced\s+on\s+(credit\s+)?watch\s+(negative|developing)\b',     -3, "placed on watch negative"),
    (r'\bloss\s+widen(s|ed|ing)\b',                                      -3, "loss widens"),
    (r'\bnet\s+loss\s+(report|record|post)\w+\b',                        -3, "net loss reported"),
    (r'\bearnings?\s+miss\b',                                            -3, "earnings miss"),
    (r'\bprofit\s+(falls?|declin\w+|drops?)\s+(sharply|significantly)?\b', -3, "profit falls"),
    (r'\brevenue\s+(declin\w+|falls?|drops?|contracts?)\b',              -3, "revenue declines"),
    (r'\bmargin\s+contracts?\b|\bebitda\s+(declin\w+|falls?|drops?)\b',  -3, "margin/ebitda declines"),
    (r'\bproduction\s+(halt|shutdown|suspend\w+|stopped)\b',             -3, "production halt/shutdown"),
    (r'\bplant\s+(shut\s*down|closed?|suspend\w+)\b',                    -3, "plant shutdown"),
    (r'\bfactory\s+fire\b|\bforce\s+majeure\s+(declar\w+|invok\w+)\b',  -3, "factory fire/force majeure"),
    (r'\boperations?\s+(suspend\w+|halt\w+|stopped)\b',                  -3, "operations suspended"),
    (r'\bgovernance\s+(concern|issue|lapse)\b',                          -3, "governance concern"),
    (r'\bpromoter\s+conflict\b|\bboard\s+disput(e|ing)\b',               -3, "promoter conflict/board dispute"),
    (r'\b(ceo|md|cfo|coo|chairman)\s+resign(s|ed|ation)\b',              -3, "ceo/md/cfo resigns"),
    (r'\bkey\s+management\s+resignation\b|\bmass\s+resignation\b',       -3, "key management resignation"),
    (r'\bindependent\s+director\s+resign(s|ed|ation)\b',                 -3, "independent director resigns"),
    (r'\b(ed|cbi|income\s*tax)\s+raid\b|\bsearch\s+and\s+seizure\b',    -3, "ed/cbi/it raid"),
    (r'\bassets?\s+attach\w+\b|\battachment\s+order\b',                  -3, "assets attached"),
    (r'\bresignation\s+of\s+(director|kmp|smp|company\s+secretary|compliance\s+officer)\b', -3, "resignation of director/kmp"),

    # ── LIGHT SELL (-1) ─────────────────────────────────────────────────
    (r'\bpromoter\s+(sells?|sold|reduc\w+|offload\w+)\s+(shares?|stake)\b', -1, "promoter sells shares"),
    (r'\bbulk\s+deal\s+(sell|sold|offload)\b',                           -1, "bulk deal sold"),
    (r'\bmargin\s+pressure\b',                                           -1, "margin pressure"),
    (r'\bguidance\s+(cut|lower\w+|revis\w+\s+down)\b',                  -1, "guidance cut"),
    (r'\bpenalty\s+(impos\w+|levied?)\b',                                -1, "penalty imposed"),
    (r'\bfine\s+(impos\w+|levied?)\s+by\b',                              -1, "fine imposed by"),
    (r'\blitigation\s+(pending|filed|against)\b',                        -1, "litigation"),
    (r'\b(legal|regulatory|show\s*cause|demand)\s+notice\s+(receiv\w+|issu\w+)\b', -1, "legal/regulatory notice"),
    (r'\btax\s+demand\s+(rais\w+|receiv\w+|issu\w+)\b',                -1, "tax demand"),
    (r'\bcontingent\s+liability\b',                                      -1, "contingent liability"),
]

IGNORE_PATTERNS = [
    r'\bboard\s+meeting\s+(intimation|scheduled|notice)\b',
    r'\bpostal\s+ballot\b',
    r'\b(agm|egm)\s+(notice|on|scheduled)\b',
    r'\binvestor\s+meet\b|\banalyst\s+meet\b|\bearnings?\s+(call|conference\s+call)\b',
    r'\btrading\s+window\s+(clos\w+|open\w+|shall)\b',
    r'\bclarification\s+(sought|submitted|given)\b',
    r'\bnewspaper\s+publication\b|\bnewspaper\s+advertisement\b',
    r'\bsaksham\s+niveshak\b',
    r'\bchange\s+of\s+(registered\s+)?address\b',
    r'\bbook\s+closure\b',
    r'\brecord\s+date\s+for\s+dividend\b',
    r'\b(interim|final)\s+dividend\b|\bdividend\s+payment\b|\bdividend\s+of\s+rs\b',
    r'\bloss\s+of\s+share\s+certificate\b|\bduplicate\s+share\s+certificate\b',
    r'\btransmission\s+of\s+shares\b',
    r'\breg(ulation)?\s*(74|40|7)\b',
    r'\blarge\s+corporate\s+(disclosure|criteria|entity)\b',
    r'\bformat\s+of\s+(initial|annual)\s+disclosure\b',
    r'\bsecretarial\s+compliance\s+report\b',
    r'\bmonthly\s+reporting\b',
    r'\bchange\s+in\s+kmp\b',
    r'\bscrutinizer\s+report\b|\bvoting\s+result\b|\be-?voting\b',
    r'\bemployee\s+stock\s+option\b|\besop\b',
    r'\bpublic\s+notice\b',
    r'\bconference\s+call\s+(invitation|scheduled)\b',
    r'\bnot\s+a?\s+large\s+corporate\b|\bdoes\s+not\s+fall\s+under\b',
    r'\bnon.?applicability\b',
    r'\besg\s+rating\b',
    r'\bintimation\s+of\s+postal\s+ballot\b',
    r'\btenure\s+of\b',
    r'\binternal\s+reorgani[sz]ation\b',
    r'\bpost\s+offer\s+advertisement\b',
]

_BUY_COMPILED = [(re.compile(p, re.IGNORECASE), sc, lbl) for p, sc, lbl in BUY_PATTERNS]
_SELL_COMPILED = [(re.compile(p, re.IGNORECASE), sc, lbl) for p, sc, lbl in SELL_PATTERNS]
_IGNORE_COMPILED = [re.compile(p, re.IGNORECASE) for p in IGNORE_PATTERNS]

# =============================
# DYNAMIC TICKER EXTRACTION
# =============================
def extract_symbol_advanced(source, row, full_text):
    if source == "nse":
        return row[0].strip().upper() if row[0].strip() else None
    elif source == "bse":
        return row[1].strip().upper() if len(row) > 1 and row[1].strip() else None
    
    # Target MONC and ET Headlines
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
# CONTEXT & SENTENCE ENGINE
# =============================
def contextual_event_score(text):
    # Check absolute bypass rules
    for pat in _IGNORE_COMPILED:
        if pat.search(text):
            return 0, 0, []

    buy_score, sell_score = 0, 0
    reasons = []

    # Tokenize by hard stops ONLY (protects grammar constraints like commas)
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
# MAGNITUDE AMPLIFIER
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
# SIGNAL MATHEMATICS
# =============================
def compute_confidence(buy_raw, sell_raw):
    buy_conf = min(100, int((buy_raw / BUY_100_SCORE) * 100)) if buy_raw > 0 else 0
    sell_conf = min(100, int((sell_raw / SELL_100_SCORE) * 100)) if sell_raw < 0 else 0
    return buy_conf, sell_conf

def get_signal_label(conf, direction):
    if conf < BUY_CONF_THRESHOLD: return None
    if direction == "BUY": return "STRONG BUY 🟢🟢" if conf >= 90 else "BUY 🟢"
    if direction == "SELL": return "STRONG SELL 🔴🔴" if conf >= 90 else "SELL 🔴"
    return None

# =============================
# MAIN PROCESSOR
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
        
        # Apply magnitude multiplier
        m = money_score(text)
        weight = SOURCE_WEIGHT.get(source, 1)
        
        b_total = (b + m) * weight if b > 0 else 0
        s_total = (s - m) * weight if s < 0 else 0

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
            continue # Suppress mixed signals

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
