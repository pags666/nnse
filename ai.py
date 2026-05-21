import os
import json
import re
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo
import gspread
from groq import Groq
from oauth2client.service_account import ServiceAccountCredentials

# =========================================================
# CONFIG & AUTH
# =========================================================
SHEET_ID = "1le7tQxVkznMvphgOB2T0tGyzb_ByeaOHJ4R9E5piY_A"
INPUT_SHEETS = ["nse", "bse", "monc", "et"]
OUTPUT_WS = "groq"

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)
spreadsheet = client.open_by_key(SHEET_ID)

try:
    output_ws = spreadsheet.worksheet(OUTPUT_WS)
except:
    output_ws = spreadsheet.add_worksheet(title=OUTPUT_WS, rows="1000", cols="20")

groq = Groq(api_key=os.environ["GROQ_API_KEY"])

# =========================================================
# PRO-LEVEL VOCABULARY DICTIONARIES
# =========================================================

BUY_PATTERNS = [
    # Orders & Contracts
    r'\bl1\s*bidder\b', r'\blowest\s+bidder\b', r'\bletter\s+of\s+award\b', 
    r'\bloa\s+(received|issued|awarded)\b', r'\bwork\s+order\s+(received|awarded|secured|worth)\b',
    r'\bcontract\s+(awarded|secured|signed|worth|received)\b', r'\border\s+(secured|received|awarded|bagged|won)\b',
    r'\border(s)?\s+(worth|valued?\s+at|of\s+rs|of\s+inr)\b', r'\b(large|mega|significant|major|repeat)\s+order\b',
    r'\border\s+intak(e|es)\b', r'\bexecut(e|ed|ing)\s+(a\s+)?share\s+purchase\s+agreement\b',
    r'\baquir(e|es|ed|ing)\b|\bacquisition\b', r'\b49\s*%\s*(equity\s+)?stake\b',
    
    # Financial Milestones
    r'\brecord\s+(profit|revenue|sales|earnings|ebitda)\b', r'\bhighest\s+ever\s+(profit|revenue|sales)\b',
    r'\ball.?time\s+high\s+(profit|revenue|sales)\b', r'\bprofit\s+(doubles?|triples?|surges?|jumps?|soars?)\b',
    r'\bnet\s+profit\s+(surges?|jumps?|rises?|up)\b', r'\bebitda\s+(surges?|jumps?|rises?|up|grows?)\b',
    r'\bbeat(s|ing)?\s+(estimates?|expectations?|consensus)\b', r'\brevenue\s+(growth|grew|rises?|up)\b',
    r'\bearnings?\s+beat\b', r'\bmargin\s+expan(sion|d|ding)\b',
    
    # Corporate Actions & Expansion
    r'\bbuy\s*back\b|\bshare\s+buy\s*back\b', r'\bbonus\s+(issue|shares?)\b',
    r'\bstock\s+split\b|\bshare\s+split\b', r'\brights?\s+issue\b',
    r'\bdebt[\s-]?free\b|\bzero\s+debt\b', r'\bdelevera(ge|ging|ged)\b|\bdebt\s+(repaid|cleared|fully\s+paid)\b',
    r'\bpromoter\s+(increases?|buys?|acquires?|purchased?)\s+(stake|shares?)\b', r'\bopen\s+market\s+(purchase|buy)\b',
    r'\bcapacity\s+expan(sion|d|ding)\b', r'\b(brownfield|greenfield)\s+expan(sion|d)\b',
    r'\bnew\s+(manufacturing\s+)?plant\b', r'\bcapex\s+(of|plan|worth|investment)\b',
    r'\bcapacity\s+addition\b', r'\bjoint\s+venture\b|\bjv\s+(agreement|formed|signed)\b',
    r'\bstrategic\s+partner(ship)?\b', r'\bcollabor(ation|ate|ating)\s+(agreement|with)\b',
    r'\btechnology\s+(transfer|agreement|licens(e|ing))\b', r'\bdefinitive\s+agreement\s+(signed|executed)\b',
    
    # Fundraising & Launches
    r'\bqip\b|\bqualified\s+institutional\s+placement\b', r'\bpreferential\s+allotment\b',
    r'\bncd\s+(issue|allotment|raised)\b', r'\bfund\s*(raise|raising|raised)\b|\bprivate\s+placement\b',
    r'\b(launches?|launched|launching)\s+(india.?s?\s+first|world.?s?\s+first)\b',
    r'\bnew\s+product\s+(launch|launched)\b', r'\bexport\s+(order|contract)\b',
    r'\bturnaround\b', r'\breturns?\s+to\s+profit\b|\bback\s+in\s+black\b', r'\bvalue\s+unlocking\b'
]

SELL_PATTERNS = [
    # SEBI, Legal & Fraud
    r'\bsebi\s+(order|action|notice|penalty|ban|restraint|investigation)\s+(against|on|to)\b',
    r'\bsebi\s+show\s+cause\s+notice\b', r'\bfraud\s+(detected|alleged|committed|found)\b',
    r'\baccounting\s+irregularities?\b', r'\bforensic\s+(audit|investigation)\b',
    r'\bmisappropriat(e|ion|ing)\b|\bembezzl(e|ement|ing)\b', r'\bfalsif(y|ied|ication)\s+of\s+(accounts?|records?|books?)\b',
    
    # Default, Insolvency & Ratings
    r'\bnclt\s+admits?\b|\binsolvency\s+petition\s+admit(ted)?\b', r'\bcorporate\s+insolvency\s+resolution\b|\bcirp\b',
    r'\bdefault\s+on\s+(ncd|debenture|loan|bond|repayment)\b', r'\bloan\s+default\b|\bpayment\s+default\b',
    r'\bwilful\s+default(er)?\b', r'\baccount\s+classified\s+(as\s+)?npa\b|\bnpa\s+classification\b',
    r'\bauditor\s+(resign(s|ed|ation)|quit(s|ting))\b', r'\bgoing\s+concern\s+(doubt|qualif|disclaim)\b',
    r'\b(credit\s+)?rating\s+downgrad\w+\b', r'\bplaced\s+on\s+(credit\s+)?watch\s+(negative|developing)\b',
    
    # Financial Misses & Disasters
    r'\bloss\s+widen(s|ed|ing)\b', r'\bnet\s+loss\s+(report|record|post)\w+\b', r'\bearnings?\s+miss\b',
    r'\bprofit\s+(falls?|declin\w+|drops?)\s+(sharply|significantly)?\b', r'\brevenue\s+(declin\w+|falls?|drops?|contracts?)\b',
    r'\bmargin\s+contracts?\b|\bebitda\s+(declin\w+|falls?|drops?)\b',
    r'\bproduction\s+(halt|shutdown|suspend\w+|stopped)\b', r'\bplant\s+(shut\s*down|closed?|suspend\w+)\b',
    r'\bfactory\s+fire\b|\bforce\s+majeure\s+(declar\w+|invok\w+)\b',
    
    # Governance & Equity
    r'\bpledge\s+(invok|trigger)\w+\b|\bpledged\s+shares\s+invok\w+\b', r'\bmargin\s+call\s+trigger\w+\b',
    r'\bgovernance\s+(concern|issue|lapse)\b', r'\bpromoter\s+conflict\b|\bboard\s+disput(e|ing)\b',
    r'\b(ceo|md|cfo|coo|chairman)\s+resign(s|ed|ation)\b', r'\bindependent\s+director\s+resign(s|ed|ation)\b',
    r'\b(ed|cbi|income\s*tax)\s+raid\b|\bsearch\s+and\s+seizure\b', r'\bassets?\s+attach\w+\b',
    r'\bpromoter\s+(sells?|sold|reduc\w+|offload\w+)\s+(shares?|stake)\b', r'\bmargin\s+pressure\b',
    r'\bguidance\s+(cut|lower\w+|revis\w+\s+down)\b', r'\bpenalty\s+(impos\w+|levied?)\b'
]

IGNORE_PATTERNS = [
    r'\bboard\s+meeting\s+(intimation|scheduled|notice)\b', r'\bpostal\s+ballot\b',
    r'\b(agm|egm)\s+(notice|on|scheduled)\b', r'\binvestor\s+meet\b|\banalyst\s+meet\b',
    r'\bearnings?\s+(call|conference\s+call)\b', r'\btrading\s+window\s+(clos\w+|open\w+|shall)\b',
    r'\bclarification\s+(sought|submitted|given)\b', r'\bnewspaper\s+publication\b|\bnewspaper\s+advertisement\b',
    r'\bchange\s+of\s+(registered\s+)?address\b', r'\bbook\s+closure\b', r'\brecord\s+date\s+for\s+dividend\b',
    r'\b(interim|final)\s+dividend\b|\bdividend\s+payment\b|\bdividend\s+of\s+rs\b',
    r'\bloss\s+of\s+share\s+certificate\b|\bduplicate\s+share\s+certificate\b',
    r'\bsecretarial\s+compliance\s+report\b', r'\bscrutinizer\s+report\b|\bvoting\s+result\b|\be-?voting\b',
    r'\bpublic\s+notice\b', r'\bconference\s+call\s+(invitation|scheduled)\b', r'\besg\s+rating\b'
]

_BUY_COMPILED = [re.compile(p, re.IGNORECASE) for p in BUY_PATTERNS]
_SELL_COMPILED = [re.compile(p, re.IGNORECASE) for p in SELL_PATTERNS]
_IGNORE_COMPILED = [re.compile(p, re.IGNORECASE) for p in IGNORE_PATTERNS]

# =========================================================
# HEURISTIC GATEKEEPER
# =========================================================
def passes_pro_filter(text):
    """Checks if the news contains your specified advanced vocabulary."""
    # 1. If it's explicitly routine junk, kill it.
    for pat in _IGNORE_COMPILED:
        if pat.search(text):
            return False
    
    # 2. If it hits ANY of your BUY or SELL dictionary words, pass it to the AI.
    for pat in _BUY_COMPILED:
        if pat.search(text): return True
        
    for pat in _SELL_COMPILED:
        if pat.search(text): return True
        
    return False

# =========================================================
# DATA INGESTION & TICKER CLEANING
# =========================================================
all_rows = []
for sheet_name in INPUT_SHEETS:
    try:
        ws = spreadsheet.worksheet(sheet_name)
        data = ws.get_all_records()
        print(f"✅ Loaded {sheet_name.upper()}: {len(data)} rows")
        for row in data:
            all_rows.append(row)
    except Exception as e:
        print(f"❌ Error loading {sheet_name}: {e}")

company_news = defaultdict(list)
seen = set()

for row in all_rows:
    company = ""
    news = ""

    if "DETAILS" in row:
        company = str(row.get("SYMBOL", "")).strip().upper()
        news = str(row.get("DETAILS", "")).strip()
    elif "ANNOUNCEMENT" in row:
        company = str(row.get("SYMBOL", "")).strip().upper()
        news = str(row.get("ANNOUNCEMENT", "")).strip()
    elif "SUBJECT" in row:
        news = str(row.get("SUBJECT", "")).strip()
        company = " ".join(news.split()[:2]).upper()
    elif "TITLE" in row:
        news = str(row.get("TITLE", "")).strip()
        company = " ".join(news.split()[:2]).upper()

    if not company or not news: continue

    key = (company, news[:150]) 
    if key not in seen:
        company_news[company].append(news)
        seen.add(key)

# =========================================================
# AI VALIDATION ENGINE
# =========================================================
def analyze(company, combined_news):
    prompt = f"""
You are an elite institutional quantitative analyst for the Indian Stock Market.
Evaluate this corporate announcement for immediate, definitive (1-2 days) price action.

Company: {company}
Announcement: {combined_news}

RULES:
1. "BUY": Only if the news provides definitive, material financial upside (e.g. executed acquisition, confirmed massive order, record earnings beat).
2. "SELL": Only if there is structural damage (e.g. fraud, auditor resignation, insolvency, regulatory penalty, board REJECTING a positive action).
3. "NO TRADE": If the news was canceled, rejected, mixed, long-term non-binding MoU, or just a routine update.
4. Confidence MUST be realistic. 85+ means high certainty of gap-up/gap-down.

Return ONLY a valid JSON object exactly like this:
{{
    "action": "BUY | SELL | NO TRADE",
    "confidence": 95,
    "reason": "One specific factual sentence explaining the catalyst."
}}
"""
    try:
        response = groq.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            response_format={"type": "json_object"} # FORCES LLM to output valid JSON
        )
        return json.loads(response.choices[0].message.content.strip())
    except Exception as e:
        print(f"❌ Groq API Error for {company}: {e}")
        return None

# =========================================================
# EXECUTION LOOP
# =========================================================
results = []
print("\n🔍 Scanning news using Advanced Rule-Based Dictionaries...\n")

for company, news_list in company_news.items():
    combined_news = " | ".join(news_list)
    
    # 1. PASS THROUGH PRO-LEVEL REGEX FILTER
    if not passes_pro_filter(combined_news):
        continue

    # 2. SEND VALIDATED HITS TO AI FOR CONTEXT CHECK
    data = analyze(company, combined_news)
    if not data: continue

    action = data.get("action", "NO TRADE").upper()
    confidence = int(data.get("confidence", 0))
    reason = data.get("reason", "").strip()

    # 3. APPEND EXTREME CONFIDENCE ONLY
    if action in ["BUY", "SELL"] and confidence >= 85:
        results.append([company, confidence, f"{action} {'🟢' if action=='BUY' else '🔴'}", reason])
        print(f"{'🟢' if action=='BUY' else '🔴'} {company} -> {action} ({confidence}%)")

# =========================================================
# OUTPUT & EXPORT
# =========================================================
results.sort(key=lambda x: (x[2].startswith("BUY"), x[1]), reverse=True)

existing_data = output_ws.get_all_values()
if not existing_data:
    output_ws.append_row(["Company", "Probability %", "Action", "Reason"])

if not results:
    print("\n❌ NO HIGH-CONFIDENCE SIGNALS MET THE STRICT THRESHOLD.")
else:
    print(f"\n✅ WRITING {len(results)} VALIDATED SIGNALS TO SHEET.")
    for row in results:
        output_ws.append_row(row)

ist_time = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")
output_ws.append_row(["Updated (IST)", ist_time, "", ""])

last_row = len(output_ws.get_all_values())
output_ws.format(f"A{last_row}:D{last_row}", {
    "backgroundColor": {"red": 1, "green": 0.9, "blue": 1},
    "textFormat": {"bold": True}
})

print("\n✅ COMPLETED SUCCESSFULLY")
