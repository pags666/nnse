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
# JSON EXTRACTOR
# =========================================================
def extract_json(text):

    try:
        match = re.search(r'\{.*\}', text, re.DOTALL)

        if not match:
            return None

        return json.loads(match.group())

    except Exception:
        return None


# =========================================================
# CONFIG
# =========================================================
SHEET_ID = "1le7tQxVkznMvphgOB2T0tGyzb_ByeaOHJ4R9E5piY_A"

INPUT_SHEETS = [
    "nse",
    "bse"
]

OUTPUT_WS = "groq"


# =========================================================
# GOOGLE SHEETS AUTH
# =========================================================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    "service_account.json",
    scope
)

client = gspread.authorize(creds)

spreadsheet = client.open_by_key(SHEET_ID)


# =========================================================
# OUTPUT SHEET
# =========================================================
try:

    output_ws = spreadsheet.worksheet(OUTPUT_WS)

except:

    output_ws = spreadsheet.add_worksheet(
        title=OUTPUT_WS,
        rows="1000",
        cols="20"
    )


# =========================================================
# GROQ
# =========================================================
groq = Groq(
    api_key=os.environ["GROQ_API_KEY"]
)


# =========================================================
# READ DATA
# =========================================================
all_rows = []

for sheet_name in INPUT_SHEETS:

    try:

        ws = spreadsheet.worksheet(sheet_name)

        data = ws.get_all_records()

        print(f"✅ Loaded {sheet_name}: {len(data)} rows")

        for row in data:

            row["EXCHANGE"] = sheet_name.upper()

            all_rows.append(row)

    except Exception as e:

        print(f"❌ Error loading {sheet_name}: {e}")


# =========================================================
# GROUP NEWS
# =========================================================
company_news = defaultdict(list)

seen = set()

for row in all_rows:

    company = ""
    news = ""

    # =====================================================
    # NSE
    # =====================================================
    if "DETAILS" in row:

        company = str(
            row.get("SYMBOL", "")
        ).strip().upper()

        news = str(
            row.get("DETAILS", "")
        ).strip()

    # =====================================================
    # BSE
    # =====================================================
    elif "ANNOUNCEMENT" in row:

        company = str(
            row.get("COMPANY NAME", "")
        ).strip().upper()

        news = str(
            row.get("ANNOUNCEMENT", "")
        ).strip()

    if not company:
        continue

    if not news:
        continue

    key = (company, news)

    if key not in seen:

        company_news[company].append(news)

        seen.add(key)


# =========================================================
# IGNORE KEYWORDS
# =========================================================
IGNORE_KEYWORDS = [

    # compliance
    "scrutinizer",
    "certificate",
    "postal ballot",
    "agm",
    "newspaper publication",
    "trading window",
    "shareholding pattern",
    "voting results",
    "analyst meeting",
    "investor presentation",
    "record date",
    "book closure",
    "committee meeting",
    "clarification",
    "corporate action",

    # appointments
    "re-appointment",
    "appointment",
    "cessation",

    # meetings
    "board meeting",
    "outcome of board meeting",

    # filing noise
    "financial statement",
    "compliance certificate",
    "newspaper advertisement",
    "postal ballot notice",
    "esop",
    "intimation",
    "updates",
]


# =========================================================
# EVENT MAP
# =========================================================
EVENT_MAP = {

    # =====================================================
    # STRONG BUY EVENTS
    # =====================================================
    "received order": "ORDER",
    "bagging order": "ORDER",
    "work order": "ORDER",
    "letter of award": "ORDER",
    "large order": "ORDER",
    "major order": "ORDER",
    "export order": "ORDER",
    "contract": "ORDER",

    "acquisition": "ACQUISITION",
    "stake acquisition": "ACQUISITION",

    "buyback": "BUYBACK",

    "capacity expansion": "EXPANSION",
    "commercial production": "EXPANSION",
    "commissioning": "EXPANSION",
    "plant expansion": "EXPANSION",

    "strategic partnership": "PARTNERSHIP",
    "joint venture": "PARTNERSHIP",

    "profit increase": "EARNINGS",
    "margin expansion": "EARNINGS",
    "revenue growth": "EARNINGS",
    "ebitda growth": "EARNINGS",
    "strong earnings": "EARNINGS",

    "dividend": "DIVIDEND",

    "approval received": "APPROVAL",

    "debt reduction": "DEBT_REDUCTION",

    # =====================================================
    # STRONG SELL EVENTS
    # =====================================================
    "auditor resignation": "AUDITOR_RISK",

    "insolvency": "INSOLVENCY",
    "nclt": "INSOLVENCY",

    "default": "DEFAULT",

    "fraud": "FRAUD",

    "sebi action": "REGULATORY",
    "penalty": "REGULATORY",

    "bankruptcy": "BANKRUPTCY",

    "loss increase": "WEAK_EARNINGS",

    "pledged shares": "PLEDGE_RISK",

    "downgrade": "DOWNGRADE",
}


# =========================================================
# BUY EVENTS
# =========================================================
BUY_EVENTS = [

    "ORDER",
    "ACQUISITION",
    "BUYBACK",
    "EXPANSION",
    "PARTNERSHIP",
    "EARNINGS",
    "DIVIDEND",
    "APPROVAL",
    "DEBT_REDUCTION"
]


# =========================================================
# SELL EVENTS
# =========================================================
SELL_EVENTS = [

    "AUDITOR_RISK",
    "INSOLVENCY",
    "DEFAULT",
    "FRAUD",
    "REGULATORY",
    "BANKRUPTCY",
    "WEAK_EARNINGS",
    "PLEDGE_RISK",
    "DOWNGRADE"
]


# =========================================================
# EVENT PROBABILITY
# =========================================================
EVENT_PROBABILITY = {

    # BUY
    "ORDER": 100,
    "ACQUISITION": 100,
    "BUYBACK": 100,
    "EXPANSION": 100,
    "PARTNERSHIP": 100,
    "EARNINGS": 100,
    "DIVIDEND": 100,
    "APPROVAL": 100,
    "DEBT_REDUCTION": 100,

    # SELL
    "AUDITOR_RISK": 100,
    "INSOLVENCY": 100,
    "DEFAULT": 100,
    "FRAUD": 100,
    "REGULATORY": 100,
    "BANKRUPTCY": 100,
    "WEAK_EARNINGS": 100,
    "PLEDGE_RISK": 100,
    "DOWNGRADE": 100,
}


# =========================================================
# CLASSIFY NEWS
# =========================================================
def classify_news(news_list):

    text = " ".join(news_list).lower()

    # =====================================================
    # IGNORE
    # =====================================================
    for word in IGNORE_KEYWORDS:

        if word in text:

            return "IGNORE"

    # =====================================================
    # EVENT DETECTION
    # =====================================================
    for keyword, event_type in EVENT_MAP.items():

        if keyword in text:

            return event_type

    return "UNKNOWN"


# =========================================================
# AI ANALYSIS
# =========================================================
def analyze(company, news_list):

    combined_news = "\n".join(news_list[:3])

    prompt = f"""
You are a professional stock market event analyst.

Analyze the following stock market news carefully.

Company:
{company}

News:
{combined_news}

Your task:

1. Detect whether the event is materially bullish or bearish.
2. Ignore compliance and useless filing noise.
3. Focus ONLY on major price-moving events.

Bullish events:
- large orders
- acquisitions
- earnings growth
- margin expansion
- capacity expansion
- strategic partnerships
- debt reduction
- approvals

Bearish events:
- insolvency
- fraud
- defaults
- auditor resignation
- sebi action
- large losses
- bankruptcy

Return ONLY valid JSON.

NO markdown.
NO explanations.
NO paragraphs.

Valid JSON format:

{{
    "probability": 100,
    "action": "BUY",
    "reason": "short reason"
}}

OR

{{
    "probability": 100,
    "action": "SELL",
    "reason": "short reason"
}}

OR

{{
    "probability": 0,
    "action": "NO TRADE",
    "reason": "no meaningful trigger"
}}
"""

    response = groq.chat.completions.create(

        model="llama-3.1-8b-instant",

        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ],

        temperature=0
    )

    content = response.choices[0].message.content.strip()

    content = content.replace("```json", "")
    content = content.replace("```", "")
    content = content.strip()

    return content


# =========================================================
# PROCESS
# =========================================================
results = []

for company, news_list in company_news.items():

    if not news_list:
        continue

    signal = classify_news(news_list)

    # =====================================================
    # IGNORE
    # =====================================================
    if signal == "IGNORE":

        continue

    try:

        # =================================================
        # RULE BASED BUY
        # =================================================
        if signal in BUY_EVENTS:

            results.append([
                company,
                EVENT_PROBABILITY.get(signal, 100),
                "BUY",
                signal
            ])

            continue

        # =================================================
        # RULE BASED SELL
        # =================================================
        elif signal in SELL_EVENTS:

            results.append([
                company,
                EVENT_PROBABILITY.get(signal, 100),
                "SELL",
                signal
            ])

            continue

        # =================================================
        # AI ANALYSIS
        # =================================================
        ai_output = analyze(
            company,
            news_list
        )

        if not ai_output:
            continue

        data = extract_json(ai_output)

        if not data:

            print(f"❌ Invalid JSON for {company}")

            print(ai_output)

            continue

        probability = data.get(
            "probability",
            0
        )

        action = data.get(
            "action",
            "NO TRADE"
        )

        reason = data.get(
            "reason",
            ""
        )

        # =================================================
        # FINAL FILTER
        # =================================================
        if action in ["BUY", "SELL"]:

            results.append([
                company,
                probability,
                action,
                reason
            ])

    except Exception as e:

        print(f"❌ Error for {company}: {e}")


# =========================================================
# SORT RESULTS
# =========================================================
results.sort(
    key=lambda x: x[1],
    reverse=True
)

results = results[:10]


# =========================================================
# WRITE OUTPUT
# =========================================================
existing_data = output_ws.get_all_values()

if not existing_data:

    output_ws.append_row([
        "Company",
        "Probability %",
        "Action",
        "Reason"
    ])


# =========================================================
# APPEND RESULTS
# =========================================================
for row in results:

    output_ws.append_row(row)


# =========================================================
# TIMESTAMP
# =========================================================
ist_time = datetime.now(
    ZoneInfo("Asia/Kolkata")
).strftime("%Y-%m-%d %H:%M:%S")

output_ws.append_row([
    "Updated (IST)",
    ist_time
])


# =========================================================
# FORMAT
# =========================================================
last_row = len(
    output_ws.get_all_values()
)

output_ws.format(

    f"A{last_row}:B{last_row}",

    {
        "backgroundColor": {
            "red": 1,
            "green": 0.9,
            "blue": 1
        },

        "textFormat": {
            "bold": True
        }
    }
)

print("✅ COMPLETED SUCCESSFULLY")
