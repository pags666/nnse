import os
import json
import re
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from groq import Groq
from oauth2client.service_account import ServiceAccountCredentials

# =========================
# JSON EXTRACTOR
# =========================
def extract_json(text):

    match = re.search(r'\{.*?\}', text, re.DOTALL)

    if not match:
        return None

    try:
        return json.loads(match.group())

    except Exception:
        return None


# =========================
# CONFIG
# =========================
SHEET_ID = "1le7tQxVkznMvphgOB2T0tGyzb_ByeaOHJ4R9E5piY_A"

INPUT_SHEETS = [
    "nse",
    "bse"
]

OUTPUT_WS = "groq"


# =========================
# GOOGLE SHEETS AUTH
# =========================
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


# =========================
# OUTPUT SHEET
# =========================
try:
    output_ws = spreadsheet.worksheet(OUTPUT_WS)

except:
    output_ws = spreadsheet.add_worksheet(
        title=OUTPUT_WS,
        rows="100",
        cols="20"
    )


# =========================
# GROQ API
# =========================
groq = Groq(
    api_key=os.environ["GROQ_API_KEY"]
)


# =========================
# READ DATA FROM NSE + BSE
# =========================
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


# =========================
# GROUP NEWS
# =========================
company_news = defaultdict(list)

seen = set()

for row in all_rows:

    # =========================
    # NSE
    # =========================
    if "DETAILS" in row:

        company = str(
            row.get("SYMBOL", "")
        ).strip().upper()

        news = row.get("DETAILS", "")

    # =========================
    # BSE
    # =========================
    elif "ANNOUNCEMENT" in row:

        company = str(
            row.get("COMPANY NAME", "")
        ).strip().upper()

        news = row.get("ANNOUNCEMENT", "")

    else:
        continue

    key = (company, news)

    if company and news and key not in seen:

        company_news[company].append(news)

        seen.add(key)


# =========================
# IGNORE KEYWORDS
# =========================
IGNORE_KEYWORDS = [

    "scrutinizer",
    "certificate",
    "postal ballot",
    "agm",
    "newspaper",
    "trading window",
    "esop",
    "compliance",
    "shareholding pattern",
    "voting results",
    "analyst meeting",
    "investor presentation",
    "record date",
    "book closure",
    "committee meeting",
    "clarification",
]


# =========================
# EVENT MAP
# =========================
EVENT_MAP = {

    # =========================
    # BUY EVENTS
    # =========================
    "received order": "ORDER",
    "bagging order": "ORDER",
    "work order": "ORDER",
    "letter of award": "ORDER",
    "contract": "ORDER",

    "acquisition": "ACQUISITION",

    "buyback": "BUYBACK",

    "capacity expansion": "EXPANSION",
    "commissioning": "EXPANSION",

    "strategic partnership": "PARTNERSHIP",

    "profit increase": "EARNINGS",
    "margin expansion": "EARNINGS",
    "revenue growth": "EARNINGS",

    # =========================
    # SELL EVENTS
    # =========================
    "auditor resignation": "AUDITOR_RISK",

    "insolvency": "INSOLVENCY",

    "default": "DEFAULT",

    "fraud": "FRAUD",

    "sebi action": "REGULATORY",

    "bankruptcy": "BANKRUPTCY",

    "loss increase": "WEAK_EARNINGS",
}


# =========================
# BUY EVENTS
# =========================
BUY_EVENTS = [

    "ORDER",
    "ACQUISITION",
    "BUYBACK",
    "EXPANSION",
    "PARTNERSHIP",
    "EARNINGS"
]


# =========================
# SELL EVENTS
# =========================
SELL_EVENTS = [

    "AUDITOR_RISK",
    "INSOLVENCY",
    "DEFAULT",
    "FRAUD",
    "REGULATORY",
    "BANKRUPTCY",
    "WEAK_EARNINGS"
]


# =========================
# CLASSIFY NEWS
# =========================
def classify_news(news_list):

    text = " ".join(news_list).lower()

    # =========================
    # IGNORE NOISE
    # =========================
    for word in IGNORE_KEYWORDS:

        if word in text:

            return "IGNORE"

    # =========================
    # EVENT DETECTION
    # =========================
    for keyword, event_type in EVENT_MAP.items():

        if keyword in text:

            return event_type

    return "UNKNOWN"


# =========================
# AI ANALYSIS
# =========================
def analyze(company, news_list):

    combined_news = "\n".join(news_list[:3])

    prompt = f"""
You are a stock market event analyst.

Company:
{company}

News:
{combined_news}

Your job:

1. Detect whether news is materially positive or negative.
2. Ignore compliance filings and useless updates.
3. Focus ONLY on:
   - large orders
   - earnings growth
   - acquisitions
   - insolvency
   - fraud
   - defaults
   - auditor resignation
   - margin expansion
   - large contracts

Return ONLY JSON:

{{
    "probability": 0-100,
    "action": "BUY / SELL / NO TRADE",
    "reason": "short factual reason"
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

        temperature=0.1
    )

    return response.choices[0].message.content.strip()


# =========================
# PROCESS
# =========================
results = []

for company, news_list in company_news.items():

    signal = classify_news(news_list)

    # =========================
    # IGNORE NOISE
    # =========================
    if signal == "IGNORE":

        continue

    try:

        # =========================
        # RULE BASED BUY
        # =========================
        if signal in BUY_EVENTS:

            results.append([
                company,
                80,
                "BUY",
                signal
            ])

            continue

        # =========================
        # RULE BASED SELL
        # =========================
        elif signal in SELL_EVENTS:

            results.append([
                company,
                80,
                "SELL",
                signal
            ])

            continue

        # =========================
        # AI FOR UNKNOWN
        # =========================
        ai_output = analyze(
            company,
            news_list
        )

        if not ai_output:
            continue

        ai_output = ai_output.replace(
            "```json",
            ""
        ).replace(
            "```",
            ""
        ).strip()

        data = extract_json(ai_output)

        if not data:

            print(f"❌ Invalid JSON for {company}")

            print("RAW:", ai_output)

            continue

        prob = data.get("probability", 0)

        action = data.get(
            "action",
            "NO TRADE"
        )

        reason = data.get(
            "reason",
            ""
        )

        if action == "BUY" and prob >= 75:

            results.append([
                company,
                prob,
                "BUY",
                reason
            ])

        elif action == "SELL" and prob >= 75:

            results.append([
                company,
                prob,
                "SELL",
                reason
            ])

    except Exception as e:

        print(f"❌ Error for {company}: {e}")


# =========================
# SORT RESULTS
# =========================
results.sort(
    key=lambda x: x[1],
    reverse=True
)

results = results[:5]


# =========================
# WRITE OUTPUT
# =========================
existing_data = output_ws.get_all_values()

if not existing_data:

    output_ws.append_row([
        "Company",
        "Probability %",
        "Action",
        "Reason"
    ])


# =========================
# APPEND RESULTS
# =========================
for row in results:

    output_ws.append_row(row)


# =========================
# TIMESTAMP
# =========================
ist_time = datetime.now(
    ZoneInfo("Asia/Kolkata")
).strftime("%Y-%m-%d %H:%M:%S")


output_ws.append_row([
    "Updated (IST)",
    ist_time
])


# =========================
# FORMAT TIMESTAMP ROW
# =========================
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

print("✅ Completed Successfully")
