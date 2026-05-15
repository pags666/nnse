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
def clean_text(text):

    text = text.lower()

    text = re.sub(r'[^a-z0-9 ]', ' ', text)

    text = " ".join(text.split())

    return text  

# =========================================================
# CONFIG
# =========================================================
SHEET_ID = "1le7tQxVkznMvphgOB2T0tGyzb_ByeaOHJ4R9E5piY_A"

INPUT_SHEETS = [
    "nse",
    "bse",
    "monc",
    "et"
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

    else:
        continue

    if not company:
        continue

    if not news:
        continue

    cleaned_news = clean_text(news)

    key = (
        company,
        cleaned_news[:120]
    )
    
    if key not in seen:
    
        company_news[company].append(news)
    
        seen.add(key)


# =========================================================
# IGNORE KEYWORDS
# =========================================================
IGNORE_KEYWORDS = [

 "conference call",
    "investor meet",
    "transcript",
    "press release",
    "media release",
    "disclosure",
    "certificate under regulation",
    "secretarial compliance",
    "newspaper cutting",
    "corrigendum",
    "agm notice",
    "egm notice",
    "voting",
    "scrutinizer report",
    "authorized capital",
    "loss of share certificate",
    "change in name",
    "change in registered office",
    "trading approval",
    "notice of postal ballot",
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
    "re-appointment",
    "appointment",
    "cessation",
    "board meeting",
    "outcome of board meeting",
    "financial statement",
    "compliance certificate",
    "newspaper advertisement",
    "postal ballot notice",
    "esop",
    "intimation",
    "updates",
    "closure of trading window",
]


# =========================================================
# EVENT MAP
# =========================================================
EVENT_SCORES = {

    # =================================================
    # VERY BULLISH
    # =================================================
    "received order": 90,
    "bagging order": 90,
    "letter of award": 90,
    "export order": 95,
    "strategic partnership": 75,
    "capacity expansion": 80,
    "commercial production": 85,
    "commissioning": 80,
    "buyback": 95,
    "acquisition": 70,
    "usfda approval": 90,
    "approval received": 85,

    # =================================================
    # EARNINGS
    # =================================================
    "revenue growth": 50,
    "profit increase": 60,
    "ebitda growth": 60,
    "margin expansion": 70,

    # =================================================
    # VERY BEARISH
    # =================================================
    "fraud": -100,
    "default": -100,
    "bankruptcy": -100,
    "insolvency": -100,
    "auditor resignation": -95,
    "sebi action": -90,
    "pledged shares": -70,
    "downgrade": -60,
    "loss increase": -75,
    "fire accident": -80,
    "plant shutdown": -85,
    "income tax raid": -90,
    "arrested": -95,
}

# =========================================================
# CLASSIFY NEWS
# =========================================================
def classify_news(news_list):

    text = " ".join(news_list).lower()

    # ================================================
    # IGNORE NOISE
    # ================================================
    for word in IGNORE_KEYWORDS:

        if word in text:
            return 0

    score = 0

    for keyword, value in EVENT_SCORES.items():

        if keyword in text:
            score += value

    return score
def extract_order_value(text):

    text = text.lower()

    patterns = [

        r'rs\.?\s?([\d,]+)\s?crore',
        r'₹\s?([\d,]+)\s?crore',
        r'order worth rs\.?\s?([\d,]+)',
        r'worth rs\.?\s?([\d,]+)',
        r'usd\s?([\d,.]+)\s?million',
    ]

    for pattern in patterns:

        match = re.search(pattern, text)

        if match:

            try:
                return float(
                    match.group(1).replace(",", "")
                )
            except:
                pass

    return 0

def extract_growth(text):

    text = text.lower()

    patterns = [

        r'([\d.]+)%\s?revenue growth',
        r'([\d.]+)%\s?profit growth',
        r'([\d.]+)%\s?ebitda growth',
        r'pat growth of\s?([\d.]+)%'
    ]

    growths = []

    for pattern in patterns:

        matches = re.findall(pattern, text)

        for m in matches:

            try:
                growths.append(float(m))
            except:
                pass

    if growths:
        return max(growths)

    return 0


# =========================================================
# AI ANALYSIS
# =========================================================
def analyze(company, news_list):

    combined_news = "\n".join(news_list[:3])
    order_value = extract_order_value(combined_news)
    growth = extract_growth(combined_news)

    prompt = f"""
You are an institutional equity research analyst.

Analyze whether the following news is likely to create a significant short-term stock price movement.

Company:
{company}

News:
{combined_news}

IMPORTANT RULES:

1. Ignore routine filings and weak announcements.
2. Most news should result in NO TRADE.
3. Only generate BUY or SELL if the event is genuinely material.
4. Consider:
   - order size
   - earnings impact
   - strategic importance
   - regulatory impact
   - financial risk
   - whether the event is transformational
5. Be conservative.
6. Avoid hype.
7. Never assume future growth without evidence.

Strong BUY examples:
- very large order
- major acquisition
- major regulatory approval
- exceptional earnings surprise
- buyback
- major expansion

Strong SELL examples:
- fraud
- bankruptcy
- auditor resignation
- insolvency
- severe losses
- plant shutdown
- regulatory crackdown

Return ONLY valid JSON.

{
    "action": "BUY",
    "confidence": 85,
    "impact": "HIGH",
    "reason": "large export order relative to company scale"
}

OR

{
    "action": "SELL",
    "confidence": 90,
    "impact": "HIGH",
    "reason": "auditor resignation raises governance concerns"
}

OR

{
    "action": "NO TRADE",
    "confidence": 20,
    "impact": "LOW",
    "reason": "non-material or routine announcement"
}
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

    score = classify_news(news_list)

    if score == 0:
        continue

    try:
    
        combined_news = " ".join(news_list)
    
        # ================================================
        # MATERIALITY CHECK
        # ================================================
        order_value = extract_order_value(
            combined_news
        )
    
        growth = extract_growth(
            combined_news
        )
    
        # ================================================
        # FILTER SMALL ORDERS
        # ================================================
        if "order" in combined_news.lower():
    
            if order_value < 50:
                continue
    
        # ================================================
        # FILTER WEAK GROWTH
        # ================================================
        if "growth" in combined_news.lower():
    
            if growth < 20:
                continue
    
        # ================================================
        # AI ANALYSIS
        # ================================================
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

        confidence = data.get(
            "confidence",
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
        # ONLY EXTREME CONFIDENCE
        # =================================================
        if action == "NO TRADE":
            continue

        if confidence < 80:
            continue

        if action == "BUY" and confidence >= 90:
            pass
        elif action == "SELL" and confidence >= 85:
            pass
        else:
            continue

        # =================================================
        # APPEND
        # =================================================
        results.append([
            company,
            confidence,
            action,
            reason
        ])

    except Exception as e:

        print(f"❌ Error for {company}: {e}")


# =========================================================
# SORT RESULTS
# =========================================================
results.sort(

    key=lambda x: (

        x[2] == "BUY",
        x[1]

    ),

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
# =========================================================
# APPEND RESULTS
# =========================================================
if not results:

    print("❌ NO OUTPUT SIGNALS FOUND")

else:

    print("\n✅ FINAL SIGNALS:\n")

    for row in results:

        print(row)

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
# FORMAT TIMESTAMP
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
