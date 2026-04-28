
import os
import json
import math
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from groq import Groq
from oauth2client.service_account import ServiceAccountCredentials
import re

def extract_json(text):
    match = re.search(r'\{.*?\}', text, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group())
    except Exception as e:
        return None
# =========================
# CONFIG
# =========================
SHEET_ID = "1le7tQxVkznMvphgOB2T0tGyzb_ByeaOHJ4R9E5piY_A"
INPUT_SHEETS = ["nse", "bse"]   # ✅ MULTI SHEET
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
    output_ws = spreadsheet.add_worksheet(title=OUTPUT_WS, rows="100", cols="20")

# =========================
# GROQ API
# =========================
groq = Groq(api_key=os.environ["GROQ_API_KEY"])

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
        print(f"❌ Error loading {sheet_name}:", e)

# =========================
# GROUP NEWS (REMOVE DUPLICATES)
# =========================
company_news = defaultdict(list)
seen = set()

for row in all_rows:

    # =========================
    # NSE STRUCTURE
    # =========================
    if "DETAILS" in row:
        company = str(row.get("SYMBOL", "")).strip().upper()
        news = row.get("DETAILS", "")

    # =========================
    # BSE STRUCTURE
    # =========================
    elif "ANNOUNCEMENT" in row:
        company = str(row.get("COMPANY NAME", "")).strip().upper()
        news = row.get("ANNOUNCEMENT", "")

    else:
        continue

    # remove duplicates
    key = (company, news)

    if company and news and key not in seen:
        company_news[company].append(news)
        seen.add(key)
def is_meaningful_news(news_list):
    text = " ".join(news_list).lower()

    important_keywords = [
        "order", "contract", "award", "loa",
        "profit", "revenue", "ebitda",
        "acquisition", "stake", "buyback",
        "expansion", "capex"
    ]

    ignore_keywords = [
        "scrutinizer", "compliance", "certificate",
        "newspaper", "postal ballot", "agm",
        "trading window", "esop"
    ]

    # ❌ ignore junk
   # Remove junk words but don't discard full news
    for k in ignore_keywords:
        text = text.replace(k, "")

    # ✅ must have at least one real trigger
    return any(k in text for k in important_keywords)
# =========================
# AI ANALYSIS FUNCTION
# =========================
def analyze(company, news_list):

    combined_news = "\n".join(news_list[:3)

    prompt = f"""
You are a strict stock market analyst.
You are going to invest so you have to predict the future stock behaviour.
Be strict and factual.
Do NOT assume or infer anything not present in the news.
If no strong trigger exists, return NO TRADE..
Company: {company}

News:
{combined_news}

Rules:
1. ONLY consider REAL price-moving events:
   - large orders/contracts
   - strong earnings (profit growth, margin expansion)
   - acquisitions or stake changes
   - promoter buying

2. IGNORE completely:
   - scrutinizer reports
   - compliance certificates
   - SDD filings
   - newspaper publication
   - general updates

3. If no strong trigger → return:
   "action": "NO TRADE"

4. DO NOT assume or guess.
5. DO NOT connect unrelated macro news.
6. Do not hallucinate yourself

Return ONLY JSON:

{{
  "score": -1 to 1,
  "probability": 0-100,
  "action": "BUY / SELL / NO TRADE",
  "reason": "short factual reason"
}}
"""

    response = groq.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1
    )

    return response.choices[0].message.content.strip()

# =========================
# PROCESS
# =========================
results = []

for company, news_list in company_news.items():

    if not is_meaningful_news(news_list):
        continue   # 🔥 SKIP NOISE

    try:
        ai_output = analyze(company, news_list)

        if not ai_output:
            continue

        # clean response
        ai_output = ai_output.replace("```json", "").replace("```", "").strip()

        data = extract_json(ai_output)

        if not data:
            print(f"❌ Invalid JSON for {company}")
            print("RAW:", ai_output)
            continue

        score = data.get("score", 0)
        prob = data.get("probability", 0)
        reason = data.get("reason", "")

        # =========================
        # DECISION LOGIC
        # =========================
        action = data.get("action", "NO TRADE")
        if action == "BUY" and prob >= 70:
            results.append([company, prob, "BUY", reason])
        elif action == "SELL" and prob >= 60:
            results.append([company, prob, "SELL", reason])

    except Exception as e:
        print("❌ Error:", company, e)

# =========================
# SORT & TOP 3
# =========================
results.sort(key=lambda x: x[1], reverse=True)
results = results[:5]

# =========================
# WRITE OUTPUT (APPEND)
# =========================
existing_data = output_ws.get_all_values()

# header only once
if not existing_data:
    output_ws.append_row(["Company", "Probability %", "Action", "Reason"])

# append results
for row in results:
    output_ws.append_row(row)

# =========================
# TIMESTAMP
# =========================
ist_time = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")

output_ws.append_row(["Updated (IST)", ist_time])

last_row = len(output_ws.get_all_values())

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
