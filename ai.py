'''import os
import json
import math
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from groq import Groq
from oauth2client.service_account import ServiceAccountCredentials

# =========================
# CONFIG
# =========================
SHEET_ID = "1le7tQxVkznMvphgOB2T0tGyzb_ByeaOHJ4R9E5piY_A"
INPUT_WS = "nse"
OUTPUT_WS = "result"

# =========================
# GOOGLE SHEETS AUTH (GITHUB SECRET)
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

# create output sheet if not exists
try:
    output_ws = spreadsheet.worksheet(OUTPUT_WS)
except:
    output_ws = spreadsheet.add_worksheet(title=OUTPUT_WS, rows="100", cols="20")

input_ws = spreadsheet.worksheet(INPUT_WS)

# =========================
# GROQ (GITHUB SECRET)
# =========================
groq = Groq(api_key=os.environ["GROQ_API_KEY"])

rows = input_ws.get_all_records()

company_news = defaultdict(list)

for row in rows:
    company = row["SYMBOL"]     # Column A
    news = row["DETAILS"]       # Column D

    if company and news:
        company_news[company].append(news)

# =========================
# AI ANALYSIS FUNCTION
# =========================
def analyze(company, news_list):

    combined_news = "\n".join(news_list)

    prompt = f"""
You are a strict stock trader.

Analyze the news impact.

Company: {company}
News:
{combined_news}

Return ONLY JSON:

{{
  "score": number between -1 and 1,
  "probability": number (0-100),
  "reason": "short explanation"
}}

Rules:
- Strong positive → score > 0.5
- Strong negative → score < -0.5
- Weak / unclear → score negative
- If unsure → NEGATIVE score
"""
    response = groq.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1
    )

    return response.choices[0].message.content.strip()
# =========================
# PROBABILITY FUNCTION
# =========================
def calc_prob(data):

    S = data["sentiment"]
    C = data["confidence"]

    impact_map = {"low": 0.5, "medium": 1, "high": 1.5}
    I = impact_map.get(data["impact"], 1)

    P = -0.3 if data["risk"] else 0

    score = (S * 0.5) + (C * 0.3) + (I * 0.1) + (P * 0.1)

    buy = 1 / (1 + math.exp(-score))
    sell = 1 - buy

    return round(buy * 100, 2), round(sell * 100, 2)

# =========================
# PROCESS
# =========================
results = []

for company, news_list in company_news.items():

    try:
        ai_output = analyze(company, news_list)

        if not ai_output:
            continue

        ai_output = ai_output.replace("```json", "").replace("```", "").strip()

        start = ai_output.find("{")
        end = ai_output.rfind("}") + 1

        if start == -1 or end == -1:
            continue

        data = json.loads(ai_output[start:end])
        score = data["score"]
        prob = data["probability"]
        reason = data["reason"]
        # convert score → action
        if score > 0.3 and prob >= 65:
            results.append([company, prob, "BUY", reason])
        elif score < -0.2 and prob >= 50:
            results.append([company, prob, "SELL", reason])

    except Exception as e:
        print("Error:", company, e)

# =========================
# SORT
# =========================
results.sort(key=lambda x: x[1], reverse=True)

# keep top 3 only
results = results[:3]
# =========================
# WRITE OUTPUT (APPEND MODE)
# =========================
# =========================
# APPEND OUTPUT (NO DELETE)
# =========================

existing_data = output_ws.get_all_values()

# add header only once (first run)
if not existing_data:
    output_ws.append_row(["Company", "Probability %", "Action", "Reason"])

# append new AI results
for row in results:
    output_ws.append_row(row)

from datetime import datetime
from zoneinfo import ZoneInfo

ist_time = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")

output_ws.append_row(["Updated (IST)", ist_time])

last_row = len(output_ws.get_all_values())

output_ws.format(
    f"A{last_row}:B{last_row}",
    {
        "backgroundColor": {
            "red":1,
            "green": 0.9,
            "blue": 1
        },
        "textFormat": {
            "bold": True
        }
    }
)
'''



import os
import json
import math
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from groq import Groq
from oauth2client.service_account import ServiceAccountCredentials

# =========================
# CONFIG
# =========================
SHEET_ID = "1le7tQxVkznMvphgOB2T0tGyzb_ByeaOHJ4R9E5piY_A"
INPUT_SHEETS = ["nse", "bse"]   # ✅ MULTI SHEET
OUTPUT_WS = "result"

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

# =========================
# AI ANALYSIS FUNCTION
# =========================
def analyze(company, news_list):

    combined_news = "\n".join(news_list)

    prompt = f"""
You are a professional stock trader.

Analyze the news impact.

Company: {company}
News:
{combined_news}

Return ONLY JSON:

{{
  "score": number between -1 and 1,
  "probability": number (0-100),
  "action": "BUY / SELL / NO TRADE",
  "reason": "short explanation"
}}

Rules:
- Strong positive → BUY
- Strong negative → SELL
- Routine / no impact → NO TRADE
- Ignore:
  trading window, postal ballot, AGM, newspaper, compliance updates
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

    try:
        ai_output = analyze(company, news_list)

        if not ai_output:
            continue

        # clean response
        ai_output = ai_output.replace("```json", "").replace("```", "").strip()

        start = ai_output.find("{")
        end = ai_output.rfind("}") + 1

        if start == -1 or end == -1:
            continue

        data = json.loads(ai_output[start:end])

        score = data.get("score", 0)
        prob = data.get("probability", 0)
        reason = data.get("reason", "")

        # =========================
        # DECISION LOGIC
        # =========================
        action = data.get("action", "NO TRADE")
        if action == "BUY" and prob >= 60:
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
