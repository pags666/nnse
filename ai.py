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
INPUT_WS = "nse"
OUTPUT_WS = "result"

# =========================
# GOOGLE SHEETS AUTH (GITHUB SECRET)
# =========================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
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

# =========================
# READ DATA
# =========================
rows = input_ws.get_all_records()
company_news = defaultdict(list)

for row in rows:
    company = row.get("SYMBOL")
    news = row.get("DETAILS")

    if company and news:
        company_news[company].append(news)

# =========================
# AI ANALYSIS
# =========================
def analyze(company, news_list):

    combined_news = "\n".join(news_list)

    prompt = f"""
    Analyze stock news.

    Company: {company}
    News:
    {combined_news}

    Return ONLY JSON:

    {{
      "sentiment": number between -1 and 1,
      "confidence": number between 0 and 1,
      "impact": "low" or "medium" or "high",
      "risk": true or false
    }}
    """

    response = groq.chat.completions.create(
        model="llama-3.1-8b-instant",   # ✅ working model
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1
    )

    return response.choices[0].message.content.strip()

# =========================
# PROBABILITY
# =========================
def calc_prob(data):

    S = data["sentiment"]
    C = data["confidence"]

    impact_map = {"low": 0.5, "medium": 1, "high": 1.5}
    I = impact_map.get(data["impact"], 1)

    P = -0.4 if data["risk"] else 0   # slightly stronger negative

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

        # clean
        ai_output = ai_output.replace("```json", "").replace("```", "").strip()

        # extract JSON safely
        start = ai_output.find("{")
        end = ai_output.rfind("}") + 1

        if start == -1 or end == -1:
            print("Invalid:", company, ai_output)
            continue

        json_text = ai_output[start:end]

        data = json.loads(json_text)

        buy, sell = calc_prob(data)

        # better thresholds
        signal = "BUY" if buy > 60 else "SELL" if buy < 50 else "HOLD"

        results.append([company, buy, sell, signal])

        print(company, buy)

    except Exception as e:
        print("Error:", company, e)

# =========================
# SORT
# =========================
results.sort(key=lambda x: x[1], reverse=True)

# =========================
# WRITE OUTPUT (APPEND MODE)
# =========================
existing_data = output_ws.get_all_values()
start_row = len(existing_data) + 1

# header only once
if start_row == 1:
    output_ws.append_row(["Company", "Buy %", "Sell %", "Signal"])

# append results
for row in results:
    output_ws.append_row(row)

# =========================
# ADD IST TIMESTAMP
# =========================
ist_time = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")

output_ws.append_row(["Updated (IST)", ist_time])

last_row = len(output_ws.get_all_values())

output_ws.format(
    f"A{last_row}:B{last_row}",
    {
        "backgroundColor": {
            "red": 0.85,
            "green": 0.9,
            "blue": 1
        },
        "textFormat": {"bold": True}
    }
)
