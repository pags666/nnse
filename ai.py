import os
import json
import math
from collections import defaultdict
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
# GOOGLE SHEETS AUTH
# =========================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds_dict = json.loads(os.environ["service_account.json"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

input_ws = client.open_by_key(SHEET_ID).worksheet(INPUT_WS)
output_ws = client.open_by_key(SHEET_ID).worksheet(OUTPUT_WS)

# =========================
# GROQ
# =========================
groq = Groq(api_key=os.environ["GROQ_API_KEY"])

# =========================
# READ DATA
# =========================
rows = input_ws.get_all_records()

company_news = defaultdict(list)

for row in rows:
    company = row["Company"]
    news = row["News"]

    if company and news:
        company_news[company].append(news)

# =========================
# GROQ ANALYSIS
# =========================
def analyze(company, news_list):

    combined_news = "\n".join(news_list)

    prompt = f"""
    You are a stock analyst.

    Company: {company}
    News:
    {combined_news}

    Return STRICT JSON:
    {{
      "sentiment": (-1 to 1),
      "confidence": (0 to 1),
      "impact": "low/medium/high",
      "risk": true/false
    }}
    """

    response = groq.chat.completions.create(
        model="llama3-70b-8192",
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

        # clean JSON
        ai_output = ai_output.replace("```json", "").replace("```", "").strip()

        data = json.loads(ai_output)

        buy, sell = calc_prob(data)

        signal = "BUY" if buy > 60 else "SELL" if buy < 40 else "HOLD"

        results.append([company, buy, sell, signal])

        print(company, buy)

    except Exception as e:
        print("Error:", company, e)

# =========================
# SORT
# =========================
results.sort(key=lambda x: x[1], reverse=True)

# =========================
# WRITE OUTPUT
# =========================
output_ws.clear()
output_ws.append_row(["Company", "Buy %", "Sell %", "Signal"])

for row in results:
    output_ws.append_row(row)
