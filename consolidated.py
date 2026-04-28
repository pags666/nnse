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
# CONFIG
# =========================
SHEET_ID          = "1le7tQxVkznMvphgOB2T0tGyzb_ByeaOHJ4R9E5piY_A"
GROQ_SHEET        = "groq"          # sheet with Groq AI output  (Company | Probability % | Action | Reason)
WORD_SHEET        = "wordf"         # sheet with word/keyword signals (COMPANY | SCORE | ACTION | REASON)
OUTPUT_SHEET      = "consolidated"  # where we write the final result
CONFIDENCE_CUTOFF = 70              # only emit rows where final confidence >= this

# =========================
# GOOGLE SHEETS AUTH
# =========================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
gc    = gspread.authorize(creds)
ss    = gc.open_by_key(SHEET_ID)

# =========================
# GROQ CLIENT
# =========================
groq_client = Groq(api_key=os.environ["GROQ_API_KEY"])
# =========================
# FINBERT (ADD THIS BLOCK)
# =========================
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

tokenizer = AutoTokenizer.from_pretrained("yiyanghkust/finbert-tone",use_fast=False)
finbert_model = AutoModelForSequenceClassification.from_pretrained("yiyanghkust/finbert-tone")

def finbert_sentiment(text):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
    outputs = finbert_model(**inputs)
    probs = torch.nn.functional.softmax(outputs.logits, dim=1)

    labels = ["negative", "neutral", "positive"]
    score = probs.detach().numpy()[0]

    return labels[score.argmax()].upper(), float(score.max())
# =========================
# HELPERS
# =========================
def open_or_create(title, rows=200, cols=10):
    try:
        return ss.worksheet(title)
    except Exception:
        return ss.add_worksheet(title=title, rows=str(rows), cols=str(cols))

def sheet_to_records(ws):
    """Return list-of-dicts; first row = headers."""
    rows = ws.get_all_values()
    if len(rows) < 2:
        return []
    headers = [h.strip().upper() for h in rows[0]]
    return [dict(zip(headers, r)) for r in rows[1:] if any(r)]

def normalise_ticker(raw: str) -> str:
    """Upper-case, strip spaces."""
    return str(raw).strip().upper()

# =========================
# READ SOURCE SHEETS
# =========================
groq_ws  = ss.worksheet(GROQ_SHEET)
word_ws  = ss.worksheet(WORD_SHEET)

groq_rows = sheet_to_records(groq_ws)
word_rows = sheet_to_records(word_ws)

print(f"✅  Groq sheet  : {len(groq_rows)} rows")
print(f"✅  Word sheet  : {len(word_rows)} rows")

# =========================
# INDEX BOTH SHEETS BY COMPANY
# =========================
# groq_map : ticker → {probability, action, reason}
groq_map = {}
for r in groq_rows:
    ticker = normalise_ticker(r.get("COMPANY") or r.get("SYMBOL") or "")
    if not ticker:
        continue
    try:
        prob = float(str(r.get("PROBABILITY %") or r.get("PROBABILITY") or 0).replace("%", ""))
    except ValueError:
        prob = 0.0
    action = str(r.get("ACTION", "")).strip().upper()
    reason = str(r.get("REASON", "")).strip()
    if ticker not in groq_map or prob > groq_map[ticker]["probability"]:
        groq_map[ticker] = {"probability": prob, "action": action, "reason": reason}

# word_map : ticker → {score, action, reason}
word_map = {}
for r in word_rows:
    ticker = normalise_ticker(
        r.get("COMPANY") or r.get("COMPANY NAME") or r.get("SYMBOL") or ""
    )
    if not ticker:
        continue
    try:
        score = float(str(r.get("SCORE") or r.get("PROBABILITY %") or 0).replace("%", ""))
    except ValueError:
        score = 0.0
    action = str(r.get("ACTION", "")).strip().upper()
    reason = str(r.get("REASON", "") or r.get("DETAILS", "")).strip()
    if ticker not in word_map or score > word_map[ticker]["score"]:
        word_map[ticker] = {"score": score, "action": action, "reason": reason}

# =========================
# MERGE: all tickers from both sheets
# =========================
all_tickers = sorted(set(groq_map) | set(word_map))

# =========================
# CONSOLIDATION via Groq AI
# =========================
SYSTEM_PROMPT = """You are a senior quantitative stock analyst for Indian equity markets (NSE/BSE).

You receive two independent signals for a stock:

1. GROQ AI SIGNAL  — generated from real-time NSE/BSE exchange announcements using an LLM.
   Fields: action (BUY/SELL/NO TRADE), probability (0-100), reason.

2. KEYWORD/WORD SIGNAL — generated from a rule-based keyword scan of the same announcements.
   Fields: action (BUY/SELL/NO TRADE), score (0-100), reason.

Your task: consolidate both signals into ONE final recommendation.

Rules:

- If BOTH signals say BUY → STRONG BUY. Confidence = avg(prob, score) + 5 (max 100)

- If BOTH signals say SELL → STRONG SELL. Confidence = avg(prob, score) + 5 (max 100)

- If signals CONFLICT:
  → choose the signal with higher probability/score
  → reduce confidence by 5 only
  → do NOT default to NO TRADE

- If one signal is NO TRADE and other is BUY/SELL:
  → allow if confidence >= 60
  → otherwise NO TRADE

- Do NOT ignore valid SELL signals

- Ignore only:
  compliance filings, scrutinizer reports, certificates, newspaper ads

- Focus ONLY on price-moving events:
  orders, results, contracts, losses, resignations, penalties

Return ONLY valid JSON (no markdown, no explanation outside JSON):
{
  "final_action": "BUY" | "SELL" | "NO TRADE",
  "confidence": <integer 0-100>,
  "signal_agreement": "AGREE" | "CONFLICT" | "PARTIAL",
  "groq_weight": <0.0-1.0>,
  "word_weight": <0.0-1.0>,
  "consolidated_reason": "<2-3 sentence professional reasoning>"
}
"""

def consolidate_with_groq(ticker, g, w):
    """Call Groq to merge two signals. Returns dict."""

    user_msg = f"""
Ticker: {ticker}

--- GROQ AI SIGNAL ---
Action      : {g.get("action", "NO TRADE")}
Probability : {g.get("probability", 0)}%
Reason      : {g.get("reason", "N/A")}

--- KEYWORD/WORD SIGNAL ---
Action      : {w.get("action", "NO TRADE")}
Score       : {w.get("score", 0)}%
Reason      : {w.get("reason", "N/A")}

Consolidate and return JSON only.
"""

    resp = groq_client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_msg},
        ],
        temperature=0.1,
        max_tokens=300,
    )

    raw = resp.choices[0].message.content.strip()
    raw = raw.replace("```json", "").replace("```", "").strip()

    start = raw.find("{")
    end   = raw.rfind("}") + 1
    if start == -1 or end <= start:
        raise ValueError(f"No JSON found in: {raw!r}")

    return json.loads(raw[start:end])

# =========================
# RUN CONSOLIDATION
# =========================
final_results = []

for ticker in all_tickers:

    g = groq_map.get(ticker, {"probability": 0, "action": "NO TRADE", "reason": ""})
    w = word_map.get(ticker,  {"score": 0,       "action": "NO TRADE", "reason": ""})
    # =========================
    # FINBERT FILTER (ADD HERE)
    # =========================
    combined_text = f"{g['reason']} {w['reason']}"
    sentiment, sent_conf = finbert_sentiment(combined_text)

    # skip weak neutral signals
    if sentiment == "NEUTRAL" and sent_conf < 0.6:
        continue

    # block conflicting sentiment
    if sentiment == "NEGATIVE" and g["action"] == "BUY":
        continue

    if sentiment == "POSITIVE" and g["action"] == "SELL":
        continue
    # skip if both empty
    if g["action"] == "NO TRADE" and w["action"] == "NO TRADE":
        continue

    # 🔥 DIRECT PASS if no word signal
    if ticker not in word_map:
        if g["action"] in ("BUY", "SELL") and g["probability"] >= 65:
            final_results.append({
                "ticker": ticker,
                "action": g["action"],
                "confidence": int(g["probability"]),
                "agreement": "GROQ_ONLY",
                "groq_action": g["action"],
                "groq_prob": g["probability"],
                "word_action": "NONE",
                "word_score": 0,
                "reason": g["reason"],
            })
            continue  # ✅ VERY IMPORTANT

    try:
        result = consolidate_with_groq(ticker, g, w)

        final_action = result.get("final_action", "NO TRADE").upper()
        confidence   = int(result.get("confidence", 0))
        agreement    = result.get("signal_agreement", "")
        reason       = result.get("consolidated_reason", "")

        # only keep trades above confidence cutoff
        # allow strong SELL even if below cutoff
        if final_action in ("BUY", "SELL") and confidence >= 70:
            final_results.append({
                "ticker": ticker,
                 "action": final_action,
                "confidence": confidence,
                "agreement": agreement,
                "groq_action": g["action"],
                "groq_prob": g["probability"],
                "word_action": w["action"],
                "word_score": w["score"],
                "reason": reason,
            })
        continue

        print(f"  {ticker:20s}  {final_action:8s}  conf={confidence}%  agree={agreement}")

    except Exception as e:
        print(f"❌  {ticker}: {e}")

# =========================
# SORT: confidence DESC, BUY first
# =========================
final_results.sort(key=lambda x: (-x["confidence"], x["action"] != "BUY"))

# =========================
# WRITE TO OUTPUT SHEET
# =========================
out_ws = open_or_create(OUTPUT_SHEET)
out_ws.clear()

headers = [
    "Ticker",
    "Final Action",
    "Confidence %",
    "Signal Agreement",
    "Groq Action",
    "Groq Prob %",
    "Word Action",
    "Word Score %",
    "Consolidated Reason",
]
out_ws.append_row(headers)

# format header row
out_ws.format("A1:I1", {
    "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.6},
    "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
})

# write data rows
for row in final_results:
    out_ws.append_row([
        row["ticker"],
        row["action"],
        row["confidence"],
        row["agreement"],
        row["groq_action"],
        row["groq_prob"],
        row["word_action"],
        row["word_score"],
        row["reason"],
    ])

# colour-code BUY = green, SELL = red
all_vals = out_ws.get_all_values()
for i, row_vals in enumerate(all_vals[1:], start=2):
    action_cell = row_vals[1].upper() if len(row_vals) > 1 else ""
    if action_cell == "BUY":
        bg = {"red": 0.85, "green": 1.0, "blue": 0.85}
    elif action_cell == "SELL":
        bg = {"red": 1.0, "green": 0.85, "blue": 0.85}
    else:
        continue
    out_ws.format(f"A{i}:I{i}", {"backgroundColor": bg})

# timestamp row
ist_time = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M IST")
last_row = len(out_ws.get_all_values()) + 1
out_ws.append_row(["Last Updated", ist_time])
out_ws.format(f"A{last_row}:B{last_row}", {
    "backgroundColor": {"red": 1.0, "green": 0.95, "blue": 0.7},
    "textFormat": {"bold": True, "italic": True},
})

print(f"\n✅  Done — {len(final_results)} consolidated signals written to '{OUTPUT_SHEET}' sheet.")
print(f"⏰  Timestamp: {ist_time}")
