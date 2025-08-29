#!/usr/bin/env python3
"""
Emails the Top-10 most-active (by volume) NSE (.NS) and BSE (.BO) stocks.
Runs in GitHub Actions on a schedule. No local Python needed.

Note: Uses Yahoo Finance Screener (unofficial). For official/real-time data,
use an exchange-approved provider.
"""
import os
import json
import time
import logging
import smtplib
import ssl
from datetime import datetime, timedelta, time as dtime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

# --- Config via env (set in GitHub Actions secrets or workflow env) ---
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
FROM_EMAIL = os.getenv("FROM_EMAIL", SMTP_USER or "")
TO_EMAILS = [e.strip() for e in os.getenv("TO_EMAILS", "").split(",") if e.strip()]
TOP_N = int(os.getenv("TOP_N", "10"))
ENFORCE_MARKET_HOURS = os.getenv("ENFORCE_MARKET_HOURS", "true").lower() in ("1","true","yes","y")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")

# --- Time helpers (IST = UTC+5:30) ---
def now_ist_dt():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def is_market_open_ist(dt=None):
    """
    Mon–Fri, 09:15–15:30 IST (no holiday calendar).
    """
    if dt is None:
        dt = now_ist_dt()
    if dt.weekday() >= 5:  # 5=Sat, 6=Sun
        return False
    t = dt.time()
    return (t >= dtime(9, 15)) and (t <= dtime(15, 30))

# --- Data fetch ---
def yahoo_screener_most_active_india(size=120, retries=3, timeout=15):
    url = "https://query1.finance.yahoo.com/v1/finance/screener"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
    }
    payload = {
        "offset": 0,
        "size": size,
        "sortField": "dayvolume",
        "sortType": "DESC",
        "quoteType": "EQUITY",
        "query": {"operator": "AND", "operands": [{"operator": "eq", "operands": ["region", "in"]}]},
    }
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=timeout)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("finance", {}).get("result", [{}])[0].get("quotes", []) or []
            logging.warning("Yahoo Screener HTTP %s: %s", resp.status_code, resp.text[:200])
        except Exception as e:
            logging.exception("Yahoo Screener attempt %d failed: %s", attempt, e)
        time.sleep(1.5 * attempt)
    return []

def select_top_by_exchange(quotes, suffix, top_n):
    rows = []
    for q in quotes:
        sym = q.get("symbol") or ""
        if not sym.endswith(suffix):
            continue
        vol = q.get("regularMarketVolume") or q.get("averageDailyVolume3Month") or 0
        name = q.get("shortName") or q.get("longName") or sym
        price = q.get("regularMarketPrice")
        chg = q.get("regularMarketChange")
        chg_pct = q.get("regularMarketChangePercent")
        rows.append({
            "symbol": sym,
            "name": name,
            "volume": int(vol) if isinstance(vol, (int, float)) else 0,
            "price": price if isinstance(price, (int, float)) else None,
            "change": chg if isinstance(chg, (int, float)) else None,
            "change_pct": chg_pct if isinstance(chg_pct, (int, float)) else None,
        })
    rows.sort(key=lambda r: r["volume"], reverse=True)
    return rows[:top_n]

def build_email_html(ist_time_str, nse_rows, bse_rows):
    def tbl(rows, title):
        if not rows:
            return f"<p>No data for {title}.</p>"
        head = "<tr><th>#</th><th>Symbol</th><th>Name</th><th>Price</th><th>Δ</th><th>Δ%</th><th>Volume</th></tr>"
        body = ""
        for i, r in enumerate(rows, 1):
            price = f'{r["price"]:.2f}' if r["price"] is not None else "-"
            chg = f'{r["change"]:.2f}' if r["change"] is not None else "-"
            chg_pct = f'{r["change_pct"]:.2f}' if r["change_pct"] is not None else "-"
            body += (
                f"<tr><td>{i}</td><td>{r['symbol']}</td><td>{r['name']}</td>"
                f"<td>{price}</td><td>{chg}</td><td>{chg_pct}</td><td>{r['volume']:,}</td></tr>"
            )
        return f"<h3>{title}</h3><table border='1' cellspacing='0' cellpadding='6'>{head}{body}</table>"
    return f"""
    <html><body>
      <p><strong>Top {len(nse_rows)} Most Active (by volume) — NSE</strong><br/>
      <em>Run at {ist_time_str} IST</em></p>
      {tbl(nse_rows, "NSE (.NS)")}
      <br/>
      <p><strong>Top {len(bse_rows)} Most Active (by volume) — BSE</strong></p>
      {tbl(bse_rows, "BSE (.BO)")}
      <p style="color:#666;font-size:12px;">Source: Yahoo Finance Screener (unofficial).</p>
    </body></html>
    """

def send_email(subject, html_body):
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS and FROM_EMAIL and TO_EMAILS):
        raise RuntimeError("Email config missing. Set SMTP_*, FROM_EMAIL, TO_EMAILS as GitHub Secrets.")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = FROM_EMAIL
    msg["To"] = ", ".join(TO_EMAILS)
    msg.attach(MIMEText(html_body, "html"))

    context = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls(context=context)
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(FROM_EMAIL, TO_EMAILS, msg.as_string())
    logging.info("Email sent to: %s", TO_EMAILS)

def main():
    ist_dt = now_ist_dt()
    ist_time_str = ist_dt.strftime("%Y-%m-%d %H:%M")
    if ENFORCE_MARKET_HOURS and not is_market_open_ist(ist_dt):
        logging.info("Outside Indian market hours. Skipping send. Now IST: %s", ist_time_str)
        return

    quotes = yahoo_screener_most_active_india(size=max(TOP_N * 6, 60))
    if not quotes:
        logging.warning("No quotes fetched; aborting email.")
        return

    nse = select_top_by_exchange(quotes, ".NS", TOP_N)
    bse = select_top_by_exchange(quotes, ".BO", TOP_N)

    subject = f"India Most-Active (Vol) — NSE/BSE Top {TOP_N} @ {ist_time_str} IST"
    html = build_email_html(ist_time_str, nse, bse)
    send_email(subject, html)

if __name__ == "__main__":
    main()
