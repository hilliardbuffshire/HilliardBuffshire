#!/usr/bin/env python3
"""
Hilliard Buffshire Investment Dashboard - Combined Server
Serves static files AND Yahoo Finance data from a single port (8000).
Run with: python server.py
"""

import http.server
import urllib.parse
import json
import sys
import subprocess
import math
import os

# Render/Railway/cloud platforms inject PORT via environment variable
PORT = int(os.environ.get("PORT", 8000))

# ── Auto-install yfinance ─────────────────────────────────────────
def install_yfinance():
    print("  Installing yfinance (one-time, ~30 sec)...")
    for cmd in [
        [sys.executable, "-m", "pip", "install", "yfinance", "--quiet"],
        [sys.executable, "-m", "pip", "install", "yfinance", "--quiet", "--user"],
    ]:
        try:
            subprocess.check_call(cmd)
            return True
        except Exception:
            continue
    return False

try:
    import yfinance as yf
    print("  yfinance OK")
except ImportError:
    if install_yfinance():
        import yfinance as yf
        print("  yfinance installed OK")
    else:
        print("  ERROR: could not install yfinance. Run: pip install yfinance")
        sys.exit(1)


# ── Helpers ───────────────────────────────────────────────────────
def v(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return {}
    return {"raw": val, "fmt": str(val)}

def safe(x):
    if x is None:
        return 0
    try:
        f = float(x)
        return 0 if math.isnan(f) or math.isinf(f) else f
    except Exception:
        return 0

def row(df, *keys):
    for k in keys:
        if k in df.index:
            try:
                loc = df.index.get_loc(k)
                if isinstance(loc, slice):
                    val = df.iloc[loc.start, 0]
                elif hasattr(loc, '__len__'):
                    val = df.iloc[0, 0]
                else:
                    val = df.iloc[loc, 0]
                r = safe(val)
                if r != 0:
                    return r
            except Exception:
                continue
    # partial match fallback
    lower_keys = [k.lower() for k in keys]
    for idx_name in df.index:
        for lk in lower_keys:
            if lk in str(idx_name).lower():
                try:
                    r = safe(df.loc[idx_name].iloc[0])
                    if r != 0:
                        return r
                except Exception:
                    pass
    return 0


# ── Yahoo Finance data fetchers ───────────────────────────────────
def build_quotesummary(ticker_str):
    t = yf.Ticker(ticker_str)
    info = t.info or {}

    # Income statement
    try:
        inc = t.income_stmt
        if inc is not None and not inc.empty:
            total_rev    = row(inc, "Total Revenue")
            gross_profit = row(inc, "Gross Profit")
            ebit         = row(inc, "EBIT", "Operating Income")
            net_income   = row(inc, "Net Income")
        else:
            total_rev = gross_profit = ebit = net_income = 0
    except Exception:
        total_rev = gross_profit = ebit = net_income = 0

    # Cash flow
    try:
        cf = t.cash_flow
        if cf is not None and not cf.empty:
            fcf = row(cf, "Free Cash Flow")
            if not fcf:
                ocf   = row(cf, "Operating Cash Flow", "Cash Flow From Operations")
                capex = abs(row(cf, "Capital Expenditure", "Purchase Of PPE"))
                if ocf:
                    fcf = ocf - capex
        else:
            fcf = 0
    except Exception:
        fcf = 0
    if not fcf:
        fcf = safe(info.get("freeCashflow"))

    # Balance sheet
    try:
        bs = t.balance_sheet
        if bs is not None and not bs.empty:
            long_term_debt  = row(bs, "Long Term Debt", "Long-Term Debt")
            short_term_debt = row(bs, "Current Debt", "Short Long Term Debt", "Short-Term Debt")
            total_equity    = row(bs, "Stockholders Equity", "Total Stockholder Equity",
                                      "Common Stock Equity") or 1
            total_assets    = row(bs, "Total Assets") or 1
        else:
            long_term_debt = short_term_debt = 0
            total_equity = total_assets = 1
    except Exception:
        long_term_debt = short_term_debt = 0
        total_equity = total_assets = 1

    price      = safe(info.get("currentPrice") or info.get("regularMarketPrice"))
    market_cap = safe(info.get("marketCap"))
    shares     = safe(info.get("sharesOutstanding")) or 1

    result = {
        "financialData": {
            "currentPrice":   v(price),
            "freeCashflow":   v(fcf),
            "totalRevenue":   v(total_rev or safe(info.get("totalRevenue"))),
            "returnOnEquity": v(info.get("returnOnEquity")),
            "returnOnAssets": v(info.get("returnOnAssets")),
            "debtToEquity":   v(info.get("debtToEquity")),
            "grossMargins":   v(info.get("grossMargins")),
            "earningsGrowth": v(info.get("earningsGrowth") or info.get("revenueGrowth")),
        },
        "defaultKeyStatistics": {
            "sharesOutstanding": v(shares),
            "trailingEps":       v(info.get("trailingEps")),
            "bookValue":         v(info.get("bookValue")),
            "pegRatio":          v(info.get("pegRatio")),
            "forwardPE":         v(info.get("forwardPE")),
            "enterpriseValue":   v(info.get("enterpriseValue")),
            "beta":              v(info.get("beta")),
        },
        "assetProfile": {
            "longName":  info.get("longName") or info.get("shortName") or ticker_str,
            "shortName": info.get("shortName") or ticker_str,
            "sector":    info.get("sector") or "Unknown",
            "industry":  info.get("industry") or "",
        },
        "summaryDetail": {
            "marketCap":     v(market_cap),
            "previousClose": v(info.get("previousClose")),
            "trailingPE":    v(info.get("trailingPE")),
        },
        "incomeStatementHistory": {
            "incomeStatementHistory": [{
                "totalRevenue":    v(total_rev or safe(info.get("totalRevenue"))),
                "grossProfit":     v(gross_profit),
                "ebit":            v(ebit),
                "operatingIncome": v(ebit),
                "netIncome":       v(net_income),
            }]
        },
        "cashflowStatementHistory": {
            "cashflowStatements": [{"freeCashflow": v(fcf)}]
        },
        "balanceSheetHistory": {
            "balanceSheetStatements": [{
                "longTermDebt":           v(long_term_debt),
                "shortLongTermDebt":      v(short_term_debt),
                "totalStockholderEquity": v(total_equity),
                "totalAssets":            v(total_assets),
            }]
        },
    }
    return {"quoteSummary": {"result": [result], "error": None}}


def build_chart(ticker_str, range_="6mo"):
    t = yf.Ticker(ticker_str)
    hist = t.history(period=range_, interval="1mo")
    closes, timestamps = [], []
    for ts, row_data in hist.iterrows():
        c = row_data.get("Close")
        closes.append(round(float(c), 2) if c is not None and not math.isnan(float(c)) else None)
        try:
            timestamps.append(int(ts.timestamp()))
        except Exception:
            timestamps.append(0)
    return {
        "chart": {
            "result": [{
                "meta": {"symbol": ticker_str.upper()},
                "timestamp": timestamps,
                "indicators": {"quote": [{"close": closes}]}
            }],
            "error": None
        }
    }


# ── Combined HTTP Handler ─────────────────────────────────────────
class Handler(http.server.SimpleHTTPRequestHandler):

    def log_message(self, fmt, *args):
        # Only show data requests
        if any(p in self.path for p in ['/yf', '/chart']):
            print(f"  [data] {self.path.split('?')[0]} → {self.path.split('ticker=')[-1].split('&')[0]}")

    def _json(self, code, obj):
        data = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if parsed.path == "/yf":
            ticker = (params.get("ticker", [None])[0] or "").upper().strip()
            if not ticker:
                self._json(400, {"error": "ticker required"})
                return
            try:
                self._json(200, build_quotesummary(ticker))
            except Exception as e:
                self._json(500, {"error": str(e)})

        elif parsed.path == "/chart":
            ticker = (params.get("ticker", [None])[0] or "").upper().strip()
            range_ = params.get("range", ["6mo"])[0]
            if not ticker:
                self._json(400, {"error": "ticker required"})
                return
            try:
                self._json(200, build_chart(ticker, range_))
            except Exception as e:
                self._json(500, {"error": str(e)})

        else:
            # Serve static files normally
            super().do_GET()


# ── Main ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print()
    print("  ================================================")
    print("    Hilliard Buffshire Investment Dashboard")
    print("  ================================================")
    print(f"  Open: http://localhost:{PORT}/hilliard-buffshire-dashboard.html")
    print("  Keep this window open. Close to stop.")
    print()

    try:
        server = http.server.HTTPServer(("", PORT), Handler)
    except OSError as e:
        print(f"  ERROR: Port {PORT} is in use. Close other servers first.")
        print(f"  Detail: {e}")
        sys.exit(1)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")
        server.server_close()
