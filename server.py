#!/usr/bin/env python3
"""
Hilliard Buffshire Investment Dashboard - Data Server
Data sources:
  • QuickFS (https://quickfs.net) — 10년치 재무제표 (revenue, FCF, capex 등)
  • yfinance                      — 실시간 가격·시장 데이터 (무료·무제한)
Run: python server.py
QuickFS free tier: 500 calls/month  →  캐시(7일)로 충분히 커버
"""

import http.server
import urllib.parse
import json
import sys
import subprocess
import math
import os
import threading
import time
import datetime
import concurrent.futures

PORT = int(os.environ.get("PORT", 8000))

# ── QuickFS API Key ────────────────────────────────────────────────
# 무료 키 발급: https://quickfs.net  → 회원가입 → API Keys
# 여기 직접 입력하거나, 환경변수 QFS_API_KEY 로 설정하거나,
# 대시보드 ⚙️ 설정에서 입력해도 됩니다 (localStorage 저장).
QFS_API_KEY = os.environ.get("QFS_API_KEY", "")   # ← 키를 여기에 붙여넣기
QFS_BASE    = "https://public-api.quickfs.net/v1"


# ── Auto-install dependencies ──────────────────────────────────────
def _install(pkg):
    print(f"  Installing {pkg} (one-time)...")
    for cmd in [
        [sys.executable, "-m", "pip", "install", pkg, "--quiet"],
        [sys.executable, "-m", "pip", "install", pkg, "--quiet", "--user"],
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
    if _install("yfinance"):
        import yfinance as yf
        print("  yfinance installed OK")
    else:
        print("  ERROR: pip install yfinance 를 먼저 실행하세요")
        sys.exit(1)

try:
    import requests as req
    print("  requests OK")
except ImportError:
    if _install("requests"):
        import requests as req
        print("  requests installed OK")
    else:
        print("  ERROR: pip install requests 를 먼저 실행하세요")
        sys.exit(1)

# ── yfinance 전용 세션 ─────────────────────────────────────────────
# 문제: Yahoo Finance는 Render 등 클라우드 IP에서 TLS 지문(fingerprint)으로
#       서버 요청을 탐지해 차단함. User-Agent 변경만으로는 부족함.
# 해결: curl_cffi 로 실제 Chrome 브라우저의 TLS 핸드셰이크를 위조 → 클라우드 차단 우회
_YF_SESSION = None
_YF_CURL_OK = False
try:
    from curl_cffi.requests import Session as _CurlSession
    _YF_SESSION = _CurlSession(impersonate="chrome120")
    _YF_CURL_OK = True
    print("  curl_cffi session OK (Chrome TLS impersonation)")
except ImportError:
    try:
        print("  curl_cffi 설치 중...")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "curl-cffi", "--quiet"],
            timeout=120
        )
        from curl_cffi.requests import Session as _CurlSession
        _YF_SESSION = _CurlSession(impersonate="chrome120")
        _YF_CURL_OK = True
        print("  curl_cffi installed OK")
    except Exception as _curl_err:
        print(f"  curl_cffi 설치 실패 ({_curl_err}), requests 폴백 사용")
        _YF_SESSION = req.Session()
        _YF_SESSION.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
        })

# ── In-memory cache (서버 실행 중 유지) ───────────────────────────
_cache      = {}
_cache_lock = threading.Lock()
PRICE_TTL   = 5 * 60             # 5분   — 실시간 가격 (정확도 향상)
FUND_TTL    = 7 * 24 * 3600      # 7일   — 재무제표 (API 절약)

def _cget(key):
    with _cache_lock:
        if key in _cache:
            ts, ttl, data = _cache[key]
            if time.time() - ts < ttl:
                return data
    return None

def _cset(key, data, ttl=FUND_TTL):
    with _cache_lock:
        _cache[key] = (time.time(), ttl, data)


# ── QuickFS helpers ────────────────────────────────────────────────
# 공식 메트릭 목록: https://public-api.quickfs.net/v1/api_docs.yaml
QFS_METRICS = {
    # Income Statement
    "revenue":          "Total Revenue",
    "gross_profit":     "Gross Profit",
    "operating_income": "Operating Income (EBIT)",
    "net_income":       "Net Income",
    "eps_diluted":      "Diluted EPS",
    # Cash Flow
    "operating_cash_flow": "Operating Cash Flow",
    "free_cash_flow":      "Free Cash Flow",
    "capex":               "Capital Expenditures",
    # Balance Sheet
    "total_assets":         "Total Assets",
    "total_equity":         "Total Equity",
    "long_term_debt":       "Long-term Debt",
    "total_debt":           "Total Debt",
    "cash_and_equivalents": "Cash & Equivalents",
    "shares_diluted":       "Diluted Shares",
    "book_value_per_share": "Book Value/Share",
    # Ratios
    "roe":         "Return on Equity",
    "roic":        "Return on Invested Capital",
    "gross_margin":"Gross Margin",
    "pe":          "P/E Ratio",
}

def qfs_fetch(company, metric, period="TTM", range_=1, api_key=None):
    """QuickFS 단일 메트릭 조회. 반환: [oldest, ..., newest] 리스트."""
    key = api_key or QFS_API_KEY
    if not key:
        return []
    ck = f"qfs:{company}:{metric}:{period}:{range_}"
    cached = _cget(ck)
    if cached is not None:
        return cached
    try:
        url    = f"{QFS_BASE}/data/{company}/{metric}"
        params = {"period": period, "range": range_}
        hdr    = {"X-QFS-API-Key": key}
        r      = req.get(url, headers=hdr, params=params, timeout=12)
        if r.status_code == 401:
            print(f"  QuickFS: API 키 오류 ({metric})")
            return []
        if r.status_code == 404:
            print(f"  QuickFS: 메트릭 없음 — {metric} ({company})")
            return []
        if r.status_code == 429:
            print(f"  QuickFS: 월간 한도 초과 (500 calls/month)")
            return []
        r.raise_for_status()
        vals = r.json().get("data", [])
        ttl  = FUND_TTL if period == "Annual" else PRICE_TTL
        _cset(ck, vals, ttl)
        return vals
    except Exception as e:
        print(f"  QuickFS 오류 ({metric}): {e}")
        return []

def qfs_last(company, metric, period="TTM", api_key=None):
    """가장 최근 값 반환 (TTM 기본)."""
    vals = qfs_fetch(company, metric, period, 1, api_key)
    if vals:
        v = vals[-1]
        return float(v) if v is not None else 0.0
    return 0.0

def qfs_series(company, metric, period="Annual", years=5, api_key=None):
    """연도별 시계열 반환 (oldest→newest, 부족하면 끝에 0 패딩)."""
    vals   = qfs_fetch(company, metric, period, years, api_key)
    result = [float(v) if v is not None else 0.0 for v in vals]
    while len(result) < years:
        result.append(0.0)
    return result[:years]

def safe(x):
    if x is None:
        return 0
    try:
        f = float(x)
        return 0 if math.isnan(f) or math.isinf(f) else f
    except Exception:
        return 0

def v(val):
    """yfinance quoteSummary 형식 래핑 (front-end 호환)."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return {}
    return {"raw": val, "fmt": str(val)}


# ── Main data builders ─────────────────────────────────────────────
def build_quotesummary(ticker_str, qfs_key=None):
    """
    주식 스냅샷:
      - 실시간 가격/시장데이터  → yfinance  (무료, 무제한)
      - 재무제표 펀더멘털       → QuickFS   (캐시 7일)
    front-end mapYFToStock 과 동일 포맷 반환.
    """
    company = f"US:{ticker_str}"

    # ── 1. yfinance: 실시간 가격·시장 데이터 ──────────────────────
    # Ticker 객체 하나로 info + fast_info 모두 수집 (rate-limit 방지)
    yf_ck = f"yf:bundle:{ticker_str}"
    _yf_bundle = _cget(yf_ck)
    if _yf_bundle is None:
        _yf_bundle = {"info": {}, "price": 0.0, "shares": 0.0, "mktcap": 0.0}

        # ── yfinance fetch: 최대 2회 재시도 (클라우드 cold-start 대응) ──
        for _attempt in range(2):
            try:
                _t = yf.Ticker(ticker_str, session=_YF_SESSION)
                # fast_info 먼저 (빠르고 신뢰성 높음)
                try:
                    fi = _t.fast_info
                    _lp = (getattr(fi, "last_price",  None) or
                           getattr(fi, "lastPrice",   None))
                    _pc = (getattr(fi, "regular_market_previous_close", None) or
                           getattr(fi, "regularMarketPreviousClose",    None) or
                           getattr(fi, "previous_close",                None) or
                           getattr(fi, "previousClose",                 None))
                    _yf_bundle["price"]  = float(_lp or _pc or 0.0)
                    _yf_bundle["shares"] = float(getattr(fi, "shares",      None) or
                                                 getattr(fi, "sharesOutstanding", None) or 0.0)
                    _yf_bundle["mktcap"] = float(getattr(fi, "market_cap",  None) or
                                                 getattr(fi, "marketCap",   None) or 0.0)
                    if not _yf_bundle["price"]:
                        print(f"  {ticker_str}: fast_info 가격 없음 (last={_lp}, prev={_pc})")
                except Exception as _fi_err:
                    print(f"  {ticker_str}: fast_info 오류: {_fi_err}")
                # info dict
                try:
                    _yf_bundle["info"] = _t.info or {}
                except Exception:
                    pass
                # price 폴백 1: info 필드
                if not _yf_bundle["price"]:
                    _yf_bundle["price"] = safe(
                        _yf_bundle["info"].get("regularMarketPrice") or
                        _yf_bundle["info"].get("currentPrice") or
                        _yf_bundle["info"].get("previousClose"))
                # price 폴백 2: history (1d → 5d)
                if not _yf_bundle["price"]:
                    for _period in ("1d", "5d"):
                        try:
                            hist = _t.history(period=_period)
                            if not hist.empty:
                                _yf_bundle["price"] = float(hist["Close"].dropna().iloc[-1])
                                print(f"  {ticker_str}: history({_period}) 가격 사용 = {_yf_bundle['price']:.2f}")
                                break
                        except Exception:
                            pass
                # 가격 취득 성공 시 재시도 루프 탈출
                if _yf_bundle["price"] > 0:
                    break
                # 첫 번째 시도 실패 시 0.8초 대기 후 재시도
                if _attempt == 0:
                    print(f"  {ticker_str}: 1차 가격 fetch 실패, 재시도...")
                    time.sleep(0.8)
            except Exception as e:
                print(f"  {ticker_str} yfinance 오류 (시도 {_attempt+1}): {e}")
                if _attempt == 0:
                    time.sleep(0.8)

        # price 폴백 3: yf.download (다른 엔드포인트)
        if not _yf_bundle["price"]:
            try:
                _dl = yf.download(ticker_str, period="5d", progress=False,
                                  auto_adjust=True, actions=False)
                if not _dl.empty:
                    # yfinance 0.2.44+: MultiIndex 컬럼 대응
                    _close = None
                    if "Close" in _dl.columns:
                        _close = _dl["Close"]
                    elif hasattr(_dl.columns, "levels") and ("Close", ticker_str) in _dl.columns:
                        _close = _dl[("Close", ticker_str)]
                    if _close is not None:
                        _last = _close.dropna().iloc[-1]
                        if _last > 0:
                            _yf_bundle["price"] = float(_last)
                            print(f"  {ticker_str}: yf.download 가격 사용 = {_yf_bundle['price']:.2f}")
            except Exception as _dl_err:
                print(f"  {ticker_str}: yf.download 오류: {_dl_err}")

        # price 폴백 4: Yahoo Finance v8 Chart API 직접 호출
        # yfinance의 cookie/crumb 인증을 완전히 우회하는 별도 엔드포인트
        if not _yf_bundle["price"]:
            for _yf_host in ("query1.finance.yahoo.com", "query2.finance.yahoo.com"):
                try:
                    _url = (f"https://{_yf_host}/v8/finance/chart/{ticker_str}"
                            f"?interval=1d&range=2d&includePrePost=false")
                    _r = _YF_SESSION.get(_url, timeout=10)
                    _j = _r.json()
                    _meta = _j["chart"]["result"][0]["meta"]
                    _p = float(
                        _meta.get("regularMarketPrice") or
                        _meta.get("previousClose") or 0
                    )
                    if _p > 0:
                        _yf_bundle["price"] = _p
                        # mktcap/shares도 여기서 보완
                        if not _yf_bundle["mktcap"]:
                            _yf_bundle["mktcap"] = float(_meta.get("marketCap") or 0)
                        print(f"  {ticker_str}: v8 Chart API 가격 사용 = {_p:.2f}")
                        break
                except Exception as _v8_err:
                    print(f"  {ticker_str}: v8 API ({_yf_host}) 오류: {_v8_err}")

        # shares 폴백
        if not _yf_bundle["shares"]:
            for _sk in ["sharesOutstanding", "impliedSharesOutstanding", "floatShares"]:
                _sv = safe(_yf_bundle["info"].get(_sk))
                if _sv and _sv > 1e6:
                    _yf_bundle["shares"] = _sv
                    break
        # shares 최종 폴백: 시총 / 가격
        if (not _yf_bundle["shares"] or _yf_bundle["shares"] < 1e6) and \
           _yf_bundle["mktcap"] > 0 and _yf_bundle["price"] > 0:
            _yf_bundle["shares"] = _yf_bundle["mktcap"] / _yf_bundle["price"]
            print(f"  {ticker_str}: 주식수 추정(시총÷가격) = {_yf_bundle['shares']/1e9:.2f}B")
        # mktcap 폴백
        if not _yf_bundle["mktcap"]:
            _yf_bundle["mktcap"] = safe(_yf_bundle["info"].get("marketCap"))

        _cset(yf_ck, _yf_bundle, PRICE_TTL)

    info       = _yf_bundle["info"]
    price      = _yf_bundle["price"] or 0.0
    # ── shares_yf: 0 이면 1.0 으로 임시 설정 (아래서 market_cap으로 재추정)
    _shares_raw = _yf_bundle["shares"]
    shares_yf   = _shares_raw if _shares_raw > 1e6 else 0.0  # 1M 미만 → 0 (신뢰불가)
    market_cap  = _yf_bundle["mktcap"] or safe(info.get("marketCap"))
    # market_cap 폴백: info 추가 필드
    if not market_cap:
        market_cap = safe(info.get("marketCap") or info.get("enterpriseValue"))
    # shares_yf 폴백: market_cap ÷ price (가장 신뢰성 높음)
    if not shares_yf and market_cap > 0 and price > 0:
        shares_yf = market_cap / price
        print(f"  {ticker_str}: 주식수 → 시총/가격 = {shares_yf/1e9:.3f}B")
    # market_cap 폴백: price × shares (shares가 있을 때)
    if not market_cap and price > 0 and shares_yf > 1e6:
        market_cap = price * shares_yf
    # 최종 안전장치: 여전히 0이면 최소값 설정 (FCF per share 계산 방지)
    if not shares_yf:
        shares_yf = 1.0   # frontend 가드(< 1e6)가 DCF null 반환 처리함

    pe_yf      = safe(info.get("trailingPE") or info.get("forwardPE"))
    beta       = safe(info.get("beta") or 1)
    name       = info.get("longName") or info.get("shortName") or ticker_str
    sector     = info.get("sector") or "Unknown"
    industry   = info.get("industry") or ""

    # ── 2. QuickFS: 재무제표 펀더멘털 (7일 캐시) ──────────────────
    qfs_ck     = f"qfs:bundle:{ticker_str}"
    fund       = _cget(qfs_ck)

    if fund is None and (qfs_key or QFS_API_KEY):
        METRICS = [
            ("revenue",           "TTM"),
            ("gross_profit",      "TTM"),
            ("operating_income",  "TTM"),
            ("net_income",        "TTM"),
            ("eps_diluted",       "TTM"),
            ("operating_cash_flow","TTM"),
            ("free_cash_flow",    "TTM"),
            ("capex",             "TTM"),
            ("total_debt",        "Annual"),
            ("total_equity",      "Annual"),
            ("long_term_debt",    "Annual"),
            ("cash_and_equivalents","Annual"),
            ("shares_diluted",    "Annual"),
            ("book_value_per_share","Annual"),
            ("roe",               "TTM"),
            ("roic",              "TTM"),
            ("gross_margin",      "TTM"),
        ]
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
            futs = {ex.submit(qfs_last, company, m, p, qfs_key): (m, p)
                    for m, p in METRICS}
            fund = {}
            for fut, (metric, _) in futs.items():
                try:
                    fund[metric] = fut.result()
                except Exception:
                    fund[metric] = 0.0
        _cset(qfs_ck, fund, FUND_TTL)

    # QuickFS 없으면 yfinance 데이터로 폴백
    if not fund:
        fund = {}
        try:
            t = yf.Ticker(ticker_str)
            cf = t.cash_flow
            bs = t.balance_sheet
            inc = t.income_stmt

            def row(df, *keys):
                if df is None or df.empty:
                    return 0
                for k in keys:
                    if k in df.index:
                        try:
                            return safe(df.loc[k].iloc[0])
                        except Exception:
                            pass
                    for idx in df.index:
                        if k.lower() in str(idx).lower():
                            try:
                                r = safe(df.loc[idx].iloc[0])
                                if r: return r
                            except Exception:
                                pass
                return 0

            ocf   = row(cf,  "Operating Cash Flow", "Total Cash From Operating Activities")
            capex = abs(row(cf, "Capital Expenditure", "Purchase Of PPE"))
            fcf   = row(cf,  "Free Cash Flow") or (ocf - capex)
            fund = {
                "revenue":            safe(info.get("totalRevenue")) or row(inc, "Total Revenue"),
                "gross_profit":       row(inc, "Gross Profit"),
                "operating_income":   row(inc, "EBIT", "Operating Income"),
                "net_income":         row(inc, "Net Income"),
                "eps_diluted":        safe(info.get("trailingEps")),
                "operating_cash_flow":ocf or safe(info.get("operatingCashflow")),
                "free_cash_flow":     fcf or safe(info.get("freeCashflow")),
                "capex":              capex or abs(safe(info.get("capitalExpenditures", 0))),
                "total_debt":         row(bs, "Long Term Debt") + row(bs, "Current Debt"),
                "total_equity":       row(bs, "Stockholders Equity", "Total Stockholder Equity") or 1,
                "long_term_debt":     row(bs, "Long Term Debt"),
                "cash_and_equivalents": row(bs, "Cash And Cash Equivalents") or safe(info.get("totalCash")),
                "shares_diluted":     shares_yf,
                "book_value_per_share": safe(info.get("bookValue")),
                "roe":                safe(info.get("returnOnEquity")),
                "roic":               0,
                "gross_margin":       safe(info.get("grossMargins")),
            }
        except Exception as e:
            print(f"  yfinance fallback error ({ticker_str}): {e}")

    # ── 3. 값 추출 ─────────────────────────────────────────────────
    total_rev    = fund.get("revenue", 0) or safe(info.get("totalRevenue", 0))
    gross_profit = fund.get("gross_profit", 0)
    ebit         = fund.get("operating_income", 0)
    net_income   = fund.get("net_income", 0)
    eps          = fund.get("eps_diluted", 0) or safe(info.get("trailingEps", 0))
    ocf          = fund.get("operating_cash_flow", 0)
    capex        = abs(fund.get("capex", 0))
    fcf          = fund.get("free_cash_flow", 0) or max(ocf - capex, 0)
    # ── 금융주 FCF 폴백 ─────────────────────────────────────────────
    # 은행·보험·증권사는 영업현금흐름 구조가 달라 yfinance/QuickFS의
    # free_cash_flow 가 0 또는 음수로 잡히는 경우가 많음 (JPM 등).
    # 이 경우 순이익(net_income)을 FCF 대리지표로 사용.
    # 0.7 계수: 세후 순이익 중 주주귀속 현금 추정 (보수적 할인)
    if not fcf and sector in ("Financials", "Financial Services") and net_income > 0:
        fcf = net_income * 0.7
        print(f"  {ticker_str}: 금융주 FCF 폴백 → 순이익×0.7 = {fcf/1e9:.2f}B")
    # yfinance info에서도 freeCashflow 폴백 시도
    if not fcf:
        fcf = safe(info.get("freeCashflow", 0))
    if not fcf and net_income > 0:
        fcf = net_income * 0.7   # 최후 폴백: 업종 무관 순이익 기반
    total_debt   = fund.get("total_debt", 0)
    lt_debt      = fund.get("long_term_debt", 0)
    equity       = fund.get("total_equity", 0) or 1
    total_cash   = fund.get("cash_and_equivalents", 0) or safe(info.get("totalCash", 0))
    shares_q     = fund.get("shares_diluted", 0)
    # 1M 이상이어야 유효한 주식수 (공개기업 최소 기준)
    # shares_yf는 위에서 market_cap/price로 재추정한 값
    shares       = shares_q if shares_q > 1e6 else shares_yf
    book_val     = fund.get("book_value_per_share", 0) or safe(info.get("bookValue", 0))
    roe          = fund.get("roe", 0) or safe(info.get("returnOnEquity", 0))
    roic         = fund.get("roic", 0)
    gross_margin = fund.get("gross_margin", 0) or safe(info.get("grossMargins", 0))
    d2e          = (total_debt / equity) if equity > 0 else safe(info.get("debtToEquity", 0)) / 100
    ev           = market_cap + total_debt - total_cash

    # QuickFS는 growth를 직접 안 주므로 yfinance growth 유지
    rev_growth = safe(info.get("revenueGrowth", 0))
    earn_growth = safe(info.get("earningsGrowth", 0))

    # ── 4. 이전 연도 데이터 (growth 계산용) ─────────────────────
    # QuickFS 있으면 2년치 가져오기, 없으면 yfinance growth 사용
    rev_prev = 0
    fcf_prev = 0
    if (qfs_key or QFS_API_KEY):
        rev_series = qfs_series(company, "revenue",        "Annual", 2, qfs_key or QFS_API_KEY)
        fcf_series = qfs_series(company, "free_cash_flow", "Annual", 2, qfs_key or QFS_API_KEY)
        if len(rev_series) >= 2:
            rev_prev = rev_series[-2]
        if len(fcf_series) >= 2:
            fcf_prev = fcf_series[-2]

    # ── 5. FMP-flat 포맷으로 반환 (front-end mapYFToStock 호환) ──
    result = {
        "quote": {
            "price":            price,
            "marketCap":        market_cap,
            "sharesOutstanding":shares,
            "pe":               pe_yf,
            "eps":              eps,
            "name":             name,
            "exchange":         info.get("exchange") or "",
        },
        "km": {
            "roe":                       roe,
            "roic":                      roic,
            "debtToEquity":              d2e,
            "freeCashFlowYield":         fcf / market_cap if market_cap > 0 and fcf > 0 else 0,
            "pfcfRatio":                 market_cap / fcf if market_cap > 0 and fcf > 0 else 0,
            "enterpriseValueOverEBITDA": ev / ebit if ebit > 0 else 0,
            "enterpriseValue":           ev,
            "peRatio":                   pe_yf,
        },
        "inc": {
            "revenue":         total_rev,
            "grossProfit":     gross_profit,
            "operatingIncome": ebit,
            "netIncome":       net_income,
        },
        "incPrev": {"revenue": rev_prev} if rev_prev else None,
        "cf": {
            "operatingCashFlow":  ocf,
            "capitalExpenditure": -capex,   # FMP convention: negative capex
            "freeCashFlow":       fcf,
        },
        "cfPrev": {"freeCashFlow": fcf_prev} if fcf_prev else None,
        "bs": {
            "totalDebt":             total_debt,
            "totalEquity":           equity,
            "totalAssets":           0,
            "cashAndCashEquivalents":total_cash,
            "bookValuePerShare":     book_val,
        },
        "_source": "QuickFS+yfinance" if (qfs_key or QFS_API_KEY) else "yfinance",
        "_sector": sector,
        "_industry": industry,
        # yfinance growth 필드 — QuickFS incPrev 없을 때 대시보드 폴백으로 사용
        "_growth": {
            "revenueGrowth":  rev_growth,
            "earningsGrowth": earn_growth,
        },
    }
    return {"quoteSummary": {"result": [result], "error": None}}


def build_financials(ticker_str, qfs_key=None, years=5):
    """
    N년치 연간 재무 데이터 (DCF 현금창출능력 모델용).
    QuickFS available → 최대 10년. 없으면 yfinance 4년 폴백.
    """
    company    = f"US:{ticker_str}"
    key        = qfs_key or QFS_API_KEY
    use_qfs    = bool(key)

    if use_qfs:
        # ── QuickFS: 5~10년치 연간 데이터 ─────────────────────────
        METRICS = ["revenue", "operating_cash_flow", "capex",
                   "cash_and_equivalents", "total_debt", "shares_diluted"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(qfs_series, company, m, "Annual", years, key): m
                    for m in METRICS}
            raw = {}
            for fut, metric in futs.items():
                try:
                    raw[metric] = fut.result()
                except Exception:
                    raw[metric] = [0.0] * years

        revenues   = raw.get("revenue",            [0.0] * years)
        oper_cfs   = raw.get("operating_cash_flow", [0.0] * years)
        capexs     = [abs(c) for c in raw.get("capex", [0.0] * years)]
        cash       = raw.get("cash_and_equivalents", [0.0])[-1]
        total_debt = raw.get("total_debt",           [0.0])[-1]
        shares     = raw.get("shares_diluted",       [0.0])[-1]

        # 연도 레이블 (오래된 순)
        cur_yr     = datetime.datetime.now().year
        year_labels = [str(cur_yr - (years - 1) + i) for i in range(years)]
        source     = "QuickFS"

    else:
        # ── yfinance 폴백 ──────────────────────────────────────────
        t    = yf.Ticker(ticker_str)
        info = t.info or {}
        years = 5

        def extract(df, *keys):
            if df is None or df.empty:
                return [0.0] * years
            for k in keys:
                for idx in df.index:
                    if k.lower() in str(idx).lower():
                        vals = []
                        for col in sorted(df.columns, reverse=True)[:years]:
                            try:
                                vals.append(safe(df.loc[idx, col]))
                            except Exception:
                                vals.append(0.0)
                        vals = list(reversed(vals))
                        while len(vals) < years:
                            vals.append(0.0)
                        return vals[:years]
            return [0.0] * years

        try:
            revenues = extract(t.income_stmt, "Total Revenue")
        except Exception:
            revenues = [0.0] * years
        try:
            oper_cfs = extract(t.cash_flow,
                               "Operating Cash Flow",
                               "Total Cash From Operating Activities")
            capexs   = [abs(x) for x in extract(t.cash_flow,
                                                  "Capital Expenditure",
                                                  "Purchase Of PPE")]
        except Exception:
            oper_cfs = [0.0] * years
            capexs   = [0.0] * years

        cash       = 0
        total_debt = 0
        shares     = safe(info.get("sharesOutstanding", 0))
        try:
            bs = t.balance_sheet
            if bs is not None and not bs.empty:
                for idx in bs.index:
                    if "cash" in str(idx).lower():
                        cash = safe(bs.loc[idx].iloc[0])
                        break
                ltd = std = 0
                for idx in bs.index:
                    if "long term debt" in str(idx).lower():
                        ltd = safe(bs.loc[idx].iloc[0])
                    if "current debt" in str(idx).lower():
                        std = safe(bs.loc[idx].iloc[0])
                total_debt = ltd + std
        except Exception:
            cash = safe(info.get("totalCash", 0))

        try:
            cols = sorted(t.income_stmt.columns, reverse=True)[:years]
            year_labels = [str(c.year) for c in reversed(cols)]
            while len(year_labels) < years:
                year_labels.append("")
        except Exception:
            cur_yr = datetime.datetime.now().year
            year_labels = [str(cur_yr - (years - 1) + i) for i in range(years)]

        info2  = t.info or {}
        source = "yfinance"

    # 가격·이름은 build_quotesummary 에서 이미 수집된 bundle 재사용
    _bk = f"yf:bundle:{ticker_str}"
    _b  = _cget(_bk) or {}
    price  = _b.get("price") or 0.0
    _info2 = _b.get("info") or {}
    name   = _info2.get("longName") or _info2.get("shortName") or ticker_str
    sector = _info2.get("sector") or ""

    return {
        "ticker":     ticker_str,
        "name":       name,
        "sector":     sector,
        "price":      price,
        "shares":     shares,
        "cash":       round(cash       / 1e6, 2),   # $M
        "totalDebt":  round(total_debt / 1e6, 2),   # $M
        "yearLabels": year_labels,
        "revenues":   [round(r / 1e6, 2) for r in revenues],
        "operatingCFs":[round(o / 1e6, 2) for o in oper_cfs],
        "totalCapexs": [round(c / 1e6, 2) for c in capexs],
        "_source":    source,
    }


def build_chart(ticker_str, range_="6mo"):
    """주가 차트 (yfinance, 무료)."""
    t = yf.Ticker(ticker_str)
    hist = t.history(period=range_, interval="1mo")
    closes, timestamps = [], []
    for ts, row_data in hist.iterrows():
        c = row_data.get("Close")
        closes.append(round(float(c), 2)
                      if c is not None and not math.isnan(float(c)) else None)
        try:
            timestamps.append(int(ts.timestamp()))
        except Exception:
            timestamps.append(0)
    return {
        "chart": {
            "result": [{
                "meta":       {"symbol": ticker_str.upper()},
                "timestamp":  timestamps,
                "indicators": {"quote": [{"close": closes}]},
            }],
            "error": None,
        }
    }


# ── HTTP Handler ───────────────────────────────────────────────────
class Handler(http.server.SimpleHTTPRequestHandler):

    def log_message(self, fmt, *args):
        if any(p in self.path for p in ["/yf", "/chart", "/financials", "/qfs"]):
            tick = self.path.split("ticker=")[-1].split("&")[0] if "ticker=" in self.path else ""
            src  = "QFS+YF" if (QFS_API_KEY or self.headers.get("X-QFS-Key")) else "YF"
            print(f"  [{src}] {self.path.split('?')[0]}  {tick}")

    def _json(self, code, obj):
        data = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type",  "application/json")
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Headers",
                         "X-QFS-Key, Accept, Content-Type")
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers",
                         "X-QFS-Key, Accept, Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed  = urllib.parse.urlparse(self.path)
        params  = urllib.parse.parse_qs(parsed.query)
        qfs_key = self.headers.get("X-QFS-Key") or QFS_API_KEY

        if parsed.path in ("/", ""):
            self.send_response(302)
            self.send_header("Location",
                             "/hilliard-buffshire-dashboard.html")
            self.end_headers()
            return

        # ── /yf  — 주식 스냅샷 (QuickFS 펀더멘털 + yfinance 가격) ──
        if parsed.path == "/yf":
            ticker = (params.get("ticker", [None])[0] or "").upper().strip()
            ticker = ticker.replace(".", "-")
            if not ticker:
                self._json(400, {"error": "ticker required"}); return
            try:
                self._json(200, build_quotesummary(ticker, qfs_key))
            except Exception as e:
                self._json(500, {"error": str(e)})

        # ── /financials  — 5~10년치 재무 데이터 (CashFlowModeler 용) ──
        elif parsed.path == "/financials":
            ticker = (params.get("ticker", [None])[0] or "").upper().strip()
            ticker = ticker.replace(".", "-")
            yrs    = int(params.get("years", ["5"])[0])
            yrs    = max(1, min(10, yrs))
            if not ticker:
                self._json(400, {"error": "ticker required"}); return
            try:
                self._json(200, build_financials(ticker, qfs_key, yrs))
            except Exception as e:
                self._json(500, {"error": str(e)})

        # ── /chart  — 주가 차트 ──────────────────────────────────────
        elif parsed.path == "/chart":
            ticker = (params.get("ticker", [None])[0] or "").upper().strip()
            ticker = ticker.replace(".", "-")
            range_ = params.get("range", ["6mo"])[0]
            if not ticker:
                self._json(400, {"error": "ticker required"}); return
            try:
                self._json(200, build_chart(ticker, range_))
            except Exception as e:
                self._json(500, {"error": str(e)})

        # ── /qfs-test  — API 키 유효성 확인 ─────────────────────────
        elif parsed.path == "/qfs-test":
            if not qfs_key:
                self._json(200, {"ok": False,
                                 "msg": "API 키 없음 — yfinance 모드로 작동 중"}); return
            try:
                vals = qfs_fetch("US:AAPL", "revenue", "TTM", 1, qfs_key)
                if vals:
                    self._json(200, {"ok": True,
                                     "msg": f"✓ QuickFS 연결 성공 (AAPL revenue: ${vals[-1]/1e9:.1f}B)"})
                else:
                    self._json(200, {"ok": False,
                                     "msg": "QuickFS 응답 없음 — 키를 확인하세요"})
            except Exception as e:
                self._json(200, {"ok": False, "msg": str(e)})

        # ── /cache-clear  — 캐시 초기화 ─────────────────────────────
        elif parsed.path == "/cache-clear":
            with _cache_lock:
                _cache.clear()
            self._json(200, {"ok": True, "msg": "캐시 초기화 완료"})

        else:
            super().do_GET()


# ── Main ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    qfs_status = f"✓ 키 설정됨 ({QFS_API_KEY[:6]}...)" if QFS_API_KEY else "✗ 키 없음 → yfinance 폴백"
    print()
    print("  ════════════════════════════════════════════════")
    print("    Hilliard Buffshire Investment Dashboard")
    print("  ════════════════════════════════════════════════")
    print(f"  QuickFS : {qfs_status}")
    print(f"  yfinance: 실시간 가격 (항상 사용)")
    print(f"  캐시    : 펀더멘털 7일 / 가격 5분")
    print(f"  Open    : http://localhost:{PORT}")
    print("  ────────────────────────────────────────────────")
    print()

    try:
        server = http.server.HTTPServer(("", PORT), Handler)
    except OSError as e:
        print(f"  ERROR: Port {PORT} 사용 중. 다른 서버를 먼저 종료하세요.")
        sys.exit(1)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  서버 종료.")
