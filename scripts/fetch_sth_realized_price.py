import os
import io
import json
from pathlib import Path

import pandas as pd
import requests

# Try both, some metrics behave differently across these
URLS_TO_TRY = [
    "https://api.bitcoinmagazinepro.com/v1/metrics/sth-realized-price",
    "https://api.bitcoinmagazinepro.com/metrics/sth-realized-price",
]

OUT_PATH = Path("data/sth-realized-price.json")


def _clean_csv_text(raw: str) -> str:
    """
    BMP sometimes returns CSV as a quoted string with literal '\\n'.
    Normalize it into real CSV text.
    """
    txt = (raw or "").strip()
    if not txt:
        return ""

    if (txt.startswith('"') and txt.endswith('"')) or (txt.startswith("'") and txt.endswith("'")):
        txt = txt[1:-1]

    txt = txt.replace("\\n", "\n").replace("\\r", "\r")
    return txt.strip()


def _fetch_csv(api_key: str) -> str:
    headers = {
        "Authorization": f"Bearer {api_key}",
        # This is the key change to avoid 406 on CSV-only endpoints
        "Accept": "text/csv, application/csv;q=0.9, */*;q=0.8",
        "User-Agent": "Mozilla/5.0",
    }

    last_err = None
    for url in URLS_TO_TRY:
        try:
            resp = requests.get(url, headers=headers, timeout=60)
            print("BMP API URL:", url)
            print("BMP API status code:", resp.status_code)
            print("Content-Type:", resp.headers.get("Content-Type"))
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            last_err = e

    raise RuntimeError(f"Failed to fetch metric from all endpoints. Last error: {last_err}")


def main():
    api_key = os.environ.get("BMP_API_KEY")
    if not api_key:
        raise RuntimeError("Missing BMP_API_KEY env var. Add it in GitHub Secrets as BMP_API_KEY.")

    raw_text = _fetch_csv(api_key)
    cleaned = _clean_csv_text(raw_text)

    if not cleaned:
        raise RuntimeError("Empty response from BMP API")

    df = pd.read_csv(io.StringIO(cleaned))
    if df.empty:
        print("First 200 characters of response:", cleaned[:200])
        raise RuntimeError("Parsed empty DataFrame from CSV")

    print("Parsed columns:", list(df.columns))

    # Common BMP CSV pattern: first column is unnamed index, then Date, Price, sth_realized_price (or similar)
    cols_lower = {str(c).strip().lower(): c for c in df.columns}

    def pick(*names):
        for n in names:
            if n in cols_lower:
                return cols_lower[n]
        return None

    date_col = pick("date", "time", "timestamp")
    price_col = pick("price", "btc_price", "btcprice", "usd_price", "price_usd")
    sth_col = pick(
        "sth_realized_price",
        "sthrealizedprice",
        "sth_realized",
        "realized_price",
        "realizedprice",
        "value",
    )

    if not date_col or not sth_col:
        raise RuntimeError(
            "Could not find required columns.\n"
            f"Columns found: {list(df.columns)}\n"
            f"Picked date={date_col}, price={price_col}, sth={sth_col}"
        )

    # Keep only what we need
    keep = [date_col, sth_col] + ([price_col] if price_col else [])
    df = df[keep].copy()

    # Parse date and coerce numerics
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).sort_values(date_col)

    if price_col:
        df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df[sth_col] = pd.to_numeric(df[sth_col], errors="coerce")

    # 200-day moving average from BTC price (only if price column exists)
    if price_col:
        df["ma200"] = df[price_col].rolling(window=200, min_periods=200).mean()
    else:
        df["ma200"] = None

    out = []
    for _, r in df.iterrows():
        d = r[date_col]
        if pd.isna(d):
            continue

        item = {
            "date": d.strftime("%Y-%m-%d"),
            "sth_realized": None if pd.isna(r[sth_col]) else float(r[sth_col]),
            "price": None,
            "ma200": None,
        }

        if price_col and pd.notna(r[price_col]):
            item["price"] = float(r[price_col])
        if price_col and pd.notna(r["ma200"]):
            item["ma200"] = float(r["ma200"])

        out.append(item)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Wrote {len(out)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
