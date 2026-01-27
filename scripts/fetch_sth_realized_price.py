import os
import io
import json
from datetime import datetime
import pandas as pd
import requests


URLS_TO_TRY = [
    "https://api.bitcoinmagazinepro.com/v1/metrics/sth-realized-price",
    "https://api.bitcoinmagazinepro.com/metrics/sth-realized-price",
]

OUT_PATH = "data/sth-realized-price.json"


def _clean_csv_text(raw: str) -> str:
    """
    BMP sometimes returns CSV as a quoted string with literal '\\n'.
    This normalizes it into real CSV text.
    """
    txt = (raw or "").strip()
    if not txt:
        return ""

    # If it's a quoted blob, strip quotes and unescape newlines
    if (txt.startswith('"') and txt.endswith('"')) or (txt.startswith("'") and txt.endswith("'")):
        txt = txt[1:-1]
    txt = txt.replace("\\n", "\n").replace("\\r", "\r")

    return txt.strip()


def _fetch_csv(api_key: str) -> str:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "text/csv",
    }

    last_err = None
    for url in URLS_TO_TRY:
        try:
            resp = requests.get(url, headers=headers, timeout=60)
            print("BMP API URL:", url)
            print("BMP API status code:", resp.status_code)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            last_err = e

    raise RuntimeError(f"Failed to fetch metric from all endpoints. Last error: {last_err}")


def _pick_col(cols_lower, *candidates):
    for c in candidates:
        if c in cols_lower:
            return cols_lower[c]
    return None


def main():
    api_key = os.environ.get("BMP_API_KEY")
    if not api_key:
        raise RuntimeError("Missing BMP_API_KEY env var. Add it in GitHub Secrets.")

    raw_text = _fetch_csv(api_key)
    cleaned = _clean_csv_text(raw_text)

    if not cleaned:
        raise RuntimeError("Empty response from BMP API")

    df = pd.read_csv(io.StringIO(cleaned))

    if df.empty:
        print("First 200 characters of response:", cleaned[:200])
        raise RuntimeError("Parsed empty DataFrame from CSV")

    # Map lowercase column names -> original column names
    cols_lower = {str(c).strip().lower(): c for c in df.columns}

    date_col = _pick_col(cols_lower, "date", "time", "timestamp")
    price_col = _pick_col(cols_lower, "price", "btc_price", "btcprice", "usd_price", "price_usd")
    sth_col = _pick_col(
        cols_lower,
        "sth_realized_price",
        "sthrealizedprice",
        "sth_realized",
        "realized_price",
        "realizedprice",
        "value",
    )

    if not date_col or not price_col or not sth_col:
        raise RuntimeError(
            "Could not find required columns.\n"
            f"Columns found: {list(df.columns)}\n"
            f"Picked date={date_col}, price={price_col}, sth={sth_col}"
        )

    # Parse dates
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).sort_values(date_col)

    # Numeric
    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df[sth_col] = pd.to_numeric(df[sth_col], errors="coerce")

    # 200-day moving average from BTC price (daily data expected)
    df["ma200"] = df[price_col].rolling(window=200, min_periods=200).mean()

    out = []
    for _, r in df.iterrows():
        d = r[date_col]
        if pd.isna(d):
            continue

        out.append(
            {
                "date": d.strftime("%Y-%m-%d"),
                "price": None if pd.isna(r[price_col]) else float(r[price_col]),
                "ma200": None if pd.isna(r["ma200"]) else float(r["ma200"]),
                "sth_realized": None if pd.isna(r[sth_col]) else float(r[sth_col]),
            }
        )

    os.makedirs("data", exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)

    print(f"Wrote {len(out)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
