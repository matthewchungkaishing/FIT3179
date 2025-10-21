"""
DV2 | Sun & Skin: UV + Melanoma wrangling
------------------------------------------------------------
END PRODUCTS
UV:
1) data/uv_monthly_by_year.csv
   - 8 capitals × selected YEARS × 12 months
   - city, year, month, mean_daily_max_uvi
   - minute-level CSV -> daily max -> monthly mean of daily max.

2) data/uv_climatology_selected_years.csv
   - 8 capitals × 12 months
   - city, month, mean_daily_max_uvi_clim
   - average of (1) across selected YEARS for each month.

3) data/uv_state_metric.csv
   - 8 rows (one per state/territory via its capital)
   - state_code, state_name, capital, annual_mean_uvi, peak_month, peak_uvi
   - from (2): annual mean & peak month/UV + state mapping.

Melanoma (AIHW Book 7):
4) data/melanoma_rates_state_2017_2021.csv
   - state_code, state_name, year, asr_per_100k, count
   - filtered to 'Melanoma of the skin' | 'Incidence' | 'Persons' | years 2017–2021
   - uses a single age-standardised rate column (set RATE_STANDARD below).

5) data/melanoma_rates_state_5yr_mean.csv
   - state_code, state_name, asr_2017_2021_mean, count_sum

Join for the scatter:
6) data/uv_melanoma_scatter.csv
   - state_code, state_name, annual_mean_uvi, asr_2017_2021_mean, count_sum
"""

import os, re, io, requests
import pandas as pd
from tqdm import tqdm

# -------------------------
# SETTINGS
# -------------------------
YEARS = [2022, 2023, 2024]                 
OUTDIR = "data"
os.makedirs(OUTDIR, exist_ok=True)

# Path to AIHW Book 7 Excel (put the file in data)
AIHW_BOOK7_PATH = os.path.join(
    OUTDIR,
    "CDiA-2025-Book-7-Cancer-incidence-and-mortality-by-state-and-territory.xlsx"
)

# Choose one ASR standard for melanoma rates: "2001" or "2025"
RATE_STANDARD = "2001"

# CKAN package slugs on data.gov.au (Hobart uses Kingston)
PACKAGES = {
    "Adelaide":  "ultraviolet-radiation-index-adelaide",
    "Brisbane":  "ultraviolet-radiation-index-brisbane",
    "Canberra":  "ultraviolet-radiation-index-canberra",
    "Darwin":    "ultraviolet-radiation-index-darwin",
    "Hobart":    "ultraviolet-radiation-index-kingston",
    "Melbourne": "ultraviolet-radiation-index-melbourne",
    "Perth":     "ultraviolet-radiation-index-perth",
    "Sydney":    "ultraviolet-radiation-index-sydney",
}

# Capital -> state code/name (for the state summary)
CAP_TO_STATE = {
    "Adelaide":  ("SA",  "South Australia"),
    "Brisbane":  ("QLD", "Queensland"),
    "Canberra":  ("ACT", "Australian Capital Territory"),
    "Darwin":    ("NT",  "Northern Territory"),
    "Hobart":    ("TAS", "Tasmania"),
    "Melbourne": ("VIC", "Victoria"),
    "Perth":     ("WA",  "Western Australia"),
    "Sydney":    ("NSW", "New South Wales"),
}

NAME_TO_CODE = {
    "New South Wales":"NSW",
    "Victoria":"VIC",
    "Queensland":"QLD",
    "South Australia":"SA",
    "Western Australia":"WA",
    "Tasmania":"TAS",
    "Northern Territory":"NT",
    "Australian Capital Territory":"ACT",
}

CKAN = "https://data.gov.au/data/api/3/action/package_show?id="

# -------------------------
# HELPERS: ARPANSA listing + parsing
# -------------------------
def list_year_resources(package_id: str, city_name_for_files: str, years: list[int]):
    """Query CKAN and return a list: [(year, url, resource_name), ...] matching 'City-YYYY.csv'."""
    r = requests.get(CKAN + package_id, timeout=60)
    r.raise_for_status()
    out = []
    for res in r.json()["result"]["resources"]:
        name = (res.get("name") or "").strip()
        url  = res.get("url")
        m = re.search(rf"{re.escape(city_name_for_files)}-(\d{{4}})\.csv$", name, re.I)
        if m:
            y = int(m.group(1))
            if y in years:
                out.append((y, url, name))
    return sorted(out)

def parse_uv_csv(content: bytes) -> pd.DataFrame:
    """
    Robustly parse ARPANSA UV CSVs with varying headers.
    Returns columns: date (python date), uv (float).
    """
    # Tolerant decoding
    try:
        df = pd.read_csv(io.StringIO(content.decode("utf-8-sig", errors="ignore")))
    except UnicodeDecodeError:
        df = pd.read_csv(io.StringIO(content.decode("latin-1", errors="ignore")))

    # Normalise column names
    def norm(s): return re.sub(r"[^a-z0-9]+", "", str(s).lower())
    norm_map = {c: norm(c) for c in df.columns}
    inv = {v: k for k, v in norm_map.items()}  # normalised -> original

    # UV column
    uv_col = None
    for key in ["uvindex","uv_index","uv","uvi","uv1min","uvindex1min","uvindexminute"]:
        if key in inv: uv_col = inv[key]; break
    if uv_col is None:
        uv_guess = [orig for orig, n in norm_map.items() if n.startswith("uv")]
        if uv_guess: uv_col = uv_guess[0]

    # Time column
    t_col = None
    for key in ["utctime","utc","timestamp","datetime","datetimeutc","date_time","datetimelocal",
                "date","time","datetimeaest","datetimeaedt","datetimeacst","datetimeawst",
                "datetimeawdt","datetimeacdt"]:
        if key in inv: t_col = inv[key]; break
    if t_col is None:
        maybe = [orig for orig, n in norm_map.items() if ("time" in n or "date" in n)]
        if maybe: t_col = maybe[0]

    if uv_col is None or t_col is None:
        raise ValueError(f"Could not locate UV or time columns. Found: {df.columns.tolist()}")

    ts = pd.to_datetime(df[t_col], errors="coerce", utc=False)
    uv = pd.to_numeric(df[uv_col], errors="coerce")
    out = pd.DataFrame({"__ts": ts, "uv": uv}).dropna()
    out = out[(out["uv"] >= 0) & (out["uv"] <= 25)]  # plausible UV
    out["date"] = out["__ts"].dt.date
    return out[["date","uv"]]

def monthly_mean_of_daily_max(df: pd.DataFrame) -> pd.DataFrame:
    """Minute -> Daily max -> Monthly mean of daily max."""
    daily = df.groupby("date", as_index=False)["uv"].max()
    daily["year"] = pd.to_datetime(daily["date"]).dt.year
    daily["month"] = pd.to_datetime(daily["date"]).dt.month
    monthly = daily.groupby(["year","month"], as_index=False)["uv"].mean()
    monthly.rename(columns={"uv":"mean_daily_max_uvi"}, inplace=True)
    return monthly

# -------------------------
# HELPERS: AIHW (Book 7) parsing
# -------------------------
def read_book7_filtered(book_path: str, rate_standard: str = "2001") -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Reads 'Table S7.1' from Book 7 and filters to:
      - Data type: Incidence
      - Cancer group/site: Melanoma of the skin
      - Sex: Persons
      - Years: 2017..2021
    Picks a single ASR column (2001 or 2025) and returns:
      (yearly_df, mean_df)

    yearly_df: state_code, state_name, year, asr_per_100k, count
    mean_df:   state_code, state_name, asr_2017_2021_mean, count_sum
    """
    # Header row is at index 5 (0-based), based on the workbook layout
    df = pd.read_excel(book_path, sheet_name="Table S7.1", header=5)

    # Pick the ASR column
    if str(rate_standard) == "2025":
        rate_col = [c for c in df.columns if "Age-standardised rate" in str(c) and "2025" in str(c)]
    else:
        rate_col = [c for c in df.columns if "Age-standardised rate" in str(c) and "2001" in str(c)]
    if not rate_col:
        raise RuntimeError("Could not find an 'Age-standardised rate' column for the chosen standard.")
    rate_col = rate_col[0]

    # Filter to melanoma incidence (persons) and target years
    sub = df[(df["Data type"] == "Incidence") &
             (df["Cancer group/site"] == "Melanoma of the skin") &
             (df["Sex"] == "Persons")].copy()

    # Clean year and keep 2017..2021
    sub["Year"] = pd.to_numeric(sub["Year"], errors="coerce").astype("Int64")
    sub = sub[sub["Year"].between(2017, 2021, inclusive="both")]

    # Drop the Australia total row
    sub = sub[sub["State or Territory"] != "Australia"]

    # Cast values
    sub["asr_per_100k"] = pd.to_numeric(sub[rate_col], errors="coerce")
    sub["count"] = pd.to_numeric(sub["Count"], errors="coerce")

    yearly = sub.rename(columns={"State or Territory":"state_name", "Year":"year"})[
        ["state_name","year","asr_per_100k","count"]
    ].copy()
    yearly["state_code"] = yearly["state_name"].map(NAME_TO_CODE)
    yearly = yearly[["state_code","state_name","year","asr_per_100k","count"]].sort_values(["state_code","year"])

    mean = (yearly.groupby(["state_code","state_name"], as_index=False)
                 .agg(asr_2017_2021_mean=("asr_per_100k","mean"),
                      count_sum=("count","sum")))
    return yearly, mean

# -------------------------
# MAIN
# -------------------------
def main():
    # === ARPANSA UV: download → aggregate → export ===
    all_rows = []
    for city, pkg in PACKAGES.items():
        city_label_for_files = "Kingston" if city == "Hobart" else city
        resources = list_year_resources(pkg, city_label_for_files, YEARS)
        if not resources:
            print(f"WARNING: no resources found for {city} in {YEARS}")
            continue
        for y, url, name in tqdm(resources, desc=f"Downloading {city}", leave=False):
            csv_bytes = requests.get(url, timeout=120).content
            df_raw = parse_uv_csv(csv_bytes)
            df_month = monthly_mean_of_daily_max(df_raw)
            df_month["city"] = city
            all_rows.append(df_month)

    if not all_rows:
        raise RuntimeError("No UV data downloaded — check YEARS and network access.")

    # 1) Monthly by year
    uv_monthly = pd.concat(all_rows, ignore_index=True)
    uv_monthly = (uv_monthly[["city","year","month","mean_daily_max_uvi"]]
                  .sort_values(["city","year","month"]))
    monthly_path = os.path.join(OUTDIR, "uv_monthly_by_year.csv")
    uv_monthly.to_csv(monthly_path, index=False)

    # 2) Climatology across selected YEARS
    clim = (uv_monthly
            .groupby(["city","month"], as_index=False)["mean_daily_max_uvi"]
            .mean()
            .rename(columns={"mean_daily_max_uvi":"mean_daily_max_uvi_clim"}))
    clim_path = os.path.join(OUTDIR, "uv_climatology_selected_years.csv")
    clim.to_csv(clim_path, index=False)

    # 3) State-level UV summary for map + scatter
    annual = (clim.groupby("city", as_index=False)["mean_daily_max_uvi_clim"]
                   .mean()
                   .rename(columns={"mean_daily_max_uvi_clim":"annual_mean_uvi"}))
    idx = clim.groupby("city")["mean_daily_max_uvi_clim"].idxmax()
    peak = clim.loc[idx, ["city","month","mean_daily_max_uvi_clim"]].rename(
        columns={"month":"peak_month","mean_daily_max_uvi_clim":"peak_uvi"}
    )

    uv_state = annual.merge(peak, on="city")
    uv_state["state_code"] = uv_state["city"].map(lambda c: CAP_TO_STATE[c][0])
    uv_state["state_name"] = uv_state["city"].map(lambda c: CAP_TO_STATE[c][1])
    uv_state["capital"] = uv_state["city"]
    uv_state = uv_state[["state_code","state_name","capital","annual_mean_uvi","peak_month","peak_uvi"]] \
                     .sort_values("annual_mean_uvi", ascending=False)
    state_uv_path = os.path.join(OUTDIR, "uv_state_metric.csv")
    uv_state.to_csv(state_uv_path, index=False)

    print("\n✅ UV files:")
    print(" -", monthly_path)
    print(" -", clim_path)
    print(" -", state_uv_path)

    # === AIHW MELANOMA: read → filter → export ===
    if not os.path.exists(AIHW_BOOK7_PATH):
        print(f"\n⚠️  Skipping melanoma: Excel not found at {AIHW_BOOK7_PATH}")
        return

    yearly, mean = read_book7_filtered(AIHW_BOOK7_PATH, rate_standard=RATE_STANDARD)

    melanoma_yearly_path = os.path.join(OUTDIR, "melanoma_rates_state_2017_2021.csv")
    melanoma_mean_path   = os.path.join(OUTDIR, "melanoma_rates_state_5yr_mean.csv")
    yearly.to_csv(melanoma_yearly_path, index=False)
    mean.to_csv(melanoma_mean_path, index=False)

    print("\n✅ Melanoma files:")
    print(" -", melanoma_yearly_path)
    print(" -", melanoma_mean_path)

    # === JOIN for the scatter ===
    scatter = uv_state.merge(mean[["state_code","asr_2017_2021_mean","count_sum"]],
                             on="state_code", how="inner")
    scatter = scatter[["state_code","state_name","annual_mean_uvi","asr_2017_2021_mean","count_sum"]]
    scatter_path = os.path.join(OUTDIR, "uv_melanoma_scatter.csv")
    scatter.to_csv(scatter_path, index=False)
    print("\n✅ Scatter join:")
    print(" -", scatter_path)

if __name__ == "__main__":
    main()
