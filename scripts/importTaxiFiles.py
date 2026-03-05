import math
import tempfile
from pathlib import Path

import pandas as pd
import requests
import time

# Config
TARGET_SIZE_GB = 30
DATA_DIR = Path("data/nyc_tlc/processed_spark")
TEMP_FILE = Path("temp_raw.parquet")
KEEP_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "tip_amount",
    "total_amount", #Perhaps import all data
]

# Aim for ~250 MiB output files
TARGET_PART_MB = 250
TARGET_PART_BYTES = TARGET_PART_MB * 1024 * 1024

# Download timeouts: (connect timeout, read timeout)
REQUEST_TIMEOUT = (15, 300)  # 15s to connect, 300s between bytes
REQUEST_HEADERS = {"User-Agent": "Mozilla/5.0"}
CHUNK_DOWNLOAD_BYTES = 1024 * 1024  # 1 MiB download chunks


def get_current_storage_gb() -> float:
    if not DATA_DIR.exists():
        return 0.0
    total_bytes = sum(f.stat().st_size for f in DATA_DIR.rglob("*.parquet"))
    return total_bytes / (1024 ** 3)


def month_already_processed(year: int, month: int) -> bool:
    mm = f"{month:02d}"
    sentinel = DATA_DIR / f"part_{year}_{mm}_0.parquet"
    return sentinel.exists()


def download_to_temp(url: str, temp_path: Path) -> None:
    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT, headers=REQUEST_HEADERS) as response:
        response.raise_for_status()
        with open(temp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=CHUNK_DOWNLOAD_BYTES):
                if chunk:
                    f.write(chunk)


def estimate_rows_per_chunk(df: pd.DataFrame, target_bytes: int) -> int:
    """
    Estimate how many rows will produce ~target_bytes of parquet output
    by writing a sample to a temporary parquet file and measuring it.
    """
    if df.empty:
        return 1

    sample_rows = min(len(df), 100_000)
    sample = df.iloc[:sample_rows].copy()

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
        sample.to_parquet(tmp.name, index=False, compression="snappy")
        sample_size = Path(tmp.name).stat().st_size

    # Avoid division by zero
    bytes_per_row = max(sample_size / sample_rows, 1)

    est_rows = int(target_bytes / bytes_per_row)

    # Keep sane bounds
    est_rows = max(est_rows, 50_000)
    est_rows = min(est_rows, len(df))

    return est_rows


def process_and_save(year: int, month: int) -> bool:
    mm = f"{month:02d}"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{mm}.parquet"

    # Skip if we've already created the first output file for this month
    if month_already_processed(year, month):
        print(f"Skipping {year}-{mm}: output already exists.")
        return True

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # 1) Download source parquet
        print(f"Downloading {year}-{mm} ...")
        download_to_temp(url, TEMP_FILE)

        # 2) Read only selected columns
        print(f"Loading {year}-{mm} into pandas ...")
        df = pd.read_parquet(TEMP_FILE, columns=KEEP_COLUMNS, engine="pyarrow")

        if df.empty:
            print(f"Skipping {year}-{mm}: file loaded but contains no rows.")
            return True

        # 3) Estimate rows per output part to target ~250 MiB each
        rows_per_chunk = estimate_rows_per_chunk(df, TARGET_PART_BYTES)
        num_parts = math.ceil(len(df) / rows_per_chunk)

        print(
            f"Processed {year}-{mm}: {len(df):,} rows. "
            f"Estimated rows/part: {rows_per_chunk:,} (~{TARGET_PART_MB} MB target), "
            f"parts: {num_parts}"
        )

        # 4) Write chunked parquet outputs
        for i, start in enumerate(range(0, len(df), rows_per_chunk)):
            out_path = DATA_DIR / f"part_{year}_{mm}_{i}.parquet"

            # If rerun partially, skip existing parts
            if out_path.exists():
                print(f"  Skipping existing {out_path.name}")
                continue

            chunk = df.iloc[start:start + rows_per_chunk]
            chunk.to_parquet(out_path, index=False, compression="snappy")

            actual_mb = out_path.stat().st_size / (1024 * 1024)
            print(f"  Wrote {out_path.name} ({actual_mb:.1f} MB)")

        return True

    except Exception as e:
        print(f"Skipping {year}-{mm}: {e}")
        return False

    finally:
        if TEMP_FILE.exists():
            TEMP_FILE.unlink()


def run_pipeline() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    year = 2025
    min_year = 2009
    years_processed_counter = 0

    while get_current_storage_gb() < TARGET_SIZE_GB:
        print(f"\nCurrent data size: {get_current_storage_gb():.2f} GB")

        made_progress_this_year = False

        for month in range(12, 0, -1):
            if get_current_storage_gb() >= TARGET_SIZE_GB:
                break

            ok = process_and_save(year, month)
            if ok:
                made_progress_this_year = True

        years_processed_counter += 1

        if year <= min_year:
            print("Reached minimum year limit.")
            break

        # If every month was skipped because files existed, or every download failed,
        # keep moving backward, but don't loop forever.
        year -= 1

        if years_processed_counter % 3 == 0 and get_current_storage_gb() < TARGET_SIZE_GB:
            print(f"\n--- Processed {years_processed_counter} years. Pausing for 5 minutes to rest the connection... ---")
            time.sleep(300)

        if not made_progress_this_year and year < min_year:
            print("No progress made and minimum year reached.")
            break


if __name__ == "__main__":
    run_pipeline()