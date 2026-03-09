from pathlib import Path
import subprocess
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

# -----------------------------
# Config
# -----------------------------
TARGET_SIZE_GB = 30

# Local staging dir (temporary). Keep it small; files are deleted after upload.
DATA_DIR = Path("data/_stage_hdfs_upload")
DATA_DIR.mkdir(parents=True, exist_ok=True)

TEMP_FILE = DATA_DIR / "temp_raw.parquet"

# HDFS
HDFS_BIN = str(Path.home() / "hadoop-3.4.1" / "bin" / "hdfs")
HDFS_DIR = "/user/ubuntu/nyc_tlc/final_downloads"

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
    "total_amount",
]

TARGET_SCHEMA = pa.schema([
    pa.field("tpep_pickup_datetime", pa.timestamp("us")),
    pa.field("tpep_dropoff_datetime", pa.timestamp("us")),
    pa.field("passenger_count", pa.float64()),
    pa.field("trip_distance", pa.float64()),
    pa.field("RatecodeID", pa.float64()),
    pa.field("PULocationID", pa.int32()),
    pa.field("DOLocationID", pa.int32()),
    pa.field("payment_type", pa.int64()),
    pa.field("tip_amount", pa.float64()),
    pa.field("total_amount", pa.float64()),
])

REQUEST_TIMEOUT = (15, 300)  # (connect, read)
REQUEST_HEADERS = {"User-Agent": "Mozilla/5.0"}
CHUNK_DOWNLOAD_BYTES = 1024 * 1024  # 1 MiB


# -----------------------------
# HDFS helpers
# -----------------------------
def run_hdfs(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run([HDFS_BIN] + args, text=True, capture_output=True)


def ensure_hdfs_dir() -> None:
    r = run_hdfs(["dfs", "-mkdir", "-p", HDFS_DIR])
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip() or "Failed to create HDFS dir")


def hdfs_exists(hdfs_path: str) -> bool:
    r = run_hdfs(["dfs", "-test", "-e", hdfs_path])
    return r.returncode == 0


def hdfs_put(local_path: Path, hdfs_dir: str) -> None:
    r = run_hdfs(["dfs", "-put", "-f", str(local_path), hdfs_dir])
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip() or f"Failed to upload {local_path}")


def get_hdfs_storage_gb() -> float:
    """
    Returns logical size of HDFS_DIR in GB using: hdfs dfs -du -s
    Output is typically: <bytes> <bytes_with_replication> <path>
    We'll use the first number (logical bytes).
    """
    ensure_hdfs_dir()
    r = run_hdfs(["dfs", "-du", "-s", HDFS_DIR])
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip() or "Failed to compute HDFS size")

    parts = r.stdout.strip().split()
    if not parts:
        return 0.0

    logical_bytes = int(parts[0])
    return logical_bytes / (1024 ** 3)


# -----------------------------
# Data helpers
# -----------------------------
def month_already_processed(year: int, month: int) -> bool:
    mm = f"{month:02d}"
    hdfs_path = f"{HDFS_DIR}/part_{year}_{mm}.parquet"
    return hdfs_exists(hdfs_path)


def download_to_temp(url: str, temp_path: Path) -> None:
    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT, headers=REQUEST_HEADERS) as response:
        response.raise_for_status()
        with open(temp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=CHUNK_DOWNLOAD_BYTES):
                if chunk:
                    f.write(chunk)


def cast_and_filter_nulls(table: pa.Table) -> pa.Table:
    """
    Cast columns to TARGET_SCHEMA and drop rows where ANY KEEP_COLUMNS is null.
    """
    # Cast
    casted_cols = []
    for field in TARGET_SCHEMA:
        arr = table[field.name]
        casted_cols.append(pc.cast(arr, field.type, safe=False))
    casted = pa.Table.from_arrays(casted_cols, schema=TARGET_SCHEMA)

    # Filter out rows with any nulls
    mask = None
    for name in KEEP_COLUMNS:
        non_null = pc.invert(pc.is_null(casted[name], nan_is_null=True))
        mask = non_null if mask is None else pc.and_(mask, non_null)

    return casted.filter(mask)


# -----------------------------
# Main processing
# -----------------------------
def process_and_save(year: int, month: int) -> bool:
    mm = f"{month:02d}"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{mm}.parquet"

    if month_already_processed(year, month):
        print(f"Skipping {year}-{mm}: output already exists in HDFS.")
        return False

    try:
        ensure_hdfs_dir()

        # 1) Download source parquet locally
        print(f"Downloading {year}-{mm} ...")
        download_to_temp(url, TEMP_FILE)

        # 2) Read only selected columns
        print(f"Loading {year}-{mm} into PyArrow ...")
        table = pq.read_table(TEMP_FILE, columns=KEEP_COLUMNS)

        if table.num_rows == 0:
            print(f"Skipping {year}-{mm}: file loaded but contains no rows.")
            return True

        # 3) Cast + filter null rows
        before = table.num_rows
        table = cast_and_filter_nulls(table)
        dropped = before - table.num_rows
        print(f"Dropped {dropped:,} rows with nulls after casting.")

        if table.num_rows == 0:
            print(f"Skipping {year}-{mm}: no rows left after null filtering.")
            return True

        # 4) Write local output parquet
        out_path = DATA_DIR / f"part_{year}_{mm}.parquet"
        pq.write_table(table, out_path, compression="snappy")
        actual_mb = out_path.stat().st_size / (1024 * 1024)
        print(f"Wrote local {out_path.name} ({actual_mb:.1f} MB)")

        # 5) Upload to HDFS
        hdfs_put(out_path, HDFS_DIR)
        print(f"Uploaded to HDFS: {HDFS_DIR}/{out_path.name}")

        # Optional: remove local file after upload
        out_path.unlink(missing_ok=True)

        return True

    except requests.HTTPError as e:
        # e.g., 403/404 for months that don't exist or are blocked
        print(f"Skipping {year}-{mm}: {e}")
        return False

    except Exception as e:
        print(f"Skipping {year}-{mm}: {e}")
        return False

    finally:
        if TEMP_FILE.exists():
            TEMP_FILE.unlink()


def run_pipeline() -> None:
    year = 2025
    min_year = 2025
    years_processed_counter = 0

    while get_hdfs_storage_gb() < TARGET_SIZE_GB:
        print(f"\nCurrent HDFS size: {get_hdfs_storage_gb():.2f} GB in {HDFS_DIR}")

        made_progress_this_year = False

        for month in range(12, 0, -1):
            if get_hdfs_storage_gb() >= TARGET_SIZE_GB:
                break

            ok = process_and_save(year, month)
            if ok:
                made_progress_this_year = True
        
        if made_progress_this_year:
            years_processed_counter += 1

        if year <= min_year:
            print("Reached minimum year limit.")
            break

        year -= 1

        if years_processed_counter % 3 == 0 and get_hdfs_storage_gb() < TARGET_SIZE_GB:
            print(f"\n--- Processed {years_processed_counter} years. Pausing for 5 minutes... ---")
            time.sleep(300)

        if year < min_year:
            print("Minimum year reached.")
            break


if __name__ == "__main__":
    run_pipeline()