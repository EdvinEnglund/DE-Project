from __future__ import annotations

from pathlib import Path
import subprocess
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

MIN_MB = 128
MAX_MB = 256

HDFS_BIN = str(Path.home() / "hadoop-3.4.1" / "bin" / "hdfs")
HDFS_INPUT_DIR = "/user/ubuntu/nyc_tlc/final_downloads"
HDFS_OUTPUT_DIR = "/user/ubuntu/nyc_tlc/final_downloads_rebalanced"

STAGE_DIR = Path("/tmp/hdfs_stage_rebalance")
LOCAL_IN = STAGE_DIR / "in"
LOCAL_OUT = STAGE_DIR / "out"


def run_hdfs(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([HDFS_BIN, *args], text=True, capture_output=True)


def ensure_hdfs_dir(path: str) -> None:
    result = run_hdfs("dfs", "-mkdir", "-p", path)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"Could not create {path}")


def list_hdfs_parquet_files(path: str) -> list[str]:
    result = run_hdfs("dfs", "-ls", path)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"Could not list {path}")

    files = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if parts and parts[-1].endswith(".parquet"):
            files.append(parts[-1])
    return sorted(files)


def hdfs_get(hdfs_path: str, local_path: Path) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    result = run_hdfs("dfs", "-get", "-f", hdfs_path, str(local_path))
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"Could not get {hdfs_path}")


def hdfs_put(local_path: Path, hdfs_path: str) -> None:
    result = run_hdfs("dfs", "-put", "-f", str(local_path), hdfs_path)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"Could not put {local_path} -> {hdfs_path}")


def hdfs_rm(hdfs_path: str) -> None:
    result = run_hdfs("dfs", "-rm", "-f", hdfs_path)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"Could not remove {hdfs_path}")


def file_size_mb(path: Path) -> float:
    return path.stat().st_size / (1024 * 1024)


def combine_parquet(file1: Path, file2: Path, output_path: Path) -> Path:
    table1 = pq.read_table(file1)
    table2 = pq.read_table(file2)
    combined = pa.concat_tables([table1, table2])
    pq.write_table(combined, output_path, compression="snappy")
    return output_path


def split_parquet_in_two(file_path: Path, output_dir: Path) -> list[Path]:
    table = pq.read_table(file_path)
    midpoint = table.num_rows // 2

    part1 = output_dir / f"{file_path.stem}_part_0.parquet"
    part2 = output_dir / f"{file_path.stem}_part_1.parquet"

    pq.write_table(table.slice(0, midpoint), part1, compression="snappy")
    pq.write_table(table.slice(midpoint), part2, compression="snappy")

    return [part1, part2]


def cleanup(*paths: Path | None) -> None:
    for path in paths:
        if path is not None:
            path.unlink(missing_ok=True)


def rebalance_hdfs() -> None:
    ensure_hdfs_dir(HDFS_OUTPUT_DIR)
    shutil.rmtree(STAGE_DIR, ignore_errors=True)
    LOCAL_IN.mkdir(parents=True, exist_ok=True)
    LOCAL_OUT.mkdir(parents=True, exist_ok=True)

    hdfs_files = list_hdfs_parquet_files(HDFS_INPUT_DIR)
    if not hdfs_files:
        print(f"Inga .parquet-filer hittades i {HDFS_INPUT_DIR}")
        return

    output_index = 0
    pending_small: tuple[Path, str] | None = None

    def next_output_path() -> str:
        nonlocal output_index
        path = f"{HDFS_OUTPUT_DIR}/part_{output_index:04d}.parquet"
        output_index += 1
        return path

    for hdfs_path in hdfs_files:
        local_file = LOCAL_IN / Path(hdfs_path).name

        print(f"\nLaddar ner: {hdfs_path}")
        hdfs_get(hdfs_path, local_file)

        size_mb = file_size_mb(local_file)
        print(f"Bearbetar {local_file.name} ({size_mb:.1f} MB)")

        if MIN_MB <= size_mb <= MAX_MB:
            out_path = next_output_path()
            print(f"  OK storlek, laddar upp direkt -> {out_path}")
            hdfs_put(local_file, out_path)
            hdfs_rm(hdfs_path)
            cleanup(local_file)
            continue

        if size_mb > MAX_MB:
            print("  För stor, splittar i två delar")
            parts = split_parquet_in_two(local_file, LOCAL_OUT)

            for part in parts:
                hdfs_put(part, next_output_path())

            hdfs_rm(hdfs_path)
            cleanup(local_file, *parts)
            continue

        # size_mb < MIN_MB
        if pending_small is None:
            print("  För liten, sparar för nästa kombination")
            pending_small = (local_file, hdfs_path)
            continue

        prev_file, prev_hdfs_path = pending_small
        combined_file = LOCAL_OUT / f"{prev_file.stem}__{local_file.stem}.parquet"

        print(f"  Kombinerar {prev_file.name} + {local_file.name}")
        combine_parquet(prev_file, local_file, combined_file)

        combined_size = file_size_mb(combined_file)
        print(f"  Kombinerad storlek: {combined_size:.1f} MB")

        if combined_size <= MAX_MB:
            hdfs_put(combined_file, next_output_path())
        else:
            print("  Kombinerad fil blev för stor, splittar i två delar")
            parts = split_parquet_in_two(combined_file, LOCAL_OUT)
            for part in parts:
                hdfs_put(part, next_output_path())
            cleanup(*parts)

        hdfs_rm(prev_hdfs_path)
        hdfs_rm(hdfs_path)

        cleanup(prev_file, local_file, combined_file)
        pending_small = None

    if pending_small is not None:
        leftover_file, leftover_hdfs_path = pending_small
        out_path = next_output_path()
        print(f"\nKvarvarande liten fil: {leftover_file.name}, laddar upp som den är -> {out_path}")
        hdfs_put(leftover_file, out_path)
        hdfs_rm(leftover_hdfs_path)
        cleanup(leftover_file)

    print(f"\nKlart. Output finns i: {HDFS_OUTPUT_DIR}")


if __name__ == "__main__":
    rebalance_hdfs()