from pathlib import Path
import pandas as pd

MIN_MB = 128
MAX_MB = 256

def file_size_mb(path: Path) -> float:
    return path.stat().st_size / (1024 * 1024)

def combine_two_parquet_files(file1: Path, file2: Path, output_path: Path) -> None:
    df1 = pd.read_parquet(file1)
    df2 = pd.read_parquet(file2)

    combined = pd.concat([df1, df2], ignore_index=True)
    combined.to_parquet(output_path, index=False, compression="snappy")

def split_parquet_in_two(file_path: Path, output_dir: Path) -> None:
    df = pd.read_parquet(file_path)

    midpoint = len(df) // 2
    part1 = df.iloc[:midpoint]
    part2 = df.iloc[midpoint:]

    stem = file_path.stem
    out1 = output_dir / f"{stem}_split_0.parquet"
    out2 = output_dir / f"{stem}_split_1.parquet"

    part1.to_parquet(out1, index=False, compression="snappy")
    part2.to_parquet(out2, index=False, compression="snappy")

def rebalance_parquet_files(input_dir: str, output_dir: str) -> None:
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    files = sorted(input_path.glob("*.parquet"))

    remembered_small_file = None

    for file in files:
        size_mb = file_size_mb(file)
        print(f"Checking {file.name} ({size_mb:.1f} MB)")

        if size_mb < MIN_MB:
            if remembered_small_file is None:
                remembered_small_file = file
                print(f"  Remembering small file: {file.name}")
            else:
                out_name = (
                    f"{remembered_small_file.stem}__{file.stem}_combined.parquet"
                )
                out_file = output_path / out_name

                print(f"  Combining {remembered_small_file.name} + {file.name}")
                combine_two_parquet_files(remembered_small_file, file, out_file)

                new_size = file_size_mb(out_file)
                print(f"  Wrote {out_file.name} ({new_size:.1f} MB)")

                remembered_small_file = None

        elif size_mb > MAX_MB:
            print(f"  Splitting large file: {file.name}")
            split_parquet_in_two(file, output_path)

        else:
            print(f"  Keeping as-is: {file.name}")
            out_file = output_path / file.name

            # Copy by re-writing parquet (safe and simple)
            df = pd.read_parquet(file)
            df.to_parquet(out_file, index=False, compression="snappy")

    if remembered_small_file is not None:
        print(
            f"Leftover small file not combined: {remembered_small_file.name} "
            f"({file_size_mb(remembered_small_file):.1f} MB)"
        )