from src.analysis_jobX import spark_pipeline
import time

AVERAGING_FACTOR = 3

def run_horizontal_test():
    for i in range(1, 4):
        print(f"Running analysis with {i} workers, 1 core each...")
        total_time = 0
        for j in range(AVERAGING_FACTOR):
            print(f"Run {j + 1} out of {AVERAGING_FACTOR} for {i} workers...")
            t = time.time()
            spark_pipeline(total_cores=i)
            t = time.time() - t
            total_time += t
            print(f"Finished in {t} seconds.")
        print(f"Avg execution time with {i} workers: {(total_time/AVERAGING_FACTOR):.2f} seconds")

if __name__ == "__main__":
    run_horizontal_test()