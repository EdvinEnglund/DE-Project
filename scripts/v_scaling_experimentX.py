from src.analysis_jobX import spark_pipeline
import time

AVERAGING_FACTOR = 3
MEMORY = ["1g", "2g"]

def run_vertical_test():
    for core in range(1, 3):
        for mem in ["1g", "2g"]:
            print(f"Running analysis with {core} core/worker and {mem}b memory")
            total_time = 0
            for j in range(AVERAGING_FACTOR):
                print(f"Run {j + 1} out of {AVERAGING_FACTOR} for {core} cores, {mem}b")
                t = time.time()
                spark_pipeline(total_cores= core * 3, executor_cores=core, memory=mem)
                t = time.time() - t
                total_time += t
                print(f"Finished job w. {core} core/worker and {mem}b memory in {t:.2f} seconds.")
            print(f"Avg execution time with {core} cores, {mem}b memory: {(total_time/AVERAGING_FACTOR):.2f} seconds")

if __name__ == "__main__":
    run_vertical_test()