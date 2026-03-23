import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load data
df = pd.read_csv("./vertical_scaling_results.csv")

fig, ax = plt.subplots(figsize=(8, 5))


# Plot median line
medians = df.groupby("total_cores")["time_seconds"].median()
ax.plot(medians.index, medians.values, "-o", color="#C44E52", 
        linewidth=2, markersize=8, label="Median", zorder=4)

# Ideal linear speedup from 1-worker median

t1 = medians.iloc[0]
ideal_cores = np.array([1, 2])
ideal_times = t1 / ideal_cores
ax.plot(ideal_cores, ideal_times, ":^", color="#55A868", 
        linewidth=1.5, markersize=7, label="Ideal linear speedup", zorder=2)

ax.set_xlabel("Number of Workers", fontsize=12)
ax.set_ylabel("Time (seconds)", fontsize=12)
ax.set_title("Spark Vertical Scaling – NYC Taxi Data", fontsize=14)
ax.set_xticks([1, 2])
ax.legend(fontsize=10)
ax.grid(True, alpha=0.3)
fig.tight_layout()
plt.savefig("vertical_plot.png", dpi=150)
print("Saved vertical_plot.png")

plt.show()
