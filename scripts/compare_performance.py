import pandas as pd

# Load benchmark results
baseline_df = pd.read_csv("results/baseline_benchmark_results.csv")
optimized_df = pd.read_csv("results/optimized_benchmark_results.csv")

# Normalize query names for matching
baseline_df["query_key"] = baseline_df["query"].str.replace("_Baseline", "", regex=False)
optimized_df["query_key"] = optimized_df["query"].str.replace("_Optimized", "", regex=False)

# Merge baseline and optimized results
comparison_df = pd.merge(
    baseline_df,
    optimized_df,
    on="query_key",
    suffixes=("_baseline", "_optimized")
)

# Calculate improvement percentage
comparison_df["improvement_percent"] = (
    (comparison_df["average_time_sec_baseline"] - comparison_df["average_time_sec_optimized"])
    / comparison_df["average_time_sec_baseline"]
) * 100

# Select final columns
final_df = comparison_df[
    [
        "query_key",
        "average_time_sec_baseline",
        "average_time_sec_optimized",
        "improvement_percent"
    ]
]

# Save comparison results
final_df.to_csv("results/performance_comparison.csv", index=False)

print("\nPerformance Comparison (Baseline vs Optimized):\n")
print(final_df)

print("\nPerformance comparison saved to results/performance_comparison.csv")
