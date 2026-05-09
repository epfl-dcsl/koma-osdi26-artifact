import os
import numpy as np
import matplotlib.pyplot as plt

# Define file names and transaction types (0 to 4)
files = [f"worker_{i}_latencies.txt" for i in range(48, 72)]
transaction_types = [0, 1, 2, 3, 4]
txn_mapping = {
    0: "NewOrder",
    1: "Payment",
    2: "Delivery",
    3: "OrderStatus",
    4: "StockLevel"
}
# Define the bins for the specified latency ranges
bins = [0, 20, 100, 200, 500, 1000, 2000, 5000]
bin_labels = ["0-20us", "20-100us", "100-200us", "200-500us", "500-1000us", "1000-2000us", "2000-5000us"]

# Initialize lists to store latencies for each transaction type
latencies = {t: [] for t in transaction_types}

# Read and parse the data from the files
for file in files:
    with open(file, 'r') as f:
        for line in f:
            latency, txn_type = map(int, line.split())
            if txn_type in latencies:
                latencies[txn_type].append(latency)

# Function to compute CDF
def compute_cdf(data):
    data = np.sort(data)
    yvals = np.arange(len(data)) / float(len(data))
    return data, yvals

# Function to compute CCDF
def compute_ccdf(data):
    data = np.sort(data)
    yvals = 1 - np.arange(len(data)) / float(len(data))
    return data, yvals

# Function to count data points in each latency bin
def count_in_bins(data, bins):
    counts, _ = np.histogram(data, bins=bins)
    return counts

# Plot the CDF for each transaction type and overall
plt.figure(figsize=(10, 6))

# Plot CDF for each transaction type
for txn_type in transaction_types:
    data, cdf = compute_ccdf(latencies[txn_type])
    plt.plot(data, cdf, label=f'Txn Type {txn_mapping[txn_type]}')

    # Count the number of data points in each bin for this transaction type
    counts = count_in_bins(latencies[txn_type], bins)
    print(f"Txn Type {txn_type} counts in bins:")
    for i, count in enumerate(counts):
        print(f"  {bin_labels[i]}: {count} data points")

# Plot overall CDF (all transaction types combined)
all_latencies = [latency for latencies_list in latencies.values() for latency in latencies_list]
data, cdf = compute_ccdf(all_latencies)
plt.plot(data, cdf, label='Overall', linestyle='--', color='black')

# Count the number of data points in each bin for overall latencies
overall_counts = count_in_bins(all_latencies, bins)
print(f"Overall counts in bins:")
for i, count in enumerate(overall_counts):
    print(f"  {bin_labels[i]}: {count} data points")


# Add labels and legend
plt.xlim(0, 500)
plt.yscale('log')

plt.title('CDF of Latencies by Transaction Type')
plt.xlabel('Latency (us)')
plt.ylabel('CDF (Log Scale)')
plt.legend()

# Show the plot
plt.grid(True)
plt.savefig('worker_txn_time.pdf', format='pdf')
