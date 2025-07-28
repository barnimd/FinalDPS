import pandas as pd
import time
import threading
from multiprocessing import Process, Manager
import platform
import psutil

# ----------- LOAD DATA -------------
def load_data(filename):
    df = pd.read_csv(filename)
    return df['trip_duration'].tolist()

# ----------- SPLIT DATA -------------
def split_data(data, percentage):
    length = int(len(data) * percentage)
    return data[:length]

# ----------- SEQUENTIAL FILTERING -------------
def filter_sequential(data):
    start = time.time()
    filtered_data = [x for x in data if x < 2000 and x % 5 == 0]
    end = time.time()
    return end - start

# ----------- THREADED FILTERING -------------
def filter_threaded(data):
    result = {}

    def filter_func():
        result['filtered'] = [x for x in data if x < 2000 and x % 5 == 0]

    start = time.time()
    thread = threading.Thread(target=filter_func)
    thread.start()
    thread.join()
    end = time.time()
    return end - start

# ----------- MULTIPROCESSING FILTERING -------------
def filter_worker(data, shared_result):
    shared_result['filtered'] = [x for x in data if x < 2000 and x % 5 == 0]

def filter_multiprocessing(data):
    manager = Manager()
    result = manager.dict()

    start = time.time()
    p = Process(target=filter_worker, args=(data, result))
    p.start()
    p.join()
    end = time.time()
    return end - start

# ----------- RUN ALL FILTER TESTS -------------
def run_filter_tests(data, label):
    print(f"\n--- Filtering {label} ---")

    t_seq = filter_sequential(data)
    print(f"Sequential:     {t_seq:.4f} seconds")

    t_thread = filter_threaded(data)
    print(f"Threaded:       {t_thread:.4f} seconds")

    t_mp = filter_multiprocessing(data)
    print(f"Multiprocessing:{t_mp:.4f} seconds")

    return [label, t_seq, t_thread, t_mp]

# ----------- MAIN PROGRAM -------------
if __name__ == "__main__":
    # Load data (change the file name if needed)
    data = load_data("train.csv")

    # Split sizes
    sizes = [0.25, 0.50, 0.75, 1.0]
    filter_results = []

    for size in sizes:
        label = f"{int(size * 100)}%"
        data_split = split_data(data, size)
        result = run_filter_tests(data_split, label)
        filter_results.append(result)

    # Print final table
    print("\n=== Filtering Comparison Table ===")
    print(f"{'Size':<8}{'Sequential':<15}{'Threaded':<15}{'Multiprocessing'}")
    for row in filter_results:
        print(f"{row[0]:<8}{row[1]:<15.4f}{row[2]:<15.4f}{row[3]:.4f}")

    # Print system info
    print("\n--- System Info ---")
    print("Processor:", platform.processor())
    print("RAM:", round(psutil.virtual_memory().total / (1024 ** 3), 2), "GB")
