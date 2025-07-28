import pandas as pd
import time
import threading
from multiprocessing import Process, Manager
import platform
import psutil

# load the csv
def load_data(filename):
    df = pd.read_csv(filename)
    return df['trip_duration'].tolist()

# Splitting the data into percentage
def split_data(data, percentage):
    length = int(len(data) * percentage)
    return data[:length]

# sequential sorting process method
def sort_sequential(data):
    start = time.time()
    sorted_data = sorted(data)
    end = time.time()
    return end - start

# thread sorting method
def sort_threaded(data):
    result = {}

    def sort():
        result['sorted'] = sorted(data)

    start = time.time()
    thread = threading.Thread(target=sort)
    thread.start()
    thread.join()
    end = time.time()
    return end - start

# multiprocess sorting method

def sort_worker(data, shared_result):
    shared_result['sorted'] = sorted(data)

def sort_multiprocessing(data):
    manager = Manager()
    result = manager.dict()

    start = time.time()
    p = Process(target=sort_worker, args=(data, result))
    p.start()
    p.join()
    end = time.time()

    return end - start

# running all the sorting process
def run_tests(data, label):
    print(f"\n--- Sorting {label} ---")

    t_seq = sort_sequential(data)
    print(f"Sequential:     {t_seq:.4f} seconds")

    t_thread = sort_threaded(data)
    print(f"Threaded:       {t_thread:.4f} seconds")

    t_mp = sort_multiprocessing(data)
    print(f"Multiprocessing:{t_mp:.4f} seconds")

    return [label, t_seq, t_thread, t_mp]

# main
if __name__ == "__main__":
    data = load_data("train.csv")

    sizes = [0.25, 0.50, 0.75, 1.0]
    results = []

    for size in sizes:
        label = f"{int(size * 100)}%"
        data_split = split_data(data, size)
        result = run_tests(data_split, label)
        results.append(result)

    # Print comparison table
    print("\n=== Comparison Table ===")
    print(f"{'Size':<8}{'Sequential':<15}{'Threaded':<15}{'Multiprocessing'}")
    for row in results:
        print(f"{row[0]:<8}{row[1]:<15.4f}{row[2]:<15.4f}{row[3]:.4f}")

    # Print system info to get the Ram and Processor of the computer
    print("\n--- System Info ---")
    print("Processor:", platform.processor())
    print("RAM:", round(psutil.virtual_memory().total / (1024 ** 3), 2), "GB")
