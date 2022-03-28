#!/usr/bin/env python3
"""
Generate query workloads according to the following scheme:
Base queries arrive according to a poisson process.
After each base query returns, a number of followup queries are submitted.

Note that the output of this script only specifies the base query, the number of followup queries, and the followup query granularity.
Actual follow up queries have to be determined by query_workload_driver.c at runtime based on observed results from the base query.
"""

# Base query rate in mean queries per second
BASE_QUERY_RATES = [
    1.0/5.0,
    2.0/5.0,
    3.0/5.0,
    4.0/5.0,
    5.0/5.0,
]
OUT_FILES = [
    "data_workload/query_workload_1_5.json",
    "data_workload/query_workload_2_5.json",
    "data_workload/query_workload_3_5.json",
    "data_workload/query_workload_4_5.json",
    "data_workload/query_workload_5_5.json",
]

# Workload duration in seconds
DURATION = 180


import numpy as np
import random
import json

KEY_FIELDS = [
    "DstIPv4",
    "SrcIPv4",
]

# Range for number of epochs
NUM_EPOCHS = [1, 5]

# Initial granularity (in bits)
INIT_GRAN = [10, 12]

INIT_SIGMA = 100.0

# Range for granularity of follow ups (in bits)
FOLLOWUP_GRAN = [8, 10]

FOLLOWUP_SIGMA = 50.0

# Range for number of followup queries to submit
NUM_FOLLOWUP_QUERIES = [0, 3]

counter_id = 0
def get_next_query(rate, max_time):
    """
    Returns a tuple (delta_time, query)
    where delta_time is the time (in seconds) before the next query and query is the stql query string
    """
    global counter_id
    counter_id += 1

    while True:
        delta_time = np.random.exponential(scale = 1/rate)
        if delta_time < max_time:
            break

    key_field = random.choice(KEY_FIELDS)
    followup_gran = random.randint(FOLLOWUP_GRAN[0], FOLLOWUP_GRAN[1])
    num_followups = random.randint(NUM_FOLLOWUP_QUERIES[0], NUM_FOLLOWUP_QUERIES[1])
    timeout = random.randint(NUM_EPOCHS[0], NUM_EPOCHS[1])
    gran = random.randint(INIT_GRAN[0], INIT_GRAN[1])
    

    return {
        "dt" : delta_time,
        "key_field" : key_field,
        "init_gran" : gran,
        "followup_gran" : followup_gran,
        "num_followups" : num_followups,
        "sigma" : INIT_SIGMA,
        "followup_sigma" : FOLLOWUP_SIGMA,
        "num_epochs" : timeout
    }

def get_query_workload(duration_sec, rate):
    """
    Returns an array of query tuples (see get_next_query()) for the given duration and rate
    """
    cur_time = 0.0
    workload = []
    while cur_time < duration_sec:
        q = get_next_query(rate, duration_sec)
        cur_time += q["dt"]
        workload.append(q)
    return workload


def main():
    for BASE_QUERY_RATE, OUT_FILE in zip(BASE_QUERY_RATES, OUT_FILES):
        print(f"Running for rate: {BASE_QUERY_RATE} into {OUT_FILE}")
        w = get_query_workload(DURATION, BASE_QUERY_RATE)
        
        with open(OUT_FILE, "w") as f:
            json.dump({"queries" : w}, f)

    print("Done.")

if __name__ == "__main__":
    main()
