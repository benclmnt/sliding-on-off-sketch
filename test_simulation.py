from data_processing import (
    load_dataset,
    new_LookupTable,
    get_real_persistency,
    run_simulation,
    getBenchmark,
    idx_t_query,
    idx_t_end,
)

import pandas as pd

# from sklearn.metrics import mean_absolute_error
# from sklearn.metrics import confusion_matrix

# set filename and file directory
raw_file = "sample20"
file_dir = "./"

"""
    set basic paramters

    d: number of hash functions
    L: length of StateCounter array for PE

    win_size: time window size
    win_SL: in sliding window, the date is maintained by win_SL latest window
    t_min: earliest time in dataset
    t_max: last time in dataset
    t_start: starting point of test (greater than 1217567877)
    t_query: duration of whole time for recording persistence (less than 1218036494)

    N: sliding window width
    w: length of key-value pair array for FPI
    threshold: a array of thresholds to be query to FPI (from 5 to 105, delta = 10)

    useSL: enable sliding window option
"""
#  para_d  para_L  para_win_size  para_win_SL para_N  para_w  para_threshold  para_useSL  t_start t_query t_end
# t_end will be set programmatically.
params = [
    # [5, 10, 10000, 3, 30000, 10, 1, False, 1217567877, [1217618799, 1217623216], 0],
    # [5, 20, 10000, 2, 30000, 10, 1, False, 1217567877,[1217618799, 1217623216], 0],
    [5, 20, 1000, 2, 3000, 10, 1, True, 1217567877, [1217618799, 1217623216], 0],
]

# table to gather test results: AAE, FNR, FPR
col_names = ["testset", "queryT", "aae", "fnr", "fpr"]
output = pd.DataFrame(columns=col_names)


if __name__ == "__main__":
    # get dataset
    (dataset, t_min, t_max) = load_dataset(file_dir, raw_file)
    for i in range(len(params)):
        params[i][idx_t_end] = t_max

    # run by parameter sets
    for testset in range(len(params)):
        print()
        print("=================================")
        print("test run No.", testset)
        LookupT = new_LookupTable(dataset, len(params[testset][idx_t_query]))
        LookupT = get_real_persistency(dataset, LookupT, params[testset])
        LookupT = run_simulation(dataset, LookupT, params[testset])
        for j in range(len(params[testset][idx_t_query])):
            (aae, fnr, fpr) = getBenchmark(LookupT, j)
            output = output.append(
                {
                    "testset": testset,
                    "queryT": params[testset][idx_t_query][j],
                    "aae": aae,
                    "fnr": fnr,
                    "fpr": fpr,
                },
                ignore_index=True,
            )
    print(output)
    output.to_csv("benchmark.txt", index=False, sep=" ")
    LookupT.to_csv("LookupT.txt", index=False, sep=" ")
    print("Done")
