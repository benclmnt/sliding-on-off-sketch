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
    t_min: earliest time in dataset
    t_max: last time in dataset
    t_start: starting point of test (greater than 1217567877)
    t_query: duration of whole time for recording persistence (less than 1218036494)

    N: sliding window width
    w: length of key-value pair array for FPI
    threshold: a array of thresholds to be query to FPI (from 5 to 105, delta = 10)

    useSL: enable sliding window option
"""
#  para_d  para_L  para_win_size  para_N  para_w  para_threshold  para_useSL  t_start t_query t_end
# t_end will be set programmatically.
params = [
    [5, 10, 100000, 300000, 10, 2, True, 1217567877, [1217567877, 1218000000], 0],
    [5, 10, 100000, 300000, 10, 10, True, 1217567877,[1217567877, 1218000000], 0],
]

BY_DATE = 1

if BY_DATE == 1:
    for i in range(len(params)):
        params[i][2]= int(params[i][2]/86400)
        params[i][3]= int(params[i][3]/86400)
        for j in range(len(params[i][8])):
            params[i][8][j] = int((params[i][8][j]-1217567877)/86400)

# table to gather test results: AAE, FNR, FPR
col_names = ["testset", "queryT", "aae", "fnr", "fpr"]
output = pd.DataFrame(columns = col_names)



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
        LookupT = new_LookupTable(dataset, len(params[0][8]))
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
                ignore_index=True
            )
    print(output)
    output.to_csv('benchmark.txt', index=False,  sep=' ')
    print("Done. Saved to benchmark.txt")
