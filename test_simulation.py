from data_processing import load_dataset, new_LookupTable, get_real_persistency, run_simulation, getBenchmark

import numpy as np
import pandas as pd
# from sklearn.metrics import mean_absolute_error
# from sklearn.metrics import confusion_matrix

# set filename and file directory
raw_file = 'sample'
file_dir = './'

"""
    set basic paramters

    d: number of hash functions
    L: length of StateCounter array for PE

    w_len: time window size
    t_min: earliest time in dataset
    t_max: last time in dataset
    t_start: dstarting point of test (greater than 1217567877)
    t_query: duration of whole time for recording persistence (less than 1218036494)

    N: sliding window width 
    w: length of key-value pair array for FPI
    threshold: a array of thresholds to be query to FPI (from 5 to 105, delta = 10)

    SL_en: enable sliding window option
"""    
#         para_d    para_L  para_w_len      para_N      para_w  para_threshold  para_SL_en  t_start     t_query     t_end
paras = [[  5,       10,        100000,     300000,     10,          2,               0,   1217567877, 1217800000,  0],
         [  5,       10,        100000,     300000,     10,          10,              0,   1217567877, 1218030000,  0]] 
print(len(paras)) 

d = 0
L = 1
w_len = 2
N = 3
w = 4
threshold = 5
SL_en = 6
t_start = 7
t_query = 8
t_end = 9


# table to gather test results: AAE, FNR, FPR
col_names = ["testset", "queryT", "aae", "fnr", "fpr"]
output = pd.DataFrame(columns = col_names)


if __name__ == "__main__":
    # get dataset
    (dataset, t_min, t_max) = load_dataset(file_dir, raw_file)
    for i in range(len(paras)):
        paras[i][t_end] = t_max

    # run by parameter sets
    for testset in range(len(paras)):
        print("=================================")
        print("test run No.", testset)
        LookupT = new_LookupTable(dataset)
        LookupT = get_real_persistency(dataset, LookupT, paras[testset])
        LookupT = run_simulation(dataset, LookupT, paras[testset])
        (aae, fnr, fpr) = getBenchmark(LookupT)
        output = output.append({"testset": testset, "queryT": paras[testset][8], "aae": aae, "fnr": fnr, "fpr": fpr})
        print("Done")
    print(output)
