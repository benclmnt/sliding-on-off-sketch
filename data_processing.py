import pandas as pd
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import confusion_matrix
from On_Off_Sketch import PE, FPI

(
    idx_d,
    idx_L,
    idx_win_size,
    idx_N,
    idx_w,
    idx_threshold,
    idx_useSL,
    idx_t_start,
    idx_t_query,
    idx_t_end,
) = range(10)

# save raw data to dataframe; sort the data by timespan
def load_dataset(file_dir, raw_file):
    df_raw = pd.read_csv(
        file_dir + raw_file + ".txt",
        sep=" ",
        header=None,
        names=["UsrA", "UsrB", "Time"],
    )
    df_tmp = df_raw.drop(columns=["UsrB"])
    df = df_tmp.sort_values(by="Time")
    # print(df_tmp.head())
    print(df_tmp.dtypes)
    # print("data structure shape is", df_tmp.shape)
    # print(df.head())
    t_min = df["Time"].min()
    t_max = df["Time"].max()
    print(t_min)
    print(t_max)
    return df, t_min, t_max
    # TBD: could further simplify time precision from sec to day


# create lookup table with unique UsrID with persistency being '0'
def new_LookupTable(df):
    col_names = ["UsrA", "real_Persistency", "est_Persistency", "real_FPI", "est_FPI"]
    lookupT = pd.DataFrame(columns=col_names)
    lookupT["UsrA"] = df.UsrA.unique()
    lookupT = lookupT.fillna(0)
    print(lookupT.head())
    return lookupT


# PE benchmark- AAE
def get_avg_abs_err(lookupT):
    pe_real = lookupT.real_Persistency
    pe_est = lookupT.est_Persistency
    avg_abs_err = mean_absolute_error(pe_real, pe_est)
    print("=================================")
    print("Average Absolute Error = ", avg_abs_err)
    print("=================================")
    return avg_abs_err


# FPI benchmark - FNR, FPR
def get_fnr_fpr(lookupT):
    fpi_real = lookupT.real_FPI
    fpi_est = lookupT.est_FPI
    tn, fp, fn, tp = confusion_matrix(fpi_real, fpi_est, labels=[0, 1]).ravel()
    print("=================================")
    print("False Nagative Rate = ", fn)
    print("False Positive Rate = ", fp)
    print("=================================")
    return fn, fp


def getBenchmark(lookupT):
    aae = get_avg_abs_err(lookupT)
    (fnr, fpr) = get_fnr_fpr(lookupT)
    return aae, fnr, fpr


def get_real_persistency(df, lookupT, params):
    """
    REAL PERSISTENCY
        1. select time window
        2. get distinct item
        3. increment lookup table by one for real_Persistency
        NOTE: below for a single query time, could call it multiple time for an array of query times
    """
    #  select the duration of time to record persistence based on Sliding Window: useSL
    if params[idx_useSL]:
        win_count = int(
            (params[idx_t_query] - params[idx_t_start]) / params[idx_win_size] + 1
        )
        startT = int(
            (params[idx_t_query] - params[idx_t_start]) / params[idx_win_size] + 1
        )
    else:
        win_count = int(
            (params[idx_t_query] - params[idx_t_start]) / params[idx_win_size] + 1
        )
        startT = params[idx_t_start]

    # real_Persistency
    for i in range(win_count):
        endT = startT + params[idx_win_size]
        if endT > params[idx_t_query]:
            endT = params[idx_t_query]
        subset_a = df[(df["Time"] >= startT) & (df["Time"] <= endT)].UsrA.unique()
        for j in range(len(subset_a)):
            index = lookupT[(lookupT["UsrA"] == subset_a[j])].index
            tmp = lookupT.loc[index, "real_Persistency"]
            lookupT.loc[index, "real_Persistency"] = tmp + 1
        startT = endT

    # real_FPI
    for index in range(lookupT.shape[0]):
        tmp = lookupT.loc[index, "real_Persistency"]
        if tmp >= params[idx_threshold]:
            lookupT.loc[index, "real_FPI"] = 1
    return lookupT


def run_simulation(df, lookupT, params):
    """
    PE & FPI operation in on-off sketch
        1. along df.Time, within a time window, do insertion
        2. when cross time window, reset_state()
        3. meet query time: query for all elements and save to lookupT.loc[index,"est_Persistency"] column

        NOTE: could query an array of query times
            at each query, get real persistency, report AAE at T
    """
    pe = PE(
        params[idx_d],
        params[idx_L],
        h=[lambda x: (x + i) % params[idx_L] for i in range(params[idx_d])],
    )
    fpi = FPI(params[idx_L], params[idx_w], lambda x: x % params[idx_L])

    # max_time = min(df['Time'].max(), idx_t_start+win_size*T)

    j = params[idx_t_start]
    win_end = params[idx_t_start] + params[idx_win_size]

    # for i in range(df.shape[0]):
    for i in range(params[idx_t_query] + 1):
        if j > params[idx_t_end]:
            break
        subset_c = df[(df["Time"] == j)].UsrA.to_numpy()
        for m in range(len(subset_c)):
            pe.insert(subset_c[m])
            fpi.insert(subset_c[m])
        if j == win_end:
            pe.new_window()
            fpi.new_window()
            win_end = win_end + params[idx_win_size]
        if j == params[idx_t_query]:
            for k in range(lookupT.shape[0]):
                lookupT.loc[k, "est_Persistency"] = pe.query(lookupT.UsrA[k])
            FPI_list = fpi.find_persistent_above(params[idx_threshold])
            for k in range(len(FPI_list)):
                index = lookupT[(lookupT["UsrA"] == FPI_list[k])].index
                lookupT.loc[index, "est_FPI"] = 1
        j = j + 1
