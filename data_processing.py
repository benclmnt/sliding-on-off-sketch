import pandas as pd
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import confusion_matrix
from On_Off_Sketch import PE, FPI, get_hash_fns

BY_DATE = 1

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


(
    real_Persistency,
    est_Persistency,
    real_FPI,
    est_FPI,
) = range(4)

# save raw data to dataframe; sort the data by timespan
def load_dataset(file_dir, raw_file):
    df_raw = pd.read_csv(
        file_dir + raw_file + ".txt",
        sep=" ",
        header=None,
        names=["UsrA", "UsrB", "Time"],
    )
    df_tmp = df_raw.drop(columns=["UsrB"])
    # 2008-01-01 00:00:00 = 1199116800
    # one day = 24*60*60 secs = 86400 secs
    if BY_DATE == 1:
        df_tmp = df_tmp.apply(lambda x: int((x-1199116800)/86400) if x.name == 'UsrB' else x)
    df = df_tmp.sort_values(by="Time")
    # print(df_tmp.head())
    # print(df_tmp.dtypes)
    # print("data structure shape is", df_tmp.shape)
    # print(df.head())
    t_min = df["Time"].min()
    t_max = df["Time"].max()
    # print(t_min)
    # print(t_max)
    return df, t_min, t_max
    # TBD: could further simplify time precision from sec to day


# create lookup table with unique UsrID with persistency being '0'
def new_LookupTable(df, query_num):
    col_names = ["UsrA"]
    for i in range(query_num):
        col_names.append("real_Persistency_"+str(i))
        col_names.append("est_Persistency_"+str(i))
        col_names.append("real_FPI"+str(i))
        col_names.append("est_FPI"+str(i))
    lookupT = pd.DataFrame(columns=col_names)
    lookupT["UsrA"] = df.UsrA.unique()
    lookupT = lookupT.fillna(0)
    # print(lookupT.head())
    return lookupT


# PE benchmark- AAE
def get_avg_abs_err(lookupT, query_i):
    pe_real = lookupT.iloc[:, 4*query_i+real_Persistency]
    pe_est = lookupT.iloc[:, 4*query_i+est_Persistency]
    avg_abs_err = mean_absolute_error(pe_real, pe_est)
    # print("=================================")
    print("Average Absolute Error (AAE) = ", avg_abs_err)
    # print("=================================")
    return avg_abs_err


# FPI benchmark - FNR, FPR
def get_fnr_fpr(lookupT, query_i):
    fpi_real = lookupT.iloc[:, 4*query_i+real_FPI]
    fpi_est = lookupT.iloc[:, 4*query_i+est_FPI]
    tn, fp, fn, tp = confusion_matrix(fpi_real, fpi_est, labels=[0, 1]).ravel()
    # print("=================================")
    print("False Nagative Rate (FNR) = ", fn)
    print("False Positive Rate (FPR) = ", fp)
    # print("=================================")
    return fn, fp


def getBenchmark(lookupT, query_i):
    aae = get_avg_abs_err(lookupT, query_i)
    (fnr, fpr) = get_fnr_fpr(lookupT, query_i)
    return aae, fnr, fpr


def get_real_persistency(df, lookupT, params):
    """
    REAL PERSISTENCY
        1. select time window
        2. get distinct item
        3. increment lookup table by one for real_Persistency
        NOTE: below for a single query time, could call it multiple time for an array of query times
    """

    for j in range(len(params[idx_t_query])):
        #  select the duration of time to record persistence based on Sliding Window: useSL
        if params[idx_useSL]:
            win_count = int(
                (params[idx_t_query][j] - params[idx_t_start]) / params[idx_win_size] + 1
            )
            startT = int(
                (params[idx_t_query][j] - params[idx_t_start]) / params[idx_win_size] + 1
            )
        else:
            win_count = int(
                (params[idx_t_query][j] - params[idx_t_start]) / params[idx_win_size] + 1
            )
            startT = params[idx_t_start]

        # real_Persistency
        for _ in range(win_count):
            endT = min(startT + params[idx_win_size], params[idx_t_query][j])
            unique_user_in_window = df[
                (df["Time"] >= startT) & (df["Time"] <= endT)
            ].UsrA.unique()
            for user in range(len(unique_user_in_window)):
                index = lookupT[(lookupT["UsrA"] == unique_user_in_window[user])].index
                lookupT.iloc[index, 4*j+real_Persistency] += 1

            # go to next time window
            startT = endT

        # print(lookupT)

        # real_FPI
        for index in range(lookupT.shape[0]):
            tmp = lookupT.iloc[index, 4*j+real_Persistency]
            if tmp >= params[idx_threshold]:
                lookupT.iloc[index, 4*j+real_FPI] = 1

        # print(lookupT)

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
    d, L, w = params[idx_d], params[idx_L], params[idx_w]
    pe = PE(d, L)
    fpi = FPI(L, w)
    query_list = params[idx_t_query]

    # max_time = min(df['Time'].max(), idx_t_start+win_size*T)

    start, end, win_size = params[idx_t_start], query_list[-1], params[idx_win_size]
    win_start, win_end = start, start + win_size

    # for i in range(df.shape[0]):
    for _ in range(query_list[-1] + 1):
        if win_start > end:
            break
        subset_c = df[(df["Time"] == win_start)].UsrA.to_numpy()
        for m in range(len(subset_c)):
            pe.insert(subset_c[m])
            fpi.insert(subset_c[m])
        if win_start in query_list:
            index = query_list.index(win_start)
            for k in range(lookupT.shape[0]):
                lookupT.iloc[k, 4*index+est_Persistency] = pe.query(lookupT.UsrA[k])
            FPI_list = fpi.find_persistent_above(params[idx_threshold])
            for k in range(len(FPI_list)):
                index = lookupT[(lookupT["UsrA"] == FPI_list[k])].index
                lookupT.iloc[index, 4*index+est_FPI] = 1
        if win_start == win_end:
            pe.new_window()
            fpi.new_window()
            win_end = win_end + win_size
        win_start = win_start + 1

    # print(lookupT)

    return lookupT
