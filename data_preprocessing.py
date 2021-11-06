# import unittest
from On_Off_Sketch import StateCounter, SlidingStateCounter, ON, OFF, PE, FPI

import numpy as np
import pandas as pd
import os
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import confusion_matrix

# setup parameters
raw_file="sample"
file_dir = '/Users/jiamengma/Documents/GitHub/nus-cs5234-miniproject/'

SL_en = 1
win_size   = 50000 # window size
t_start_set = 1217567877
N = 3 # number of latest period to record 
T = 9 
threshold = 3

# get raw data and save to dataframe
df_raw = pd.read_csv(file_dir+raw_file+'.txt', sep=" ", header=None, names=["UsrA", "UsrB", "Time"])
df_tmp = df_raw.drop(columns=["UsrB"])
# print(df_tmp.head())
# print(df_tmp.dtypes)
# print("data structure shape is", df_tmp.shape)
df= df_tmp.sort_values(by='Time')
print(df.head())

# create lookup table for unique UsrID to get persistency
col_names =  ["UsrA", "real_Persistency", "est_Persistency", "real_FPI", "est_FPI"]
lookupT  = pd.DataFrame(columns = col_names)
lookupT["UsrA"] = df.UsrA.unique()
lookupT = lookupT.fillna(0)
# print(lookupT.head())


"""
    LOOKUP TABLE update with -- REAL PERSISTENCY
    1. select time window
    2. get distinct item
    3. increment lookup table by one for real_Persistency
    NOTE: below for a single query time, could call it multiple time for an array of query times
"""
#  select window according to Slinding Window: SL_en
if SL_en == 1:
    win_count  = N
    t_start = t_start+win_size*(T-N)
else:
    win_count  = T 
# get real persistency 
t_start = t_start_set
for i in range(win_count):
    t_end = t_start+win_size
    if(t_end>t_query):
        t_end = t_query
    subset_a = df[(df["Time"]>t_start)&(df["Time"]>t_end)].UsrA.unique()
    t_start = t_start + win_size
    for j in range(len(subset_a)):
        index = lookupT[(lookupT["UsrA"]==subset_a[j])].index
        tmp = lookupT.loc[index,"real_Persistency"]
        lookupT.loc[index,"real_Persistency"]=tmp+1
# at query time, set real_FPI = 1 if real_persistency >threshol

"""
    PE & FPI operation in on-off sketch
    1. along df.Time, within a time window, do insertion
    2. when cross time window, reset_state()
    3. meet query time: query for all elements and save to lookupT.loc[index,"est_Persistency"] column
    
    NOTE: could query an array of query times
        at each query, get real persistency, report AAE at T
"""
t_start = t_start_set
max_time = min(df['Time'].max(), t_start+win_size*T)
j = t_start
t_end = t_start+win_size
for i in range(df.shape[0]):
    if j > max_time: break
    subset_c = df[(df["Time"]==j)].UsrA()
    for m in range(len(subset_c)):
        pe.insert(subset_c[m])
        fpi.insert(subset_c[m])
    if j == t_end:
        pe.new_window()
        fpi.new_window()
        t_end = t_end + win_size
    if j == t_query:
        for k in range(lookupT.shape[0]):
            lookupT.loc[k,"est_Persistency"]=pe.query(lookupT.UsrA[k])
        FPI_list = fpi.query(threshold)
        for k in range(length(FPI_list)):
            index = lookupT[(lookupT["UsrA"]==FPI_list[k])].index
            lookupT.loc[index,"est_FPI"]=1
    j=j+1


"""
    LOOKUP TABLE update with -- ESTIMATED PERSISTENCY
    1. get query all
    3. increment lookup table by one for est_Persistency
"""
subset_b = df.UsrA.unique()
for j in range(len(subset_b)):
    index = lookupT[(lookupT["UsrA"]==subset_b[j])].index
    lookupT.loc[index,"est_Persistency"]=pe.query(subset_b[j]) #test


""" 
    PE benchmark- AAE
"""
pe_real = lookupT.real_Persistency
pe_est  = lookupT.est_Persistency
AAE = mean_absolute_error(pe_real, pe_est)


""" 
    FPI benchmark - FNR, FPR
    1. based on threshold: update 
"""
fpi_real = lookupT.real_FPI
fpi_est  = lookupT.est_FPI

tn, fp, fn, tp = confusion_matrix(fpi_real, fpi_est, labels=[0, 1]).ravel()
print("=================================")
print("Average Absolute Error = ", AAE)
print("False Nagative Rate = ", fn)
print("False Positive Rate = ", fp)
print("=================================")



# reset persistency lookup table
