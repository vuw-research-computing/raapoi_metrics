#!/home/software/apps/python/3.8.1/bin/python3

import sys

import pandas as pd
import numpy as np
import getpass as gp
import argparse as ap
import datetime as dt

#print(sys.version_info[0])

# if sys.version_info[0] < 3:
#     raise Exception("Python 3 or a more recent version is required.")

parser = ap.ArgumentParser()
parser.add_argument("-d", "--days", help="the number of days for the report output")
parser.add_argument("-f", "--file", help="output report to a CSV file")
args = parser.parse_args()
#print(args.days)
# parser.parse_args()
# parser.add_argument('csv', help="Save output to a CSV file.")
# args = parser.parse_args()
# print(args.csv)

num_days = 100
today = dt.date.today()
start_date = today + dt.timedelta(-num_days)
print('Collecting job efficiency statistics')
print('Report start date: ' + start_date.isoformat())
print('Report end date: ' + today.isoformat())

#pd.set_option('display.max_rows', 200, 'display.max_columns', 40, 'display.width', 200)
today_csv = dt.datetime.now()
#pd.set_option('use_inf_as_na', True)
#username = 'fageal'
username = gp.getuser()

# Import the data into a pandas dataframe - proof of concept - uses CSV.
# Eventually will need to bring a sacct query into a dataframe.
try:
    df = pd.read_csv("/nfs/scratch/admin/metrics_data/all_jobs_new_calc.csv", usecols=['Partition', 'User', 'State', 'JobID', 'Start', 'End', 'cpu_efficency', 'mem_efficiency', 'time_efficiency'], low_memory=False)
except FileNotFoundError as fnf:
    print('Error: Input data not available.')
    print('Please ask for help on the Slack channel at https://uwrc.slack.com/')
    sys.exit(fnf)

# Kludge to remove weird 0 index row - needs fixing in origin script
df = df.drop([0])
# Pull out the data we need - user and date range, exclude PENDING and RUNNING jobs. Replace all CANCELLED% with CANCELLED.
df = df.loc[(df['User'] == username)]
df['State'] = df['State'].str.replace(r"CANCELLED.*$", "CANCELLED", regex=True)
df = df.loc[(-df['State'].isin(['PENDING','RUNNING']))]
df['Start'] = pd.to_datetime(df['Start'])
df = df.loc[(df['Start'].dt.date >= start_date)]

# groupby and aggregate information

gdf = df.groupby(["User", "Partition", "State"], as_index=False, dropna=True).agg(
        **{
            "Num Jobs": pd.NamedAgg(column="JobID", aggfunc="count"),
            "Min % CPU Eff": pd.NamedAgg(column="cpu_efficency", aggfunc=np.min),
            "Max % CPU Eff": pd.NamedAgg(column="cpu_efficency", aggfunc=np.max),
            "Mean % CPU Eff": pd.NamedAgg(column="cpu_efficency", aggfunc=np.mean),
            "Min % Mem Eff": pd.NamedAgg(column="mem_efficiency", aggfunc=np.min),
            "Max % Mem Eff": pd.NamedAgg(column="mem_efficiency", aggfunc=np.max),
            "Mean % Mem Eff": pd.NamedAgg(column="mem_efficiency", aggfunc=np.mean),
            "Min % Time Eff": pd.NamedAgg(column="time_efficiency", aggfunc=np.min),
            "Max % Time Eff": pd.NamedAgg(column="time_efficiency", aggfunc=np.max),
            "Mean % Time Eff": pd.NamedAgg(column="time_efficiency", aggfunc=np.mean)
        }

)

# gdf = df.groupby(["User", "Partition", "State"], as_index=False, dropna=True).agg(
# numjobs=pd.NamedAgg(column="JobID", aggfunc="count"),
# cpu_eff_min=pd.NamedAgg(column="cpu_efficency", aggfunc="min"),
# cpu_eff_max=pd.NamedAgg(column="cpu_efficency", aggfunc="max"),
# cpu_eff_mean=pd.NamedAgg(column="cpu_efficency", aggfunc="mean"),
# mem_eff_min=pd.NamedAgg(column="mem_efficiency", aggfunc="min"),
# mem_eff_max=pd.NamedAgg(column="mem_efficiency", aggfunc="max"),
# mem_eff_mean=pd.NamedAgg(column="mem_efficiency", aggfunc="mean"),
# time_eff_min=pd.NamedAgg(column="time_efficiency", aggfunc="min"),
# time_eff_max=pd.NamedAgg(column="time_efficiency", aggfunc="max"),
# time_eff_mean=pd.NamedAgg(column="time_efficiency", aggfunc="mean")
# )

print("================================================================================================")
print("----------------------------------- Raapoi Efficiency Report -----------------------------------")
print("================================================================================================")

print(gdf.to_string(index=False))

print("=================================================================================================")
print("========== Support is available on the Raapoi Slack channel at https://uwrc.slack.com/ ==========")
print("== Raapoi Cluster Documentation lives at https://vuw-research-computing.github.io/raapoi-docs/ ==")
print("=================================================================================================")

# export to CSV
try:
    gdf.to_csv('eff_stats_' + username + '_' + today_csv.strftime('%Y%m%d_%H%M_%S') + '.csv')
except Exception as ex:
    print('Error writing the CSV export file.')
    print('Raapoi help is available on the Slack channel at https://uwrc.slack.com/')
    print(ex)
 

