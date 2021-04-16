import pandas as pd
import numpy as np

#pd.set_option('display.max_rows', 200, 'display.max_columns', 40, 'display.width', 200)
pd.set_option('use_inf_as_na', True)

# Import the data into a pandas dataframe
df = pd.read_csv("all_jobs_new_calc.csv", usecols=['Partition', 'User', 'State', 'JobID', 'cpu_efficency', 'mem_efficiency', 'time_efficiency'], low_memory=False)

# Kludge to remove weird 0 index row - needs fixing in origin script
df = df.drop([0])

# Combine individual CANCELLED states into one
#print(df['State'].value_counts())
df['State'] = df['State'].str.replace(r"CANCELLED.*$", "CANCELLED", regex=True)
#print(df['State'].value_counts())

# groupby and aggregate information
gdf = df.groupby(["Partition", "User", "State"], as_index=False, dropna=True).agg(
numjobs=pd.NamedAgg(column="JobID", aggfunc="count"),
cpu_eff_min=pd.NamedAgg(column="cpu_efficency", aggfunc="min"),
cpu_eff_max=pd.NamedAgg(column="cpu_efficency", aggfunc="max"),
cpu_eff_mean=pd.NamedAgg(column="cpu_efficency", aggfunc="mean"),
mem_eff_min=pd.NamedAgg(column="mem_efficiency", aggfunc="min"),
mem_eff_max=pd.NamedAgg(column="mem_efficiency", aggfunc="max"),
mem_eff_mean=pd.NamedAgg(column="mem_efficiency", aggfunc="mean"),
time_eff_min=pd.NamedAgg(column="time_efficiency", aggfunc="min"),
time_eff_max=pd.NamedAgg(column="time_efficiency", aggfunc="max"),
time_eff_mean=pd.NamedAgg(column="time_efficiency", aggfunc="mean")
)

# export to CSV
gdf.to_csv('eff_stats.csv')
