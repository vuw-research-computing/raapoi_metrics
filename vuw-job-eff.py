#!/home/software/apps/python/3.8.1/bin/python3

import sys
import pandas as pd
import numpy as np
import getpass as gp
import argparse as ap
import datetime as dt
import subprocess
from io import StringIO
#import pdb; pdb.set_trace()

today_csv = dt.datetime.now()
pd.set_option('use_inf_as_na', True)

def check_positive_days(value):
    days = int(value)
    if days < 0:
        raise ap.ArgumentTypeError("%s: Days cannot be a negative number" % value)
    return days

parser = ap.ArgumentParser(prog='vuw-job-eff', description='Raapoi Job Efficiency tool. Reports on the minimum, maximum and mean average efficiency of your Raapoi jobs for CPU, memory, and time requested.', epilog='For more information see the Raapoi Cluster Documentation: https://vuw-research-computing.github.io/raapoi-docs/')
parser.add_argument('-d', '--days', help='number of days for the report output; end date is today, default is 10 days', default=10, type=check_positive_days)
parser.add_argument('-f', '--file', help='save summary efficiency output to a CSV file, automatically generated in the current working directory', action='store_true')
parser.add_argument('-F', '--fullfile', help='save individual job efficiency output to a CSV file, automatically generated in the current working directory', action='store_true')
parser.add_argument('-u', '--username', help=ap.SUPPRESS, default=gp.getuser())
args = parser.parse_args()

username = args.username
num_days = args.days
today = dt.date.today()
earliest_start_date = dt.date(2019,1,1)
start_date = today + dt.timedelta(-num_days)
if (start_date < earliest_start_date):
    start_date = earliest_start_date

print('Report start date: ' + start_date.isoformat())
print('Report end date: ' + today.isoformat())
print('Collecting job efficiency statistics. This may take a minute...')

# gibikibi = 1048576  # One GiBibyte in KibiBytes
# mibikibi = 1024  #one MibiByte in Kibibytes
gibimibi = 1024 # one gibibyte in mibibytes


def timeformat_lambda(timein):
    #format times from slurms [DD-[HH:]]MM:SS to always having fields eg 01:20.456 to 00 days 00:01:20.456
    if pd.isnull(timein):
        timeout = '00 days 00:00:00'
    elif timein.count('-')==1:
        timeout = timein.replace('-', ' days ')
    elif timein.count(':')==1:
        timeout = '00 days 00:'+timein
    elif timein.count(':')==2:
        timeout = '00 days '+timein
    else:
        print(timein)
        1/0 #error! weird time function
    timeout = pd.to_timedelta(timeout)
    return timeout

def memfix(inmem):
    # takes 1.38M or nan and returns
    #1.38 or 0
    if pd.isnull(inmem):
        outmem=0
    else:
        outmem=float(inmem.strip('M'))
    return outmem

def cleanjobid(jobid):
    #takes 99162_14 or 99162_14.batch etc and returns
    # 99162_14 or 99162_14
    jobid=str(jobid)
    jobid = jobid.split('.',1)[0]
    return jobid

def collate_saact(indf):

    column_names=list(indf.columns)
    df = pd.DataFrame(columns=column_names)  # empty df with indf column names
    # rootid='xx'
    # rowkeep = df[:1]

    #All memory amounts are in M, so we can strip it out of MaxRSS and MaxVMSize
    indf['MaxRSS'] = indf['MaxRSS'].map(lambda x: memfix(x))
    indf['MaxVMSize'] = indf['MaxVMSize'].map(lambda x: memfix(x))

    #timeformat_lambda
    indf['Elapsed'] = indf['Elapsed'].map(lambda x: timeformat_lambda(x))
    indf['Timelimit'] = indf['Timelimit'].map(lambda x: timeformat_lambda(x))
    indf['TotalCPU'] = indf['TotalCPU'].map(lambda x: timeformat_lambda(x))

    #drop rows where the job didn't run
    indf.drop(indf[indf['Elapsed'] == '0 days 00:00:00'].index, inplace = True)

    #Fix all job IDs to be 23 23 23 from 23 23.batch 23.0
    indf['JobID'] = indf['JobID'].map(lambda x: cleanjobid(x))
    df_agg = indf.groupby('JobID').agg({
    'User':lambda x: x.iloc[0],
    'Account': lambda x: x.iloc[0],
    'JobID': lambda x: x.iloc[0],
    'Elapsed': np.max,
    'Timelimit': np.max,
    'Start': lambda x: x.iloc[0],  #first one in group
    'NNodes': lambda x: x.iloc[0],
    'NTasks': np.max,
    'MaxRSS' : np.max,
    'MaxVMSize' : np.max,
    'Partition': lambda x: x.iloc[0],
    'ReqCPUS': lambda x: x.iloc[0],
    'AllocCPUS': lambda x: x.iloc[0],
    'TotalCPU': np.max,
    'ReqMem': lambda x: x.iloc[0],
    'AllocGRES': lambda x: x.iloc[0],
    'State': lambda x: x.iloc[0],
    'End': lambda x: x.iloc[0]
    })

    return df_agg

def user_usage(user,start_date,calcOld=False):
    sacct_string = subprocess.run(['sacct --units=M -p -T -S ' + start_date.isoformat() + ' --format="jobid%30,Elapsed%15,Timelimit,Start,NNodes,NCPUS,NTasks,MaxRSS,MaxVMSize,Partition,ReqCPUS,AllocCPUS,TotalCPU%15,CPUtime,ReqMem,AllocGRES,State%10,End, User, Account" -u '+ username + ' --noconvert ' + '|grep -v ext'],shell=True,stdout=subprocess.PIPE).stdout.decode('utf-8')
    sacct_stringio=StringIO(sacct_string)
    df=pd.read_csv(sacct_stringio,sep='|')
    #df['User'] = username
    #drop rows for jobs that started running before the specified report start time
    #df.to_csv('testing_df1.csv')
    df.drop(df[df['Start'] == start_date.isoformat() + 'T00:00:00'].index, inplace = True)
    df.drop(df[df['Start'] == 'Unknown'].index, inplace = True)
    #df.to_csv('testing_df2.csv')
    newdf=collate_saact(df)
    newdf.to_csv('testing_newdf.csv')
    return newdf

def totalmem(row):
    '''
    Take a MemReq like 3072Mc or 3072Mn and convert it to total memory requested
    then convert memory to gibibytes
    '''
    if pd.isna(row.ReqMem):
        totalmemreq = np.nan
    elif 'n' in row.ReqMem: #memory per node
        totalmemreq = int( row.ReqMem.strip('Mn') ) * row.NNodes
    elif 'c' in row.ReqMem:  #memory per core
        totalmemreq = int( row.ReqMem.strip('Mc') ) * row.AllocCPUS
    totalmemreq = totalmemreq / gibimibi
    return totalmemreq

all_jobs_newdf = pd.DataFrame([],index=[0])

newdf = user_usage(username, start_date, calcOld=True)
all_jobs_newdf  = pd.concat([all_jobs_newdf, newdf ],sort=False)
all_jobs_newdf.dropna(how='all', inplace=True)
all_jobs_newdf = all_jobs_newdf.loc[(-all_jobs_newdf['State'].isin(['PENDING','RUNNING']))]
all_jobs_newdf['State'] = all_jobs_newdf['State'].str.replace(r'CANCELLED.*$', 'CANCELLED', regex=True)

if not all_jobs_newdf.empty:

    all_jobs_newdf['TotalReqMemGiB'] = all_jobs_newdf.apply(totalmem, axis=1)
    all_jobs_newdf['ElapsedSeconds'] = all_jobs_newdf.apply(lambda x: x['Elapsed'].total_seconds(), axis=1)
    all_jobs_newdf['TimelimitSeconds'] = all_jobs_newdf.apply(lambda x: x['Timelimit'].total_seconds(), axis=1)
    all_jobs_newdf['TotalCPUSeconds'] = all_jobs_newdf.apply(lambda x: x['TotalCPU'].total_seconds(), axis=1)

    #Add Elapsed time column(in hours)
    all_jobs_newdf['ElapsedHours'] = all_jobs_newdf['ElapsedSeconds']/3600.0
    #Add Allocated CPU hours - if requested 10 CPU for 100 Hours, but run took 10 hours and only used 5 cpu-> 10CPU still allocated for the 10 hours = 100CPU hours
    all_jobs_newdf['AllocatedCPUHours_used'] = all_jobs_newdf['ElapsedHours'] * all_jobs_newdf['AllocCPUS']

    # CPU efficiency is (TotalCPU/ncpu)/Elapsed
    all_jobs_newdf['cpu_efficency'] = (all_jobs_newdf.TotalCPU/all_jobs_newdf.ReqCPUS) / all_jobs_newdf.Elapsed * 100

    # Memory efficiency is MaxRSS/TotalReqMemGiB converted to MiB
    all_jobs_newdf['mem_efficiency'] = (all_jobs_newdf.MaxRSS / (all_jobs_newdf.TotalReqMemGiB * gibimibi)) * 100

    # Time efficiency is ElapsedSeconds/TimelimitSeconds
    all_jobs_newdf['time_efficiency'] = (all_jobs_newdf.ElapsedSeconds / all_jobs_newdf.TimelimitSeconds) * 100

gdf = pd.DataFrame()

if 'cpu_efficency' in all_jobs_newdf.columns:
    df = all_jobs_newdf[['Partition', 'User', 'State', 'JobID', 'Start', 'End', 'cpu_efficency', 'mem_efficiency', 'time_efficiency']]

    # Pull out the data we need - user and date range, exclude PENDING and RUNNING jobs. Replace all CANCELLED% with CANCELLED.
    #df = df.loc[(df['User'] == username)]
    #df['Start'] = pd.to_datetime(df['Start'])
    #df = df.loc[(df['Start'].dt.date >= start_date)]

    # groupby and aggregate information
    gdf = df.groupby(['User', 'Partition', 'State'], as_index=False, dropna=True).agg(
            **{
                'Num Jobs': pd.NamedAgg(column='JobID', aggfunc='count'),
                'Min % CPU Eff': pd.NamedAgg(column='cpu_efficency', aggfunc=np.min),
                'Max % CPU Eff': pd.NamedAgg(column='cpu_efficency', aggfunc=np.max),
                'Mean % CPU Eff': pd.NamedAgg(column='cpu_efficency', aggfunc=np.mean),
                'Min % Mem Eff': pd.NamedAgg(column='mem_efficiency', aggfunc=np.min),
                'Max % Mem Eff': pd.NamedAgg(column='mem_efficiency', aggfunc=np.max),
                'Mean % Mem Eff': pd.NamedAgg(column='mem_efficiency', aggfunc=np.mean),
                'Min % Time Eff': pd.NamedAgg(column='time_efficiency', aggfunc=np.min),
                'Max % Time Eff': pd.NamedAgg(column='time_efficiency', aggfunc=np.max),
                'Mean % Time Eff': pd.NamedAgg(column='time_efficiency', aggfunc=np.mean)
            }
    )

print("=================================================================================================")
print("----------------------------------- Raapoi Efficiency Report ------------------------------------")
print("=================================================================================================")

if not gdf.empty:
    print(gdf.to_string(index=False))
else:
    print(" *** No results found. Use the -d flag to specify the number of days to use (default is 10). *** ")

print("=================================================================================================")
print("========== Support is available on the Raapoi Slack channel at https://uwrc.slack.com/ ==========")
print("== Raapoi Cluster Documentation lives at https://vuw-research-computing.github.io/raapoi-docs/ ==")
print("=================================================================================================")

# export to CSV
if (args.file):
    try:
        gdf.to_csv('eff_summary_' + username + '_' + today_csv.strftime('%Y%m%d_%H%M_%S') + '.csv')
    except Exception as ex:
        print('Error writing the summary CSV export file.')
        print('Raapoi help is available on the Slack channel at https://uwrc.slack.com/')
        print(ex)

# export full results to CSV
if (args.fullfile):
    try:
        all_jobs_newdf.to_csv('eff_full_' + username + '_' + today_csv.strftime('%Y%m%d_%H%M_%S') + '.csv')
    except Exception as ex:
        print('Error writing the full CSV export file.')
        print('Raapoi help is available on the Slack channel at https://uwrc.slack.com/')
        print(ex)
