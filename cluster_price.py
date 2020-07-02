import pandas as pd
import subprocess
from io import StringIO
import re
from datetime import timedelta
import numpy as np
from datetime import datetime, timedelta 
from io import StringIO
import time


startdate = '2019-01-01'
use_currentdate = False # set to false to regenerate the entire dataset from the startdate. Use_currentdate assumes this has been running regulary via cron etc
overlap_length = 30 # days of overlap - when using the current date and appending the dataset, use this as the overlap to account from long running jobs this should be longer than max runtime

aws_cost = pd.read_csv('aws_cost_2019.csv')
aws_cost = aws_cost.sort_values(by=['Per_Hour'])

gibikibi = 1048576  # One GiBibyte in KibiBytes
mibikibi = 1024  #one MibiByte in Kibibytes
gibimibi = 1024 # one gibibyte in mibibytes

def timeformat(timelist):
    timeoutlist=[]
    for i, timein in enumerate(timelist):
        #format tims from slurms [DD-[HH:]]MM:SS to always having fields eg 01:20.456 to 00 days 00:01:20.456
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
        timeoutlist.append(timeout)

    return timeoutlist


def collate_saact(df):
    #     #compress
    #                          JobID         Elapsed               Start      NCPUS     MaxRSS  MaxVMSize  Partition  ReqCPUS  AllocCPUS        TotalCPU     ReqMem      State                 End 
    # ------------------------------ --------------- ------------------- ---------- ---------- ---------- ---------- -------- ---------- --------------- ---------- ---------- ------------------- 
    #                       99162_14        00:03:57 2019-03-13T23:31:36          2                         parallel        1          2       03:57.290  3145728Kc  COMPLETED 2019-03-13T23:35:33 
    #                 99162_14.batch        00:03:57 2019-03-13T23:31:36          2    136524K   1447464K                   2          2       03:57.286  3145728Kc  COMPLETED 2019-03-13T23:35:33 
    #to
    #                          JobID         Elapsed               Start      NCPUS     MaxRSS  MaxVMSize  Partition  ReqCPUS  AllocCPUS        TotalCPU     ReqMem      State                 End 
    # ------------------------------ --------------- ------------------- ---------- ---------- ---------- ---------- -------- ---------- --------------- ---------- ---------- ------------------- 
    #                       99162_14        00:03:57 2019-03-13T23:31:36          2    136524K   1447464K   parallel        1          2       03:57.290  3145728Kc  COMPLETED 2019-03-13T23:35:33 
    #                 99162_14.batch        00:03:57 2019-03-13T23:31:36          2    136524K   1447464K                   2          2       03:57.286  3145728Kc  COMPLETED 2019-03-13T23:35:33 


    #OR
    #                          JobID         Elapsed               Start   NNodes      NCPUS     MaxRSS  MaxVMSize  Partition  ReqCPUS  AllocCPUS        TotalCPU     ReqMem      State                 End 
    # ------------------------------ --------------- ------------------- -------- ---------- ---------- ---------- ---------- -------- ---------- --------------- ---------- ---------- ------------------- 
    #                         357284        00:00:13 2020-06-30T16:24:20        5         10                         parallel       10         10       00:08.045     3072Mn     FAILED 2020-06-30T16:24:33 
    #                   357284.batch        00:00:13 2020-06-30T16:24:20        1          2      1.38M    154.73M                   2          2       00:04.635     3072Mn     FAILED 2020-06-30T16:24:33 
    #                  357284.extern        00:00:13 2020-06-30T16:24:20        5         10      1.15M    154.46M                  10         10       00:00.017     3072Mn  COMPLETED 2020-06-30T16:24:33 
    #                       357284.0        00:00:08 2020-06-30T16:24:26        4          4      1.18M    221.47M                   4          4       00:03.391     3072Mn  COMPLETED 2020-06-30T16:24:34

    # TotalCPU / CPutime= cpu efficiency
    # .batch_maxRSS / reqmem*nodes (or cores?) = mem_efficiency
    # df = df[df.MaxVMSize.notna()] #Drop NaN value MaxVMSize, which is extraneous output
    
    for row in df.itertuples():
    
    costs = []
    cpu_request_list = []
    mem_request_list = []
    time_taken_hours = []
    start_time_list =[]

    #bad iterating over a df, TODO make better
    for row in df.itertuples():
        try:
            cpu_request = int(row.NCPUS)
        except:
            1/0
        if 'G' in row.MaxVMSize:
            memory_request = float(row.MaxVMSize.strip('G'))
        if 'M' in row.MaxVMSize:
            memory_request = float(row.MaxVMSize.strip('M'))/gibimibi
        elif 'K' in row.MaxVMSize:
            memory_request = float(row.MaxVMSize.strip('K'))/gibikibi
        try:
            aws_instance = aws_cost[(aws_cost.vCPU>=cpu_request) & (aws_cost.Memory>=memory_request)].iloc[0]
        except:
            # no possible instance, too much memory or too much ram, in this case, get multiples of the "closest" fitting instance.
            max_cpu = max(aws_cost.vCPU)
            max_memory = max(aws_cost.Memory)

            multiples_of_cpu = cpu_request / max_cpu
            multiples_of_memory = memory_request / max_memory

            if multiples_of_cpu > 1:
                cpu_request = max_cpu
            if multiples_of_memory > 1:
                memory_request = max_memory
            aws_instance = aws_cost[(aws_cost.vCPU>=cpu_request) & (aws_cost.Memory>=memory_request)].iloc[0]

            if multiples_of_memory > multiples_of_cpu:
                instance_multiplier = multiples_of_memory
            else:
                instance_multiplier = multiples_of_cpu
            
            aws_instance.Name = aws_instance.Name + ' *' + str(instance_multiplier)
            aws_instance.vCPU = aws_instance.vCPU * instance_multiplier
            aws_instance.Memory = aws_instance.Memory * instance_multiplier
            aws_instance.Per_Hour = aws_instance.Per_Hour * instance_multiplier


        days = 0
        if '-' in row.Elapsed:  #we have to handle days
            days,timestr = row.Elapsed.split('-')  # strip days which we will add later  *****
            days=int(days)
        else:
            timestr = row.Elapsed
            
        elapsed_time = [int(e) for e in timestr.split(':')] # hours, min, sec. 
        elapsed_time.insert(0,0)  #add days, which we have to handle manually
        elapsed_time[0] = elapsed_time[1]//24 #get the floored quotient of hours/24  ie 89 (3.7 days) will return 3 days
        elapsed_time[1] = elapsed_time[1]%24 #get the hour remainder
        elapsed_time[0] = elapsed_time[0]+days # add the days we stripped earler *****

        rt = timedelta(days=elapsed_time[0],hours=elapsed_time[1],minutes=elapsed_time[2],seconds=elapsed_time[3])
        rt_min = rt.total_seconds()/60
        rt_hours = rt_min/60 
        cost = rt_hours * aws_instance.Per_Hour

        costs.append(cost)
        cpu_request_list.append(cpu_request)
        mem_request_list.append(memory_request)
        time_taken_hours.append(rt_hours)

        start_time_list.append(row.Start)

    cpu_hours = np.array(cpu_request_list) * np.array(time_taken_hours)
    gib_hours = np.array(mem_request_list) * np.array(time_taken_hours)

    #check for empty data and nan it
    if not time_taken_hours:
        time_taken_hours = [np.nan]
    if not cpu_request_list:
        cpu_request_list=[np.nan]
    if not mem_request_list:
        mem_request_list=[np.nan] 

    numjobs = len(cpu_hours)

    if numjobs==0:
        df['user']=user
        df['group']=user_group
        df['starttime']=np.nan
        df['cpu_hours']=np.nan
        df['gib_hours']=np.nan
        df['runtime_hours']=np.nan
        df['cpu_request']=np.nan
        df['gib_request']=np.nan
        df['aws_cost']=np.nan
    else:
        df['user']=user
        df['group']=user_group
        df['starttime']=start_time_list
        df['cpu_hours']=cpu_hours
        df['gib_hours']=gib_hours
        df['runtime_hours']=time_taken_hours
        df['cpu_request']=cpu_request_list
        df['gib_request']=mem_request_list
        df['aws_cost']=costs

def user_usage(user,startdate):
    print(user)
    #get user's assigned group - we have to do via os as currently all slurm users run as user
    group_string = subprocess.run(['groups',user],stdout=subprocess.PIPE).stdout.decode('utf-8')
    user_group = group_string.strip('\n').split(' ')[-1]
    #TODO use -p for create | seperators between fields
    sacct_string = subprocess.run(['sacct --units=M -T -S ' + startdate + ' --format="jobid%30,Elapsed%15,Start,NNodes,NCPUS,MaxRSS,MaxVMSize,Partition,ReqCPUS,AllocCPUS,TotalCPU%15,CPUtime,ReqMem,State%10,End" -u '+user + '|grep -v ext'],shell=True,stdout=subprocess.PIPE).stdout.decode('utf-8')
    sacct_stringio=StringIO(sacct_string)
    df=pd.read_fwf(sacct_stringio)
    df=df.drop(df.index[0]) #remove all the ------ ----- -----




    df = df[df.MaxVMSize.notna()] #Drop NaN value MaxVMSize, which is extraneous output
    costs = []
    cpu_request_list = []
    mem_request_list = []
    time_taken_hours = []
    start_time_list =[]

    #bad iterating over a df, TODO make better
    for row in df.itertuples():
        try:
            cpu_request = int(row.NCPUS)
        except:
            1/0
        if 'G' in row.MaxVMSize:
            memory_request = float(row.MaxVMSize.strip('G'))
        if 'M' in row.MaxVMSize:
            memory_request = float(row.MaxVMSize.strip('M'))/gibimibi
        elif 'K' in row.MaxVMSize:
            memory_request = float(row.MaxVMSize.strip('K'))/gibikibi
        try:
            aws_instance = aws_cost[(aws_cost.vCPU>=cpu_request) & (aws_cost.Memory>=memory_request)].iloc[0]
        except:
            # no possible instance, too much memory or too much ram, in this case, get multiples of the "closest" fitting instance.
            max_cpu = max(aws_cost.vCPU)
            max_memory = max(aws_cost.Memory)

            multiples_of_cpu = cpu_request / max_cpu
            multiples_of_memory = memory_request / max_memory

            if multiples_of_cpu > 1:
                cpu_request = max_cpu
            if multiples_of_memory > 1:
                memory_request = max_memory
            aws_instance = aws_cost[(aws_cost.vCPU>=cpu_request) & (aws_cost.Memory>=memory_request)].iloc[0]

            if multiples_of_memory > multiples_of_cpu:
                instance_multiplier = multiples_of_memory
            else:
                instance_multiplier = multiples_of_cpu
            
            aws_instance.Name = aws_instance.Name + ' *' + str(instance_multiplier)
            aws_instance.vCPU = aws_instance.vCPU * instance_multiplier
            aws_instance.Memory = aws_instance.Memory * instance_multiplier
            aws_instance.Per_Hour = aws_instance.Per_Hour * instance_multiplier


        days = 0
        if '-' in row.Elapsed:  #we have to handle days
            days,timestr = row.Elapsed.split('-')  # strip days which we will add later  *****
            days=int(days)
        else:
            timestr = row.Elapsed
            
        elapsed_time = [int(e) for e in timestr.split(':')] # hours, min, sec. 
        elapsed_time.insert(0,0)  #add days, which we have to handle manually
        elapsed_time[0] = elapsed_time[1]//24 #get the floored quotient of hours/24  ie 89 (3.7 days) will return 3 days
        elapsed_time[1] = elapsed_time[1]%24 #get the hour remainder
        elapsed_time[0] = elapsed_time[0]+days # add the days we stripped earler *****

        rt = timedelta(days=elapsed_time[0],hours=elapsed_time[1],minutes=elapsed_time[2],seconds=elapsed_time[3])
        rt_min = rt.total_seconds()/60
        rt_hours = rt_min/60 
        cost = rt_hours * aws_instance.Per_Hour

        costs.append(cost)
        cpu_request_list.append(cpu_request)
        mem_request_list.append(memory_request)
        time_taken_hours.append(rt_hours)

        start_time_list.append(row.Start)

    cpu_hours = np.array(cpu_request_list) * np.array(time_taken_hours)
    gib_hours = np.array(mem_request_list) * np.array(time_taken_hours)

    #check for empty data and nan it
    if not time_taken_hours:
        time_taken_hours = [np.nan]
    if not cpu_request_list:
        cpu_request_list=[np.nan]
    if not mem_request_list:
        mem_request_list=[np.nan] 

    numjobs = len(cpu_hours)

    if numjobs==0:
        df['user']=user
        df['group']=user_group
        df['starttime']=np.nan
        df['cpu_hours']=np.nan
        df['gib_hours']=np.nan
        df['runtime_hours']=np.nan
        df['cpu_request']=np.nan
        df['gib_request']=np.nan
        df['aws_cost']=np.nan
    else:
        df['user']=user
        df['group']=user_group
        df['starttime']=start_time_list
        df['cpu_hours']=cpu_hours
        df['gib_hours']=gib_hours
        df['runtime_hours']=time_taken_hours
        df['cpu_request']=cpu_request_list
        df['gib_request']=mem_request_list
        df['aws_cost']=costs
    return [df]


users_string = subprocess.run(['sacctmgr','show','user'],stdout=subprocess.PIPE).stdout.decode('utf-8')
usersio=StringIO(users_string)
usersdf=pd.read_fwf(usersio)
usersdf=usersdf.drop(usersdf.index[0]) #remove all the ------ ----- -----
#usersdf=pd.read_csv(usersio)


usernames=list(usersdf.User)

all_jobs_df = pd.DataFrame([],index=[0])
# usernames=['andre']
all_strings=''

if use_currentdate == True:
    startdate = datetime.now() - timedelta(days = overlap_length) 
    startdate = startdate.strftime("%Y-%m-%d")

    #load the existing database
    old_df=pd.read_csv('all_jobs.csv')

    #load the allstrings as a dataframe, this is really inefficient, but makes deduping easy
    # old_allstrings = pd.read_csv('allstrings.txt',sep='\t') #1 column, with each row of data
    # old_allstrings=pd.read_csv('all_strings.csv')

for user in usernames:
    if user not in ['leinma']:  #TODO!  This seems tricky to solve, removed users still show up in "sacctmgr show user" but not in "sacct -S 2019-01-01 --format="jobid%30,Elapsed,Start,NCPUS,MaxRSS,MaxVMSize" -u leinma"
        [all_jobs_f] = user_usage(user, startdate) 
        all_jobs_df  = pd.concat([all_jobs_df, all_jobs_f ],sort=False)

# t0 = time.time()
# #efficenicy is (TotalCPU/ncpu)/Elapsed
# elapsed_seconds = pd.to_timedelta(timeformat(all_jobs_df.Elapsed)).dt.total_seconds()
# cpu_t_seconds = pd.to_timedelta(timeformat(all_jobs_df.TotalCPU)).dt.total_seconds() 
# all_jobs_df['cpu_efficency'] = (cpu_t_seconds/all_jobs_df.NCPUS) / elapsed_seconds *100
# t1 = time.time()
# print('eff calc time:')
# print(t1-t0)
# 1/0

t0 = time.time()
#efficenicy is (TotalCPU/ncpu)/Elapsed
all_jobs_df.Elapsed = pd.to_timedelta(timeformat(all_jobs_df.Elapsed))
all_jobs_df.TotalCPU = pd.to_timedelta(timeformat(all_jobs_df.TotalCPU))
all_jobs_df.NCPUS = pd.to_numeric(all_jobs_df.NCPUS) 
all_jobs_df['cpu_efficency'] = (all_jobs_df.TotalCPU/all_jobs_df.NCPUS) / all_jobs_df.Elapsed *100
t1 = time.time()
print('eff calc time:')
print(t1-t0)



# # Convert all_strings to pd.dataframe to make de duping easier
# stringdata = StringIO(all_strings)
# all_strings_df = pd.read_csv(stringdata,sep='\t')

if use_currentdate == True:  #concat old and new df, removing duplicates
    all_jobs_df = pd.concat([old_df,all_jobs_df],sort=False).drop_duplicates().reset_index(drop=True)
all_jobs_df.to_csv('all_jobs.csv')
# with open("allstrings.txt", "w") as text_file:
#     print(f"{all_strings}", file=text_file)