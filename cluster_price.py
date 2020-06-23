import pandas as pd
import subprocess
from io import StringIO
import re
from datetime import timedelta
import numpy as np
from datetime import datetime, timedelta 
from io import StringIO


startdate = '2019-01-01'
use_currentdate = False # set to false to regenerate the entire dataset from the startdate. Use_currentdate assumes this has been running regulary via cron etc
overlap_length = 30 # days of overlap - when using the current date and appending the dataset, use this as the overlap to account from long running jobs this should be longer than max runtime

aws_cost = pd.read_csv('aws_cost_2019.csv')
aws_cost = aws_cost.sort_values(by=['Per_Hour'])

gibikibi = 1048576  # One GiBibyte in KibiBytes
mibikibi = 1024  #one MibiByte in Kibibytes
gibimibi = 1024 # one gibibyte in mibibytes

def clean_slurm1(mystr):
    mystr = re.sub(' +', ',',mystr)
    users_list = mystr.split('\n')
    users_list.pop(1)  #remove ------
    users_list.pop(-1) #remove empty end line

    clean_user_list = [mystring[1:] if mystring[0]==',' else mystring for mystring in users_list ] #all users except the longest username will have a leading ',' at this point, remove that 
    mystr = '\n'.join(clean_user_list) # recreate the now clean string
    return mystr

def clean_slurm2(mystr):
    #prepare output from sacct initial:
    # 2979             andre   00:00:10          8                       
    # 2979.batch               00:00:10          8      1508K    157460K 
    # 2979.extern              00:00:10          8      1128K    156940K 
    # 2979.0                   00:00:09          8      1148K    225560K 
    # 3247             andre   00:00:00         16                       
    # --> grep removes .ext* lines -- uneeded, just a result of using cgroups
    # --> replace . with spaces
    # 2979             andre   00:00:10          8                       
    # 2979 batch               00:00:10          8      1508K    157460K 
    # 2979 0                   00:00:09          8      1148K    225560K 
    # 3247             andre   00:00:00         16         
    # --> remove extra white space
    # 2979 andre 00:00:10 8                       
    # 2979 batch 00:00:10 8 1508K 157460K 
    # 2979 0 00:00:09 8 1148K 225560K 
    # 3247 andre 00:00:00 16
    # so when it goes into a pandas dataframe:
    # 2979 | andre | 00:00:10 | 8 |       |                       
    # 2979 | batch | 00:00:10 | 8 | 1508K | 157460K 
    # 2979 | 0     | 00:00:09 | 8 | 1148K | 225560K 
    # 3247 | andre | 00:00:00 | 16|       |
    mystr.replace('.',' ',1)
    mystr = re.sub(' +', ',',mystr)
    jobs_list = mystr.split('\n')
    jobs_list.pop(1)  #remove ------

    mystr = '\n'.join(jobs_list) # recreate the now clean string
    return jobs_list


def user_usage(user,startdate):
    print(user)
    #get user's assigned group - we have to do via os as currently all slurm users run as user
    group_string = subprocess.run(['groups',user],stdout=subprocess.PIPE).stdout.decode('utf-8')
    user_group = group_string.strip('\n').split(' ')[-1]
    sacct_string = subprocess.run(['sacct -S ' + startdate + ' --format="jobid%30,Elapsed,Start,NCPUS,MaxRSS,MaxVMSize,Partition,ReqCPUS,AllocCPUS,TotalCPU,ReqMem" -u '+user + '|grep -v ext'],shell=True,stdout=subprocess.PIPE).stdout.decode('utf-8')
    saact_string_orig = sacct_string
    sacct_string = clean_slurm2(sacct_string)
    sacct_string = list(filter(lambda x: 'K' in x or 'M' in x or 'G' in x, sacct_string)) #remove oostinto's weird empty job strings 

    sacct_string = '\n'.join(sacct_string)

    sacctio = StringIO(sacct_string)
    sacctdf = pd.read_csv(sacctio)
    df = sacctdf

    df = df[df.MaxVMSize.notna()] #Drop NaN value MaxVMSize, which is extraneous output
    costs = []
    cpu_request_list = []
    mem_request_list = []
    time_taken_hours = []
    start_time_list =[]

    #bad iterating over a df, TODO make better
    for row in df.itertuples():
        cpu_request = row.NCPUS
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
        alljobs_data = {'user':[user],
                'group': [user_group],
                'starttime':[np.nan],
                'cpu_hours': [np.nan],
                'gib_hours': [np.nan],
                'runtime_hours':[np.nan],
                'cpu_request':[np.nan],
                'gib_request':[np.nan],
                'aws_cost':[np.nan]}
    else:   
        alljobs_data = {'user':[user]*numjobs,
                        'group': [user_group]*numjobs,
                        'starttime':start_time_list,
                        'cpu_hours': cpu_hours,
                        'gib_hours': gib_hours,
                        'runtime_hours':time_taken_hours,
                        'cpu_request':cpu_request_list,
                        'gib_request':mem_request_list,
                        'aws_cost':costs}

    return [alljobs_data, saact_string_orig]


users_string = subprocess.run(['sacctmgr','show','user'],stdout=subprocess.PIPE).stdout.decode('utf-8')
users_string = clean_slurm1(users_string)

usersio=StringIO(users_string)
usersdf=pd.read_csv(usersio)

usernames=list(usersdf.User)

all_jobs_df = pd.DataFrame([],index=[0])
# usernames=['jiaowa']
all_strings=''

if use_currentdate == True:
    startdate = datetime.now() - timedelta(days = overlap_length) 
    startdate = startdate.strftime("%Y-%m-%d")

    #load the existing database
    old_df=pd.read_csv('all_jobs.csv')

    #load the allstrings as a dataframe, this is really inefficient, but makes deduping easy
    # old_allstrings = pd.read_csv('allstrings.txt',sep='\t') #1 column, with each row of data
    old_allstrings=pd.read_csv('all_strings.csv')

for user in usernames:
    if user not in ['leinma']:  #TODO!  This seems tricky to solve, removed users still show up in "sacctmgr show user" but not in "sacct -S 2019-01-01 --format="jobid%30,Elapsed,Start,NCPUS,MaxRSS,MaxVMSize" -u leinma"
        [all_jobs, saact_string_orig] = user_usage(user, startdate) 
        all_jobs_f = pd.DataFrame.from_dict(all_jobs)
        all_jobs_df  = pd.concat([all_jobs_df, all_jobs_f ],sort=False)
        all_strings = all_strings + saact_string_orig

# Convert all_strings to pd.dataframe to make de duping easier
stringdata = StringIO(all_strings)
all_strings_df = pd.read_csv(stringdata,sep='\t')

if use_currentdate == True:  #concat old and new df, removing duplicates
    all_jobs_df = pd.concat([old_df,all_jobs_df],sort=False).drop_duplicates().reset_index(drop=True)
    all_strings_df = pd.concat([old_allstrings, all_strings_df],sort=False).drop_duplicates().reset_index(drop=True) 
all_jobs_df.to_csv('all_jobs.csv')
all_strings_df.to_csv('all_strings.csv')
# with open("allstrings.txt", "w") as text_file:
#     print(f"{all_strings}", file=text_file)