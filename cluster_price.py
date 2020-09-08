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

def burstfinder(invcpu):
    vcpu_params = re.split('vCPUs|for a|burst',invcpu)
    if len(vcpu_params) == 4:
        burst = vcpu_params.strip()
    else:
        burst = pd.to_timedelta('nan')
    return burst



#old cost sheet
aws_cost = pd.read_csv('aws_cost_2019.csv')
aws_cost = aws_cost.sort_values(by=['Per_Hour'])

# #new cost sheet
aws_cost = pd.read_csv('Amazon EC2 Instance ComparisonJune2020.csv')
aws_cost[['vCPUs', 'burst']]=aws_cost.vCPUs.str.split("for a",expand=True)
aws_cost['vCPUs'] = aws_cost['vCPUs'].map(  lambda x: int(x.replace('vCPUs',''))  )
xstr = lambda s: '' if s is None else str(s)
aws_cost['burst'] = aws_cost['burst'].map(  lambda x: xstr(x)+'burst')  
aws_cost['burst'] = aws_cost['burst'].map(  lambda x: pd.to_timedelta(x.replace('burst',''))  )
aws_cost['burstable'] = aws_cost['burst'].map(  lambda x: not pd.isnull(x))  
aws_cost['Memory'] = aws_cost['Memory'].map(  lambda x: float(x.replace('GiB',''))  )
aws_cost = aws_cost.rename(columns={'vCPUs':'vCPU', 'Linux On Demand cost':'Per_Hour'})
aws_cost['Per_Hour'] = aws_cost['Per_Hour'].map( lambda x: x.replace('$',''))
aws_cost['Per_Hour'] = aws_cost['Per_Hour'].map( lambda x: x.replace('hourly',''))
aws_cost['Per_Hour'] = aws_cost['Per_Hour'].map( lambda x: x.replace('unavailable','nan'))
aws_cost['Per_Hour'] = aws_cost['Per_Hour'].map( lambda x: float(x))
aws_cost = aws_cost.sort_values(by=['Per_Hour'])
aws_cost = aws_cost[~aws_cost['Physical Processor'].str.contains('Graviton')]  #remove ARM processors as they are harder to compare  
aws_cost.dropna(subset=['Per_Hour'], inplace=True)

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


def timeformat_lambda(timein):

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

def aws_cost_equiv(row):
    print_cost=False
    if row.ReqCPUS == '1': # if just 1 cpu - there are matching aws instances, all other cases use alloccpus as some people try non hyperthreading jobs which will require alloc cpus on AWS as well.
        cpu_request = int(row.ReqCPUS)
    else:
        cpu_request = int(row.AllocCPUS)
    
    nodes = int(row.NNodes)
    mem_req_per_node = int(''.join(re.findall(r'\d+', row.ReqMem))) / gibimibi  #in gibibytes
    
    if 'n' in row.ReqMem: #memory per node
        pass 
    elif 'c' in row.ReqMem:  #memory per core
        mem_req_per_node = (mem_req_per_node * cpu_request) / nodes
    
    cpu_request = cpu_request / nodes  #we want per node cpu_request
    memory_used = row.MaxRSS/gibimibi  #always M now
    try:
        aws_instance = aws_cost[(aws_cost.vCPU>=cpu_request) & (aws_cost.Memory>=mem_req_per_node)].iloc[0]
        if not pd.isnull(aws_instance.burst):
            if row.Elapsed > aws_instance.burst:  #outside of burst time
                aws_instance = aws_cost[((aws_cost.vCPU>=cpu_request) & (aws_cost.Memory>=mem_req_per_node)) & (aws_cost.burstable==False)].iloc[0]
                
    except:
        if row.Elapsed.total_seconds() > 10:  #warn about dubious aws fits it elapsed is actually meaningful. i.e. not just erroneous user entries.
            print('### Warning jobid',row.JobID,' Does not fit an aws instance for costing, dubious measures ensue!### ',end='')
            print_cost = True
        # no possible instance, too much memory or too much ram, in this case, get multiples of the "closest" fitting instance.
        max_cpu = max(aws_cost.vCPU)
        max_memory = max(aws_cost.Memory)

        multiples_of_cpu = cpu_request / max_cpu
        multiples_of_memory = mem_req_per_node / max_memory

        if multiples_of_cpu > 1:
            cpu_request = max_cpu
        if multiples_of_memory > 1:
            mem_req_per_node = max_memory
        aws_instance = aws_cost[(aws_cost.vCPU>=cpu_request) & (aws_cost.Memory>=mem_req_per_node)].iloc[0]

        if multiples_of_memory > multiples_of_cpu:
            instance_multiplier = multiples_of_memory
        else:
            instance_multiplier = multiples_of_cpu
        
        aws_instance.Name = aws_instance.Name + ' *' + str(instance_multiplier)
        aws_instance.vCPU = aws_instance.vCPU * instance_multiplier
        aws_instance.Memory = aws_instance.Memory * instance_multiplier
        aws_instance.Per_Hour = aws_instance.Per_Hour * instance_multiplier

    rt = row.Elapsed
    rt_min = rt.total_seconds()/60
    rt_hours = rt_min/60 
    cost = rt_hours * aws_instance.Per_Hour
    cost = cost * nodes
    if print_cost == True:
        print('AWS_est_cost =  ',cost,'  ',end='')
    # row['aws_cost']  = cost
    return cost

def collate_saact(indf):
    #     #compress
    #                          JobID         Elapsed               Start   NNodes      NCPUS     MaxRSS  MaxVMSize  Partition  ReqCPUS  AllocCPUS        TotalCPU     ReqMem      State                 End 
    # ------------------------------ --------------- ------------------- -------- ---------- ---------- ---------- ---------- -------- ---------- --------------- ---------- ---------- ------------------- 
    #                         357284        00:00:13 2020-06-30T16:24:20        5         10                         parallel       10         10       00:08.045     3072Mn     FAILED 2020-06-30T16:24:33 
    #                   357284.batch        00:00:13 2020-06-30T16:24:20        1          2      1.38M    154.73M                   2          2       00:04.635     3072Mn     FAILED 2020-06-30T16:24:33 
    #                  357284.extern        00:00:13 2020-06-30T16:24:20        5         10      1.15M    154.46M                  10         10       00:00.017     3072Mn  COMPLETED 2020-06-30T16:24:33 
    #                       357284.0        00:00:08 2020-06-30T16:24:26        4          4      1.18M    221.47M                   4          4       00:03.391     3072Mn  COMPLETED 2020-06-30T16:24:34
    #to
    #                       JobID   Elapsed                Start NNodes  MaxRSS  MaxVMSize Partition ReqCPUS AllocCPUS   TotalCPU  ReqMem   State                  End
    #             JobID                                                                                                                                              
    #             357284  357284  00:00:13  2020-06-30T16:24:20      5    1.38     221.47  parallel      10        10  00:08.045  3072Mn  FAILED  2020-06-30T16:24:33


    column_names=list(indf.columns)
    df = pd.DataFrame(columns=column_names)  # empty df with indf column names
    rootid='xx'
    rowkeep = df[:1]
    #All memory amounts are in M, so we can strip it out of MaxRSS and MaxVMSize
    indf['MaxRSS'] = indf['MaxRSS'].map(lambda x: memfix(x))
    indf['MaxVMSize'] = indf['MaxVMSize'].map(lambda x: memfix(x))

    #timeformat_lambda
    indf['Elapsed'] = indf['Elapsed'].map(lambda x: timeformat_lambda(x))
    indf['TotalCPU'] = indf['TotalCPU'].map(lambda x: timeformat_lambda(x))

    #Fix all job IDs to be 23 23 23 from 23 23.batch 23.0
    indf['JobID'] = indf['JobID'].map(lambda x: cleanjobid(x))
    df_agg = indf.groupby('JobID').agg({
    'User':lambda x: x.iloc[0],
    'Account': lambda x: x.iloc[0],
    'JobID': lambda x: x.iloc[0],
    'Elapsed': np.max,
    'Start': lambda x: x.iloc[0],  #first one in group
    'NNodes': lambda x: x.iloc[0],
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
    if not df_agg.empty:
        df_agg['aws_cost'] = df_agg.apply(aws_cost_equiv, axis = 1) 
    return df_agg


def user_usage(user,startdate,calcOld=False):
    #get user's assigned group - we have to do via os as currently all slurm users run as user
    # group_string = subprocess.run(['groups',user],stdout=subprocess.PIPE).stdout.decode('utf-8')
    # user_group = group_string.strip('\n').split(' ')[-1]
    t0 = time.time()
    sacct_string = subprocess.run(['sacct --units=M -p -T -S ' + startdate + ' --format="jobid%30,Elapsed%15,Start,NNodes,NCPUS,MaxRSS,MaxVMSize,Partition,ReqCPUS,AllocCPUS,TotalCPU%15,CPUtime,ReqMem,AllocGRES,State%10,End, User, Account" -u '+user + ' --noconvert ' + '|grep -v ext'],shell=True,stdout=subprocess.PIPE).stdout.decode('utf-8')
    t1 = time.time()
    print('  saact time: ', end='')
    print(t1-t0, end='')
    sacct_stringio=StringIO(sacct_string)
    # df=pd.read_fwf(sacct_stringio)

    #check for invalid userid = users removed!
    if not sacct_string=='':
        df=pd.read_csv(sacct_stringio,sep='|')
    else:
        print(' ### Warning! user:',user,' has been removed from sacct ### ',end='')
        1/0

    newdf=collate_saact(df)

    if calcOld ==True:
        df = df[df.MaxVMSize.notna()] #Drop NaN value MaxVMSize, which is extraneous output - effectivly removes the "root" jobid - which in the otehr method we keep instead and base off.
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
            memory_request = row.MaxVMSize/gibimibi
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


            rt = row.Elapsed
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
            df['starttime']=np.nan
            df['cpu_hours']=np.nan
            df['gib_hours']=np.nan
            df['runtime_hours']=np.nan
            df['cpu_request']=np.nan
            df['gib_request']=np.nan
            df['aws_cost']=np.nan
        else:
            df['starttime']=start_time_list
            df['cpu_hours']=cpu_hours
            df['gib_hours']=gib_hours
            df['runtime_hours']=time_taken_hours
            df['cpu_request']=cpu_request_list
            df['gib_request']=mem_request_list
            df['aws_cost']=costs
    if not newdf.empty:
        print(' New aws cost: ',end='')
        print(newdf.aws_cost.sum() ,end='')
    if 'aws_cost' in df.keys():
        print(' Old aws cost: ',end='')
        print(df.aws_cost.sum(),end='')
    

    return [df, newdf]


users_string = subprocess.run(['sacctmgr','show','user'],stdout=subprocess.PIPE).stdout.decode('utf-8')
usersio=StringIO(users_string)
usersdf=pd.read_fwf(usersio)
usersdf=usersdf.drop(usersdf.index[0]) #remove all the ------ ----- -----
#usersdf=pd.read_csv(usersio)


usernames=list(usersdf.User)

all_jobs_df = pd.DataFrame([],index=[0])
all_jobs_newdf = pd.DataFrame([],index=[0])
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
    userexists = subprocess.run(['sacct','-u',user],stdout=subprocess.PIPE).stdout.decode('utf-8')  # returns stuff for existing users and '' for users who have been deleted
    if userexists!='':  #TODO!  This seems tricky to solve, removed users still show up in "sacctmgr show user" but not in "sacct -S 2019-01-01 --format="jobid%30,Elapsed,Start,NCPUS,MaxRSS,MaxVMSize" -u leinma"
        print(user, end = '')
        t0 = time.time()
        [all_jobs_f,newdf] = user_usage(user, startdate, calcOld=True) 
        all_jobs_df  = pd.concat([all_jobs_df, all_jobs_f ],sort=False)
        all_jobs_newdf  = pd.concat([all_jobs_newdf, newdf ],sort=False)
        t1 = time.time()
        print('  pycalc time:', end='')
        print(t1-t0)
    else:
        print('### User: ', user, ' has been removed from sacct but probbaly not sacctmgr!  ###',end='')

t0 = time.time()
#efficenicy is (TotalCPU/ncpu)/Elapsed
try:
    all_jobs_df['cpu_efficency'] = (all_jobs_df.TotalCPU/all_jobs_df.NCPUS) / all_jobs_df.Elapsed *100
except:
    print('warning no jobs?')
all_jobs_newdf['cpu_efficency'] = (all_jobs_newdf.TotalCPU/all_jobs_newdf.ReqCPUS) / all_jobs_newdf.Elapsed *100
t1 = time.time()
print('eff calc time:', end='')
print(t1-t0)


# Post process for easier analysis.
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
all_jobs_newdf['TotalReqMemGiB'] = all_jobs_newdf.apply(totalmem, axis=1)
all_jobs_newdf['ElapsedSeconds'] = all_jobs_newdf.apply(lambda x: x['Elapsed'].total_seconds, axis=1)
all_jobs_newdf['ElapsedSeconds'] = all_jobs_newdf.apply(lambda x: x['TotalCPU'].total_seconds, axis=1)


# # Convert all_strings to pd.dataframe to make de duping easier
# stringdata = StringIO(all_strings)
# all_strings_df = pd.read_csv(stringdata,sep='\t')

if use_currentdate == True:  #concat old and new df, removing duplicates
    all_jobs_df = pd.concat([old_df,all_jobs_df],sort=False).drop_duplicates().reset_index(drop=True)
all_jobs_df.to_csv('all_jobs.csv')
all_jobs_newdf.to_csv('all_jobs_new_calc.csv')
# with open("allstrings.txt", "w") as text_file:
#     print(f"{all_strings}", file=text_file)