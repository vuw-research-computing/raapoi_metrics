import re
import subprocess
import time
from datetime import datetime, timedelta
from io import StringIO

import numpy as np
import pandas as pd

# Constants
startdate = '2022-10-01'
use_currentdate = False
overlap_length = 30
gibikibi = 1048576
mibikibi = 1024
gibimibi = 1024

def burstfinder(invcpu):
    vcpu_params = re.split('vCPUs|for a|burst', invcpu)
    if len(vcpu_params) == 4:
        burst = vcpu_params.strip()
    else:
        burst = pd.to_timedelta('nan')
    return burst

def prepare_aws_cost_data():
    aws_cost = pd.read_csv('aws_cost_2019.csv')
    aws_cost = aws_cost.sort_values(by=['Per_Hour'])
    aws_cost = pd.read_csv('Amazon EC2 Instance ComparisonJune2020.csv')
    aws_cost[['vCPUs', 'burst']] = aws_cost.vCPUs.str.split("for a", expand=True)
    aws_cost['vCPUs'] = aws_cost['vCPUs'].map(lambda x: int(x.replace('vCPUs', '')))
    aws_cost['burst'] = aws_cost['burst'].map(lambda x: ('' if x is None else str(x)) + 'burst')
    aws_cost['burst'] = aws_cost['burst'].map(lambda x: pd.to_timedelta(x.replace('burst', '')))
    aws_cost['burstable'] = aws_cost['burst'].map(lambda x: not pd.isnull(x))
    aws_cost['Memory'] = aws_cost['Memory'].map(lambda x: float(x.replace('GiB', '')))
    aws_cost = aws_cost.rename(columns={'vCPUs': 'vCPU', 'Linux On Demand cost': 'Per_Hour'})
    aws_cost['Per_Hour'] = aws_cost['Per_Hour'].str.replace('$', '').str.replace('hourly', '').str.replace('unavailable', 'nan').astype(float)
    aws_cost = aws_cost.sort_values(by=['Per_Hour'])
    aws_cost = aws_cost[~aws_cost['Physical Processor'].str.contains('Graviton')]
    aws_cost.dropna(subset=['Per_Hour'], inplace=True)

    return aws_cost
def timeformat(timelist):
    timeoutlist=[]
    for i, timein in enumerate(timelist):
        
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
            1/0 
        timeoutlist.append(timeout)
    return timeoutlist
def timeformat_lambda(timein):
    if pd.isnull(timein):
        timeout = '00 days 00:00:00'
    elif timein.count('-') == 1:
        timeout = timein.replace('-', ' days ')
    elif timein.count(':') == 1:
        timeout = '00 days 00:' + timein
    elif timein.count(':') == 2:
        timeout = '00 days ' + timein
    else:
        print(timein)
        raise Exception("Invalid time format")
    timeout = pd.to_timedelta(timeout)
    return timeout

def memfix(inmem):
    if pd.isnull(inmem):
        outmem = 0
    else:
        outmem = float(inmem.strip('M'))
    return outmem

def cleanjobid(jobid):
    jobid = str(jobid)
    jobid = jobid.split('.', 1)[0]
    return jobid

def aws_cost_equiv(row):
    print_cost=False
    if row.ReqCPUS == '1': 
        cpu_request = int(row.ReqCPUS)
    else:
        cpu_request = int(row.AllocCPUS)
    
    nodes = int(row.NNodes)
    if nodes <= 0:
        nodes=1
    mem_req_per_node = int(''.join(re.findall(r'\d+', row.ReqMem))) / gibimibi  
    
    if 'n' in row.ReqMem: 
        pass 
    elif 'c' in row.ReqMem:  
        mem_req_per_node = (mem_req_per_node * cpu_request) / nodes
        
        if cpu_request == 1 :
           mem_req_per_node = 2 * mem_req_per_node 
    
    try:
        cpu_request = cpu_request / nodes  
    except: 
        nodes = 1  
        cpu_request = cpu_request / nodes  
    memory_used = row.MaxRSS/gibimibi  
    
    try:
        aws_instance = aws_cost[(aws_cost.vCPU>=cpu_request) & (aws_cost.Memory>=mem_req_per_node)].iloc[0]
        if not pd.isnull(aws_instance.burst):
            if row.Elapsed > aws_instance.burst:  
                aws_instance = aws_cost[((aws_cost.vCPU>=cpu_request) & (aws_cost.Memory>=mem_req_per_node)) & (aws_cost.burstable==False)].iloc[0]
                
    except:
        if row.Elapsed.total_seconds() > 10:  
            print('
            print_cost = True
        
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
    if rt_min < 1:
        rt_min=1.00  
    rt_hours = rt_min/60 
    cost = rt_hours * aws_instance.Per_Hour
    cost = cost * nodes
    if print_cost == True:
        print('AWS_est_cost =  ',cost,'  ',end='')
    
    return cost

def collate_saact(indf):
    column_names=list(indf.columns)
    df = pd.DataFrame(columns=column_names)  
    rootid='xx'
    rowkeep = df[:1]
    indf.drop(indf[indf['Start'] == startdate + 'T00:00:00'].index, inplace = True)
    
    indf['MaxRSS'] = indf['MaxRSS'].map(lambda x: memfix(x))
    indf['MaxVMSize'] = indf['MaxVMSize'].map(lambda x: memfix(x))
    
    indf['Elapsed'] = indf['Elapsed'].map(lambda x: timeformat_lambda(x))
    indf['Timelimit'] = indf['Timelimit'].map(lambda x: timeformat_lambda(x))
    indf['TotalCPU'] = indf['TotalCPU'].map(lambda x: timeformat_lambda(x))
    
    indf['JobID'] = indf['JobID'].map(lambda x: cleanjobid(x))
    df_agg = indf.groupby('JobID').agg({
    'User':lambda x: x.iloc[0],
    'Account': lambda x: x.iloc[0],
    'JobID': lambda x: x.iloc[0],
    'Elapsed': np.max,
    'Timelimit': np.max,
    'Start': lambda x: x.iloc[0],  
    'NNodes': lambda x: x.iloc[0],
    'NTasks': np.max,
    'MaxRSS' : np.max,
    'MaxVMSize' : np.max,
    'Partition': lambda x: x.iloc[0],
    'ReqCPUS': lambda x: x.iloc[0],
    'AllocCPUS': lambda x: x.iloc[0],
    'TotalCPU': np.max,
    'ReqMem': lambda x: x.iloc[0],
    'State': lambda x: x.iloc[0],
    'End': lambda x: x.iloc[0]
    })
    if not df_agg.empty:
        df_agg['aws_cost'] = df_agg.apply(aws_cost_equiv, axis = 1) 
    return df_agg

def user_usage(user,startdate,calcOld=False):    


    t0 = time.time()
    sacct_string = subprocess.run(['sacct --units=M -p -T -S ' + startdate + ' --format="jobid%30,Elapsed%15,Timelimit,Start,NNodes,NCPUS,NTasks,MaxRSS,MaxVMSize,Partition,ReqCPUS,AllocCPUS,TotalCPU%15,CPUtime,ReqMem,State%10,End, User, Account" -u '+user + ' --noconvert ' + '|grep -v ext'],shell=True,stdout=subprocess.PIPE).stdout.decode('utf-8')
    t1 = time.time()
    print('  saact time: ', end='')
    print(t1-t0, end='')
    sacct_stringio=StringIO(sacct_string)
    
    
    if not sacct_string=='':
        df=pd.read_csv(sacct_stringio,sep='|')
    else:
        print(' ### Warning! user:',user,' has been removed from sacct ### ',end='')
        1/0
    newdf=collate_saact(df)
    if calcOld ==True:
        df = df[df.MaxVMSize.notna()] 
        costs = []
        cpu_request_list = []
        mem_request_list = []
        time_taken_hours = []
        start_time_list =[]
        
        for row in df.itertuples():
            try:
                cpu_request = int(row.NCPUS)
            except:
                1/0
            memory_request = row.MaxVMSize/gibimibi
            try:
                aws_instance = aws_cost[(aws_cost.vCPU>=cpu_request) & (aws_cost.Memory>=memory_request)].iloc[0]
            except:
                
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

def totalmem(row):
    '''
    Take a MemReq like 3072Mc or 3072Mn and convert it to total memory requested
    then convert memory to gibibytes
    '''
    if pd.isna(row.ReqMem):
        totalmemreq = np.nan
    elif 'n' in row.ReqMem: 
        totalmemreq = int( row.ReqMem.strip('Mn') ) * row.NNodes
    elif 'c' in row.ReqMem:  
        totalmemreq = int( row.ReqMem.strip('Mc') ) * row.AllocCPUS
    totalmemreq = totalmemreq / gibimibi
    return totalmemreq


# Main script
# Load and preprocess AWS cost data

aws_cost = pd.read_csv('aws_cost_2019.csv')
aws_cost = aws_cost.sort_values(by=['Per_Hour'])

#new cost sheet
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

# Get user list from the cluster
users_string = subprocess.run(['sacctmgr', 'show', 'user', '-P'], stdout=subprocess.PIPE).stdout.decode('utf-8')
usersio = StringIO(users_string)
usersdf = pd.read_csv(usersio, sep='|')
usernames = list(usersdf.User)

# Initialize dataframes for storing job information
all_jobs_df = pd.DataFrame([], index=[0])
all_jobs_newdf = pd.DataFrame([], index=[0])
all_strings = ''

if use_currentdate:
    startdate = datetime.now() - timedelta(days=overlap_length)
    startdate = startdate.strftime("%Y-%m-%d")

    old_df = pd.read_csv('all_jobs.csv')

# Process usage data for each user
for user in usernames:
    userexists = subprocess.run(['sacct', '-u', user], stdout=subprocess.PIPE).stdout.decode('utf-8')

    if userexists != '':
        print(user, end='')
        t0 = time.time()

        all_jobs_f, newdf = user_usage(user, startdate, calcOld=True)
        all_jobs_df = pd.concat([all_jobs_df, all_jobs_f], sort=False)
        all_jobs_newdf = pd.concat([all_jobs_newdf, newdf], sort=False)

        t1 = time.time()
        print('  pycalc time:', end='')
        print(t1 - t0)

    else:
        print(f'{user} not found in sacct output.')

# Save processed job data to CSV
all_jobs_df.to_csv('all_jobs.csv', index=False)
all_jobs_newdf.to_csv('all_jobs_newdf.csv', index=False)
