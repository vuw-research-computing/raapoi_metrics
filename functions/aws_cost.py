import re
import subprocess
import time
from datetime import datetime, timedelta
from io import StringIO

import os

import numpy as np
import pandas as pd

gibikibi = 1048576
mibikibi = 1024
gibimibi = 1024

A100_cost = 32.77/8  # cost of A100 based on 2023 A100 cost of p4d.24xlarge on aws



def burstfinder(invcpu):
    vcpu_params = re.split('vCPUs|for a|burst', invcpu)
    if len(vcpu_params) == 4:
        burst = vcpu_params.strip()
    else:
        burst = pd.to_timedelta('nan')
    return burst

def prepare_aws_cost_data():
    # get the directory where this script is located
    current_dir = os.path.dirname(os.path.realpath(__file__))

    # construct the file path to the csv file
    csv_file_path = os.path.join(current_dir, '../reference_data/Amazon EC2 Instance ComparisonJune2020.csv')
    # aws_cost = pd.read_csv(csv_file_path)
    # aws_cost = aws_cost.sort_values(by=['Per_Hour'])
    aws_cost = pd.read_csv(csv_file_path)
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

def aws_cost_equiv(row,aws_cost):
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
            print('### Warning jobid',row.JobID,' Does not fit an aws instance for costing, dubious measures ensue!### ',end='')
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

    #gpu instance check - currently assumes all GPUS are A100's!
    if 'gpu' in row['AllocGRES']:
        gpu_num = int(row['AllocGRES'].split(':')[1])

        cost = rt_hours * A100_cost * gpu_num 
    else:
        gpu_num = None
        cost = rt_hours * aws_instance.Per_Hour


    cost = rt_hours * aws_instance.Per_Hour
    cost = cost * nodes


    if print_cost == True:
        print('AWS_est_cost =  ',cost,'  ',end='')
    
    return cost