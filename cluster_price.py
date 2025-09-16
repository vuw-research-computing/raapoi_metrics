import pandas as pd
import numpy as np
from .aws_cost import aws_cost_equiv
from .aws_cost import prepare_aws_cost_data

def memfix(inmem):
    if pd.isnull(inmem):
        outmem = 0
    else:
        outmem = float(inmem.strip('M'))
    return outmem


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

def cleanjobid(jobid):
    jobid = str(jobid)
    jobid = jobid.split('.', 1)[0]
    return jobid

# Fix non-numeric NTasks
def fix_ntasks(val):
    if pd.isnull(val):
        return 0
    try:
        # Remove commas or stray whitespace and convert to int
        return int(str(val).replace(',', '').strip())
    except:
        return 0  # Fallback for badly formatted strings

def collate_saact(jobs_data, aws_cost_data):
    # Preprocessing steps from collate_saact function
    column_names = list(jobs_data.columns)
    df = pd.DataFrame(columns=column_names)
    rootid = 'xx'
    rowkeep = df[:1]

    # Preprocessing steps from user_usage function
    # use the loc accessor to modify the DataFrame in place
    jobs_data.loc[:, 'MaxRSS'] = jobs_data['MaxRSS'].map(lambda x: memfix(x))
    jobs_data.loc[:, 'MaxVMSize'] = jobs_data['MaxVMSize'].map(lambda x: memfix(x))

    jobs_data.loc[:, 'Elapsed'] = jobs_data['Elapsed'].map(lambda x: timeformat_lambda(x))
    jobs_data.loc[:, 'Timelimit'] = jobs_data['Timelimit'].map(lambda x: timeformat_lambda(x))
    jobs_data.loc[:, 'TotalCPU'] = jobs_data['TotalCPU'].map(lambda x: timeformat_lambda(x))

    jobs_data.loc[:, 'JobID'] = jobs_data['JobID'].map(lambda x: cleanjobid(x))
    jobs_data.loc[:, 'NTasks'] = jobs_data['NTasks'].map(lambda x: fix_ntasks(x))


    # Aggregating and processing as in the original collate_saact function
    df_agg = jobs_data.groupby('JobID').agg({
        'User': lambda x: x.iloc[0],
        'Account': lambda x: x.iloc[0],
        'JobID': lambda x: x.iloc[0],
        'Elapsed': 'max',
        'Timelimit': 'max',
        'Start': lambda x: x.iloc[0],
        'NNodes': lambda x: x.iloc[0],
        'NTasks': 'max',
        'MaxRSS': 'max',
        'MaxVMSize': 'max',
        'Partition': lambda x: x.iloc[0],
        'ReqCPUS': lambda x: x.iloc[0],
        'AllocCPUS': lambda x: x.iloc[0],
        'TotalCPU': 'max',
        'ReqMem': lambda x: x.iloc[0],
        'State': lambda x: x.iloc[0],
        'End': lambda x: x.iloc[0],
        'Submit': lambda x: x.iloc[0],
    })

    if not df_agg.empty:
        # aws_cost_data = prepare_aws_cost_data()
        df_agg[['aws_cost', 'nesi_cost']] = df_agg.apply(lambda row: aws_cost_equiv(row, aws_cost_data), axis=1)
    return jobs_data, df_agg
