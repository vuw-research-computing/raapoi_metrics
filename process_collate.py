import pandas as pd
import os
from multiprocessing import Pool, cpu_count
from functions.cluster_price import collate_saact
import time

from functions.aws_cost import aws_cost_equiv
from functions.aws_cost import prepare_aws_cost_data

def process_file(filename, aws_cost_data):
    df1 = pd.read_csv(f'nprocs_split/{filename}', header=None, delimiter="|", dtype={8: str, 16: str})

    column_names = ['User', 'JobID', 'Elapsed', 'Timelimit', 'Start', 'NNodes', 'NCPUS', 'NTasks', 'MaxRSS',
                'MaxVMSize', 'Partition', 'ReqCPUS', 'AllocCPUS', 'TotalCPU', 'CPUtime', 'ReqMem',
                'State', 'End', 'Account']
    df1.columns = column_names
    
    dfout_test, dfout = collate_saact(df1, aws_cost_data)

    return dfout_test, dfout

def multiCollate(linear=False):
    '''
    Run as a slurm job - multi processes the nprocs_split slurm data files into a single csv.
    The processing includes estimating an equivalent AWS and NeSi cost
    '''
    # Get a list of all the csv files
    files = [f for f in os.listdir('nprocs_split') if f.endswith('.csv')]

    # Get the number of CPUs available from SLURM
    n_cpus = int(os.getenv('SLURM_CPUS_PER_TASK', default=os.cpu_count()))
    
    # Prepare aws cost sheet
    aws_cost_data = prepare_aws_cost_data()

    start_time = time.time()
    results = []   # Initialize results list
    if linear==False:
        # Create a multiprocessing Pool
        pool = Pool(processes=n_cpus)
        # Use the pool to process the files in parallel
        results = pool.starmap(process_file, [(f, aws_cost_data) for f in files])
    else:
        for f in files:
            result = process_file(f, aws_cost_data)
            results.append(result)  # Append results from each file to the results list

    end_time = time.time()
    elapsed_time = end_time - start_time
    print('Processing took:', elapsed_time, 'seconds')

    # Separate out the two dataframes from results
    #dfout_test_all = pd.concat([res[0] for res in results])
    dfout_all = pd.concat([res[1] for res in results])

    # Save the combined dataframes
    #dfout_test_all.to_csv('dfout_test_all.csv', index=False)
    dfout_all.to_csv('raapoi_data.csv', index=False)


if __name__ == '__main__':
    multiCollate(linear=False)
    #multiCollate(linear=True)
    print('Done')
