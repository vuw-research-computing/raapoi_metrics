import pandas as pd
import time
from io import StringIO
from cluster_price_func import collate_saact


# Initialize dataframes for storing job information
all_jobs_df = pd.DataFrame()
all_jobs_newdf = pd.DataFrame()

start_time = time.time()
print("Collecting initial dataframe")
dtype = {8: str, 16: str}  # Columns with mixed types to be set as string
all_jobs_data = pd.read_csv("slurm_data/all_data.csv", sep='|', dtype=dtype)

# Assign column names to the DataFrame
column_names = ['User', 'jobid', 'Elapsed', 'Timelimit', 'Start', 'NNodes', 'NCPUS', 'NTasks', 'MaxRSS',
                   'MaxVMSize', 'Partition', 'ReqCPUS', 'AllocCPUS', 'TotalCPU', 'CPUtime', 'ReqMem', 'AllocGRES',
                   'State', 'End', 'Account']
all_jobs_data.columns = column_names


end_time = time.time()
elapsed_time = end_time - start_time

print(f"\nCreating the intial dataframe took: {elapsed_time:.2f} seconds.")

# Perform heavy processing on the loaded data
# all_jobs_f, newdf = collate_saact(all_jobs_data)
# all_jobs_df = pd.concat([all_jobs_df, all_jobs_f], sort=False)
# all_jobs_newdf = pd.concat([all_jobs_newdf, newdf], sort=False)