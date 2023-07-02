import os
import subprocess
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time

start_date = datetime(2023, 5, 1)  # Replace with your desired start date
end_date = datetime.now()  # Replace with your desired end date
force_retrieve = False  # Set to True to force re-gathering the data

current_start_date = start_date
current_end_date = current_start_date + relativedelta(months=1)

while current_start_date < end_date:
    start_date_str = current_start_date.strftime("%Y-%m-%d")
    end_date_str = current_end_date.strftime("%Y-%m-%d")
    month_str = current_start_date.strftime("%Y-%m")

    complete_filepath = f"slurm_data/{month_str}_complete.csv"
    incomplete_filepath = f"slurm_data/{month_str}_incomplete.csv"

    if not os.path.exists("slurm_data"):
        os.makedirs("slurm_data")

    if not os.path.exists(complete_filepath) or force_retrieve:
        print('Starting date:', start_date_str)
        
        start_time = time.time()
        job_data_str = subprocess.run(['sacct', '-S', start_date_str, '-E', end_date_str, '--noheader', '--units=M', '--state=BF,CA,CD,DL,F,NF,OOM,PR,TO', '--parsable2', '--allusers', '--format=User,jobid,Elapsed,Timelimit,Start,NNodes,NCPUS,NTasks,MaxRSS,MaxVMSize,Partition,ReqCPUS,AllocCPUS,TotalCPU,CPUtime,ReqMem,AllocGRES,State,End,Account'], stdout=subprocess.PIPE).stdout.decode('utf-8')
        end_time = time.time()
        elapsed_time = end_time - start_time
        print('Query took:', elapsed_time, 'seconds')

        rows = job_data_str.split('\n')  # Split the string into rows

        if current_end_date < end_date:
            # Write to complete file if the month is complete
            with open(complete_filepath, 'w') as file:
                file.write('\n'.join(rows))
            print(f'Saved complete data for {month_str}')
        else:
            # Write to incomplete file if the month is incomplete
            with open(incomplete_filepath, 'w') as file:
                file.write('\n'.join(rows))
            print(f'Saved incomplete data for {month_str}')
    else:
        print(f'Skipping {month_str}, complete data already exists')

    # Update the start and end dates for the next iteration
    current_start_date = current_end_date
    current_end_date += relativedelta(months=1)
