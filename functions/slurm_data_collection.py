import os
import subprocess
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
import csv

start_date = datetime(2023, 5, 1)  # Replace with your desired start date
end_date = datetime.now()  # Replace with your desired end date
force_retrieve = False  # Set to True to force re-gathering the data


def grab_slurm_data(start_date, end_date, force_retrieve):
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
            print(' ')

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


def merge_slurm_data():

    output_file = "slurm_data/all_data.csv"  # Output file path
    directory = "slurm_data"  # Directory containing the monthly CSV files

    # Get a list of all CSV files in the directory, sorted by name (date)
    csv_files = sorted([file for file in os.listdir(directory) if file.endswith(".csv")])

    start_time = time.time()

    # Check for any "complete" files and remove corresponding "incomplete" files
    for csv_file in csv_files:
        # Remove .csv extension and split at the last underscore
        month_str, status = os.path.splitext(csv_file)[0].rsplit("_", 1)
        status = status.rstrip('.csv')  # remove '.csv' from status
        if status == "complete":
            incomplete_file_path = os.path.join(directory, f"{month_str}_incomplete.csv")
            if os.path.exists(incomplete_file_path):
                os.remove(incomplete_file_path)
                print(f"Removed file: {incomplete_file_path}")

    # Refresh the list of CSV files after removing "incomplete" files
    csv_files = sorted([file for file in os.listdir(directory) if file.endswith(".csv")])

    # Now proceed to join files
    with open(output_file, "w") as outfile:
        for index, csv_file in enumerate(csv_files):
            if csv_file == "all_data.csv":
                continue  # Skip the output file itself

            file_path = os.path.join(directory, csv_file)
            with open(file_path, "r") as infile:
                outfile.write(infile.read())

            # Print the order of the file being processed
            print(f"Processing file {index+1}/{len(csv_files)}: {csv_file}")

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"\nJoining the files took: {elapsed_time:.2f} seconds.")

def split_data_nprocs():
    # Define the path to your data and the number of processes
    data_path = "slurm_data/all_data.csv"
    nprocs = 100 #Define your number of processes

    # Define column names
    column_names = ['User', 'jobid', 'Elapsed', 'Timelimit', 'Start', 'NNodes', 'NCPUS', 'NTasks', 'MaxRSS',
                'MaxVMSize', 'Partition', 'ReqCPUS', 'AllocCPUS', 'TotalCPU', 'CPUtime', 'ReqMem', 'AllocGRES',
                'State', 'End', 'Account']

    # Get the total number of lines in the file
    start_time = time.time()
    with open(data_path) as f:
        total_lines = sum(1 for line in f)
    end_time = time.time()

    print(f"Time taken to open original file: {end_time - start_time} seconds")

    # Calculate number of lines per file
    lines_per_file = total_lines // nprocs

    # Create output directory
    output_dir = "nprocs_split"
    os.makedirs(output_dir, exist_ok=True)

    # Open the input CSV file
    with open(data_path, 'r') as input_csv:
        reader = csv.reader(input_csv, delimiter='|')

        current_file_num = 0
        current_line_num = 0
        current_file = None
        writer = None

        # Keep track of the previous jobid
        prev_jobid = None

        # Loop through each row
        for row in reader:
            jobid = row[column_names.index('jobid')]

            # If we've written enough lines to this file and this jobid is different from the last, 
            # or if it's the first line of the file
            if current_line_num >= lines_per_file and "." not in jobid and jobid != prev_jobid or current_file is None:

                # Close the current file if it's open
                if current_file is not None:
                    current_file.close()

                # Open a new file
                current_file_num += 1
                current_file = open(f'{output_dir}/split_{current_file_num}.csv', 'w')
                writer = csv.writer(current_file, delimiter='|')
                print(f"Writing to file: {output_dir}/split_{current_file_num}.csv")

                current_line_num = 0

            # Write the row to the current file
            writer.writerow(row)
            current_line_num += 1
            prev_jobid = jobid

        # Close the last file if it's open
        if current_file is not None:
            current_file.close()

    print("All files have been written successfully.")