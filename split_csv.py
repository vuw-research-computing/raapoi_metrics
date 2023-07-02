import csv
import os
import time

# Define the path to your data and the number of processes
data_path = "slurm_data/all_data.csv"
nprocs = #Define your number of processes

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

    # Loop through each row
    for row in reader:
        jobid = row[column_names.index('jobid')]

        # If we've written enough lines to this file or if the current jobid contains "."
        if current_line_num >= lines_per_file or "." in jobid:
            print(f"Adjusting split index to avoid splitting job {jobid}")

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

    # Close the last file if it's open
    if current_file is not None:
        current_file.close()

print("All files have been written successfully.")
