import csv
import os
import time

def split_csv(csv_file, nprocs, output_dir):
  """Splits a CSV file into nprocs files based on the jobid column.

  Args:
    csv_file: The path to the CSV file to split.
    nprocs: The number of files to split the CSV file into.
    output_dir: The directory to put the split CSV files in.

  Returns:
    A list of the paths to the split CSV files.
  """

  start_time = time.time()

  with open(csv_file, "r") as csvfile:
    reader = csv.reader(csvfile, delimiter="|")
    jobids = {}
    for row in reader:
      jobid = row[1]
      if "." in jobid:
        jobid = jobid.split(".")[0]
      if jobid not in jobids:
        jobids[jobid] = []
      jobids[jobid].append(row)

    # Create the split CSV files.
    split_files = []
    for i in range(nprocs):
      split_file = os.path.join(output_dir, f"split_{i}.csv")
      with open(split_file, "w") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        for row in jobids.values():
          writer.writerows(row)
      split_files.append(split_file)

  end_time = time.time()
  print(f"Time to split CSV: {end_time - start_time}")

  return split_files


if __name__ == "__main__":
  csv_file = "slurm_data/all_data.csv"
  nprocs = 4
  output_dir = "nprocs_split"
  split_files = split_csv(csv_file, nprocs, output_dir)
  print(split_files)
