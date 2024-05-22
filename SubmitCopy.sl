#!/bin/bash -x
#SBATCH --job-name=file_transfer
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4     # Adjust the number of CPUs as needed
#SBATCH --mem-per-cpu=4G      # Adjust the memory requirement per CPU as needed
#SBATCH --time=1:00:00        # Adjust the time limit as needed
#SBATCH --output=file_transfer_.out
#SBATCH --error=file_transfer_.err

# Load any necessary modules
module purge
module load Python/3.6.8

source ~/raapoi-users/env/bin/activate
# Execute the Python script
python CopyFile.py

