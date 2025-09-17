#!/bin/bash

#SBATCH --job-name=csv_processing          # Job name
#SBATCH --output=_csv_processing_out.log    # Standard output and error log
#SBATCH --error=_csv_processing_err.log     # Error log
#SBATCH --partition=bigmem                 # Partition (queue) name
#SBATCH --ntasks=1                         # Run on a single CPU
#SBATCH --cpus-per-task=100                # Number of CPU cores per task
#SBATCH --mem=1000G                        # Job memory request
#SBATCH --time=00:30:00                    # Time limit hrs:min:sec

# Load Python 3.8.1
module load python/3.8.1

# Activate your Python environment
source ../env/bin/activate

# Run the Python script
python3 cluster_raw_to_dataframe.py
