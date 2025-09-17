#!/bin/bash -x

#SBATCH --job-name=csv_processing          # Job name
#SBATCH --output=_csv_processing_out.log    # Standard output and error log
#SBATCH --error=_csv_processing_err.log     # Error log
#SBATCH --partition=quicktest                 # Partition (queue) name
#SBATCH --ntasks=1                         # Run on a single CPU
#SBATCH --cpus-per-task=10                # Number of CPU cores per task
#SBATCH --mem=2G                        # Job memory request
#SBATCH --time=00:30:00                    # Time limit hrs:min:sec

# Load Python 3.8.1
module purge
module load Python/3.8.1

# Activate your Python environment
source ~/raapoi-users/env/bin/activate

# Run the Python script
python cluster_raw_to_dataframe.py
