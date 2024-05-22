#!/bin/bash -x

#SBATCH --job-name=gather_data          # Job name
#SBATCH --output=slurm.out    # Standard output and error log
#SBATCH --error=slurm.err     # Error log
#SBATCH --partition=quicktest                 # Partition (queue) name
#SBATCH --ntasks=1                         # Run on a single CPU
#SBATCH --cpus-per-task=100                # Number of CPU cores per task
#SBATCH --mem=100G                        # Job memory request
#SBATCH --time=00:30:00                    # Time limit hrs:min:sec

module purge 
#module load GCCcore/10.3.0
module load Python/3.6.8
# Load Python 3.8.1
#module load python/3.6.8

# Activate your Python environment
source /nfs/home/admduggalro/raapoi-users/env/bin/activate

# Run the Python script
#python slurm_data_collection.py
python process_collate.py
