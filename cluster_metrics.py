import pandas as pd
from functions.process_collate import multiCollate
from functions.slurm_data_collection import merge_slurm_data, grab_slurm_data
from functions.raapoi_plots import plot_all_slurm

if __name__ == '__main__':
    # multiCollate()
    plot_all_slurm()
