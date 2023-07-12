from functions.process_collate import multiCollate
from functions.slurm_data_collection import merge_slurm_data, grab_slurm_data, split_data_nprocs
from functions.raapoi_plots import plot_all_slurm


if __name__ == '__main__':
    # multiCollate()
    plot_all_slurm()
