import pandas as pd
from functions.process_collate import multiCollate
from functions.slurm_data_collection import merge_slurm_data, grab_slurm_data
from functions.raapoi_plots import preprocess_data, plot_unique_users_per_month, plot_unique_users_per_year, plot_costs_per_year, plot_costs_per_month



def plot_slurm():
    df = pd.read_csv('raapoi_data.csv', dtype={15: str})
    df = preprocess_data(df)
    plot_unique_users_per_month(df)
    plot_unique_users_per_year(df)
    plot_costs_per_year(df)
    plot_costs_per_month(df)


if __name__ == '__main__':
    # multiCollate()
    merge_slurm_data()
