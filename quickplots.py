import pandas as pd
from functions.raapoi_plots import preprocess_data, plot_unique_users_per_month, plot_unique_users_per_year


def main():
    df = pd.read_csv('dfout_all.csv', dtype={15: str})
    df = preprocess_data(df)
    plot_data_month(df)
    plot_data_year(df)

if __name__ == "__main__":
    main()