import os
import time
import pandas as pd
from plotnine import (
    aes,
    element_text,
    facet_wrap,
    geom_bar,
    geom_col,
    ggplot,
    ggsave,
    guides,
    labs,
    scale_fill_gradient,
    scale_x_date,
    theme,
)
from mizani.formatters import date_format
from typing import Optional

def preprocess_data(df):
    '''
    Load the dataframe
    Do some preprocessing of the input data, such as fixing account allocations
    '''

    # df = pd.read_csv('../dfout_all.csv', dtype={15: str})

    #Fix jiaowa account info to ferrier rather than sbs
    mask = (df['User'] == 'jiaowa') & (df['Account'] == 'sbs')
    df.loc[mask, 'Account'] = 'ferrier'

    # Mapping of old account names to new ones
    account_mapping = {
        'scpslab206': 'scps', 
        'scpslab306': 'scps',
        'spacejam': 'scps',
        'phys414': 'scps',
        'students': 'scps',
        'cad': 'admin',
        'root': 'admin'
    }

    # Replace account names
    df['Account'] = df['Account'].replace(account_mapping)

    # currently we use a fix usd to nzd exchange rate to calculate aws cost
    usd_to_nzd = 1.62
    df.aws_cost = df.aws_cost * usd_to_nzd

    # set dates as datetime and create columns for month and year
    df['Start'] = pd.to_datetime(df['Start'])
    df['Year'] = df['Start'].dt.year
    df['Month'] = df['Start'].dt.month

    return df

def generate_plot(df: pd.DataFrame, x_column: str, title: str, subtitle: str, filename: str, width: Optional[int] = 20) -> None:
    if x_column == 'Year':
        width = 0.7  # A value of 0.7 is commonly used when you have yearly data.
    elif x_column == 'YearMonth':
        width = 20  # This width can be adjusted based on how wide you want the bars to be.

    plot = (
        ggplot(df, aes(x=x_column, y='UniqueUsers', fill='UniqueUsers'))
        + geom_bar(stat='identity', width=width)
        + scale_fill_gradient(low="blue", high="red")
        + labs(x='Date', y='Unique Users', title=title, subtitle=subtitle, fill='UniqueUsers')
        + theme(axis_text_x=element_text(angle=45, hjust=1),  # rotate x-axis labels 45 degrees
                plot_title=element_text(hjust=0.5),  # center title
                plot_subtitle=element_text(hjust=0.5))  # center subtitle
        + guides(fill=False)  # remove color bar
    )

    # Save the plot
    ggsave(plot, filename=filename, format='png', dpi=300)


def plot_unique_users_per_month(df):
    
    # Group by 'Account', 'Year', 'Month' and 'User', then count unique 'User'
    unique_users = df.groupby(['Account', 'Year', 'Month', 'User']).size().reset_index().rename(columns={0:'count'})
    print(unique_users.head())

    # Now group by 'Account', 'Year' and 'Month' and count unique 'User'
    unique_users_per_month = unique_users.groupby(['Account', 'Year', 'Month']).size().reset_index().rename(columns={0:'UniqueUsers'})
    
    # Convert 'Year' and 'Month' to integer, then to string, combine them, and convert to datetime
    unique_users_per_month['YearMonth'] = pd.to_datetime(unique_users_per_month['Year'].astype(int).astype(str) + '-' + unique_users_per_month['Month'].astype(int).astype(str))
   
    # Capitalize 'Account'
    unique_users_per_month['Account'] = unique_users_per_month['Account'].str.upper()

    accounts = unique_users_per_month['Account'].unique()

    # Create the directory if it doesn't already exist
    if not os.path.exists('plots/monthly_users'):
        os.makedirs('plots/monthly_users')

    for account in accounts:
        account_data = unique_users_per_month[unique_users_per_month['Account'] == account]
        generate_plot(account_data, 'YearMonth', 'Rāpoi', f'Unique {account} Users Per Month', f'plots/monthly_users/{account}_users_per_month.png')
        
    # Produce the total unique users per month
    start_time = time.time()
    # For the total unique users per month, group the original DataFrame by Year and Month and sum the unique users
    total_users_per_month = unique_users_per_month.groupby(['YearMonth'])['UniqueUsers'].sum().reset_index()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print('Creating total unique users took:', elapsed_time, 'seconds')

    generate_plot(total_users_per_month, 'YearMonth', 'Rāpoi', 'Total Unique Users Per Month', 'plots/monthly_users/total_users_per_month.png')

def plot_unique_users_per_year(df):
    unique_users_per_year = df.groupby(['Account', 'Year', 'User']).size().reset_index().rename(columns={0:'count'})

    # Now group by 'Account' and 'Year' and count unique 'User'
    unique_users_per_year = unique_users_per_year.groupby(['Account', 'Year']).size().reset_index().rename(columns={0:'UniqueUsers'})
    accounts = unique_users_per_year['Account'].unique()

    # Create the directory if it doesn't already exist
    if not os.path.exists('plots/yearly_users'):
        os.makedirs('plots/yearly_users/')

    for account in accounts:
        account_data = unique_users_per_year[unique_users_per_year['Account'] == account]
        generate_plot(account_data, 'Year', 'Rāpoi', f'Unique {account} Users Per Year', f'plots/yearly_users/{account}_users_per_year.png')
    

    # Produce the total unique users per year
    start_time = time.time()
    # For the total unique users per year, group the original DataFrame by Year and Month and sum the unique users
    total_users_per_year = unique_users_per_year.groupby(['Year'])['UniqueUsers'].sum().reset_index()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print('Creating total unique users took:', elapsed_time, 'seconds')

    # For total users
    generate_plot(total_users_per_year, 'Year', 'Rāpoi', 'Total Unique Users Per Year', 'plots/yearly_users/total_users_per_year.png')



def plot_costs_per_year(df):
    # Group by 'Account' and 'Year' and sum 'aws_cost' and 'nesi_cost'
    cost_per_year = df.groupby(['Account', 'Year']).agg({'aws_cost': 'sum', 'nesi_cost': 'sum'}).reset_index()

    # Convert 'Year' to integer, then to string, and convert to datetime
    cost_per_year['Year'] = pd.to_datetime(cost_per_year['Year'].astype(int).astype(str))

    

    # Capitalize 'Account'
    cost_per_year['Account'] = cost_per_year['Account'].str.upper()

    accounts = cost_per_year['Account'].unique()

    # Ensure the directories exist
    os.makedirs('plots/yearly_costs/aws/', exist_ok=True)
    os.makedirs('plots/yearly_costs/nesi/', exist_ok=True)

    for account in accounts:
        account_data = cost_per_year[cost_per_year['Account'] == account]

        for cost_type in ['aws_cost', 'nesi_cost']:
            cost_title = 'AWS cost' if cost_type == 'aws_cost' else 'NeSi cost'
            cost_subtitle = 'Based on 2020 best matched instance for given core count' if cost_type == 'aws_cost' else ' '
            save_folder = 'plots/yearly_costs/aws/' if cost_type == 'aws_cost' else 'plots/yearly_costs/nesi/'

            plot = (
                ggplot(account_data, aes(x='Year', y=cost_type, fill=cost_type))
                + geom_col()  # using geom_col instead of geom_bar with stat='identity'
                + scale_fill_gradient(low = "blue", high = "red")
                + labs(x='Year', y='Cost', title=f'{cost_title} for {account} Per Year', subtitle=cost_subtitle, fill=cost_type)
                + theme(axis_text_x = element_text(angle = 45, hjust = 1),  # rotate x-axis labels 45 degrees
                        plot_title=element_text(hjust=0.5),  # center title
                        plot_subtitle=element_text(hjust=0.5))  # center subtitle
                + guides(fill=False)  # remove color bar
                + scale_x_date(date_breaks='1 year', labels=date_format('%Y'))  # set x-axis breaks and labels
            )
            
            print(plot)

            # Save the plot
            plot.save(f"{save_folder}{account}_{cost_type}.png")


def plot_costs_per_month(df):
    # Group by 'Account', 'Year' and 'Month' and sum 'aws_cost' and 'nesi_cost'
    cost_per_month = df.groupby(['Account', 'Year', 'Month']).agg({'aws_cost': 'sum', 'nesi_cost': 'sum'}).reset_index()

    # Convert 'Year' and 'Month' to integer, then to string, combine them, and convert to datetime
    cost_per_month['YearMonth'] = pd.to_datetime(cost_per_month['Year'].astype(int).astype(str) + '-' + cost_per_month['Month'].astype(int).astype(str))

    accounts = cost_per_month['Account'].unique()

    # Capitalize 'Account'
    cost_per_month['Account'] = cost_per_month['Account'].str.upper()

    # Ensure the directories exist
    os.makedirs('plots/monthly_costs/aws/', exist_ok=True)
    os.makedirs('plots/monthly_costs/nesi/', exist_ok=True)

    for account in accounts:
        account_data = cost_per_month[cost_per_month['Account'] == account]

        for cost_type in ['aws_cost', 'nesi_cost']:
            cost_title = 'AWS cost' if cost_type == 'aws_cost' else 'NeSi cost'
            cost_subtitle = 'Based on 2020 best matched instance for given core count' if cost_type == 'aws_cost' else ' '
            save_folder = 'plots/monthly_costs/aws/' if cost_type == 'aws_cost' else 'plots/monthly_costs/nesi/'

            plot = (
                ggplot(account_data, aes(x='YearMonth', y=cost_type, fill=cost_type))
                + geom_bar(stat='identity', width=20)  # adjust the width as needed
                + scale_fill_gradient(low = "blue", high = "red")
                + labs(x='Date', y='Cost', title=f'{cost_title} for {account} Per Month', subtitle=cost_subtitle, fill=cost_type)
                + theme(axis_text_x = element_text(angle = 45, hjust = 1),  # rotate x-axis labels 45 degrees
                        plot_title=element_text(hjust=0.5),  # center title
                        plot_subtitle=element_text(hjust=0.5))  # center subtitle
                + guides(fill=False)  # remove color bar
            )
            
            print(plot)

            # Save the plot
        plot.save(f"{save_folder}{account}_{cost_type}.png")


def plot_all_slurm():
    '''
    Preprocess and then plot all the raapoi user and estimated cost data.
    '''
    #df = pd.read_csv('/nfs/scratch/admduggalro/raapoi_metrics/raapoi_data.csv', dtype={15: str})
    df = pd.read_csv('/home/duggalrohi/private/demo_rohit/raapoi_metrics/raapoi_data.csv')
    df = preprocess_data(df)

    plot_unique_users_per_month(df)
    plot_unique_users_per_year(df)
    plot_costs_per_year(df)
    plot_costs_per_month(df)

plot_all_slurm()