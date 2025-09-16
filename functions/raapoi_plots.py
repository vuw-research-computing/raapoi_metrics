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
    df['Submit'] = pd.to_datetime(df['Submit'])
    df['End'] = pd.to_datetime(df['End'])
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

############################################################
# Plot wait times for jobs to start from submit time with y axis as time in seconds and x axis as the number of jobs
import pandas as pd
from plotnine import (
    ggplot,
    aes,
    geom_bar,
    geom_col, # geom_col is suitable for pre-summarized data
    scale_fill_gradient,
    labs,
    theme,
    element_text,
    guides,
)
import os # Import os for directory creation

def plot_submit_start_time(df: pd.DataFrame):
    """
    Orchestrates plotting of:
    1. Total wait time per month for each account (jobs >= 4 hours wait time).
    2. Total wait time for each unique job (jobs >= 4 hours wait time).

    Args:
        df (pd.DataFrame): DataFrame containing 'Start', 'Submit', 'Account', and 'JobID' columns.
    """

    # Create a copy to avoid modifying the original DataFrame passed into the function
    df_copy = df.copy()

    # --- Plotting Wait Time Per Account Per Month ---

    # Ensure 'Start' and 'Submit' columns are datetime objects
    df_copy['Start'] = pd.to_datetime(df_copy['Start'], errors='coerce')
    df_copy['Submit'] = pd.to_datetime(df_copy['Submit'], errors='coerce')

    # Drop rows where 'Start' or 'Submit' could not be converted to datetime
    df_copy.dropna(subset=['Start', 'Submit'], inplace=True)

    # Calculate the difference between 'Start' and 'Submit' in seconds
    df_copy['WaitTime'] = (df_copy['Start'] - df_copy['Submit']).dt.total_seconds()

    # Only keep rows where 'WaitTime' is greater than or equal to 4 hours (14400 seconds)
    df_filtered_accounts = df_copy[df_copy['WaitTime'] >= 14400]

    if df_filtered_accounts.empty:
        print("No data meets the criteria (WaitTime >= 4 hours) for per-account plotting.")
    else:
        # Convert 'WaitTime' from seconds to hours for better readability on the plot
        df_filtered_accounts['WaitTime'] = df_filtered_accounts['WaitTime'] / 3600

        # Extract Year and Month from the 'Submit' time
        df_filtered_accounts['Year'] = df_filtered_accounts['Submit'].dt.year
        df_filtered_accounts['Month'] = df_filtered_accounts['Submit'].dt.month

        # Group by 'Account', 'Year', and 'Month' and sum the 'WaitTime'
        wait_time_per_month = df_filtered_accounts.groupby(['Account', 'Year', 'Month']).agg(
            WaitTime=('WaitTime', 'sum')
        ).reset_index()

        # Create 'YearMonth' column as a datetime object for chronological plotting
        wait_time_per_month['YearMonth'] = pd.to_datetime(
            wait_time_per_month['Year'].astype(str)
            + '-'
            + wait_time_per_month['Month'].astype(str)
            + '-01'
        )

        # Get a list of unique accounts to iterate through and generate a plot for each
        accounts = wait_time_per_month['Account'].unique()

        # Create the directory for saving plots if it doesn't exist
        plots_dir_accounts = "plots/wait_times_per_account"
        os.makedirs(plots_dir_accounts, exist_ok=True)

        # Loop through each unique account to create and save a plot
        for account in accounts:
            account_data = wait_time_per_month[
                wait_time_per_month['Account'] == account
            ].copy()

            plot_account = (
                ggplot(account_data, aes(x='YearMonth', y='WaitTime', fill='WaitTime'))
                + geom_bar(stat='identity', width=20)
                + scale_fill_gradient(low="blue", high="red")
                + labs(
                    x='Date',
                    y='Wait Time (hours)',
                    title=f'Wait Time for {account} Per Month',
                    subtitle='Total Wait Time Per Month (Jobs >= 4 hours Wait Time)',
                    fill='WaitTime'
                )
                + theme(
                    axis_text_x=element_text(angle=45, hjust=1),
                    plot_title=element_text(hjust=0.5),
                    plot_subtitle=element_text(hjust=0.5)
                )
                + guides(fill=False)
            )

            print(f"Generating plot for account: {account}")
            print(plot_account)
            plot_filename_account = os.path.join(plots_dir_accounts, f"{account}_wait_time_per_month.png")
            plot_account.save(plot_filename_account)
            print(f"Saved plot: {plot_filename_account}")


    # --- Plotting Total Wait Time Per Job ---

    # We reuse the df_copy that has already been converted to datetime and dropped NaT
    # Calculate the difference between 'Start' and 'Submit' in seconds
    # (re-calculate if df_filtered_accounts modified df_copy, but here it's on a copy)
    df_copy['WaitTime'] = (df_copy['Start'] - df_copy['Submit']).dt.total_seconds()

    # Only keep rows where 'WaitTime' is greater than or equal to 4 hours (14400 seconds)
    df_filtered_jobs = df_copy[df_copy['WaitTime'] >= 14400]

    if df_filtered_jobs.empty:
        print("No data meets the criteria (WaitTime >= 4 hours) to plot total wait time per job.")
        return

    # Convert 'WaitTime' from seconds to hours for better readability
    df_filtered_jobs['WaitTime'] = df_filtered_jobs['WaitTime'] / 3600

    # Group by 'JobID' and sum the 'WaitTime'
    total_wait_time_per_job = df_filtered_jobs.groupby('JobID').agg(
        TotalWaitTime=('WaitTime', 'sum')
    ).reset_index()

    if total_wait_time_per_job.empty:
        print("No job data after aggregation to plot total wait time per job.")
        return

    # --- IMPORTANT FIX: Order the 'JobID' categories explicitly ---
    # Sort the DataFrame by TotalWaitTime in ascending order
    total_wait_time_per_job = total_wait_time_per_job.sort_values(
        by='TotalWaitTime', ascending=True
    )
    # Convert 'JobID' column to an ordered categorical type based on the sorted order.
    # plotnine will then respect this order on the x-axis, achieving the 'reorder' effect.
    total_wait_time_per_job['JobID'] = pd.Categorical(
        total_wait_time_per_job['JobID'],
        categories=total_wait_time_per_job['JobID'], # Use the current order as the categories
        ordered=True
    )

    # Create the directory for saving plots if it doesn't exist
    plots_dir_jobs = "plots/total_job_wait_times"
    os.makedirs(plots_dir_jobs, exist_ok=True)

    # Create the plot using plotnine
    plot_jobs = (
        ggplot(total_wait_time_per_job, aes(x='JobID', y='TotalWaitTime', fill='TotalWaitTime'))
        + geom_col(width=0.7)
        + scale_fill_gradient(low="blue", high="red")
        + labs(
            x='Job ID',
            y='Total Wait Time (hours)',
            title='Total Wait Time Per Job',
            subtitle='Sum of Wait Times (Jobs >= 4 hours Wait Time)',
            fill='TotalWaitTime'
        )
        + theme(
            axis_text_x=element_text(angle=90, hjust=1),
            plot_title=element_text(hjust=0.5),
            plot_subtitle=element_text(hjust=0.5)
        )
        + guides(fill=False)
    )

    print("Generating plot for total wait time per job:")
    print(plot_jobs)
    plot_filename_jobs = os.path.join(plots_dir_jobs, "total_wait_time_per_job.png")
    plot_jobs.save(plot_filename_jobs)
    print(f"Saved plot: {plot_filename_jobs}")



#########################


##################################


import pandas as pd
import numpy as np
from plotnine import (
    ggplot,
    aes,
    geom_col,
    scale_fill_gradient,
    labs,
    theme,
    element_text,
    guides,
)
import os
import concurrent.futures
import traceback

def _process_df_chunk(chunk_df):
    """
    Helper function to process a chunk of the DataFrame in parallel.
    Performs datetime conversion, wait time calculation, and initial filtering.

    Args:
        chunk_df (pd.DataFrame): A chunk of the original DataFrame.

    Returns:
        pd.DataFrame: Processed chunk with 'WaitTime' and relevant columns, or an empty DataFrame if no data meets criteria.
    """
    try:
        # Explicitly create a deep copy to prevent SettingWithCopyWarning
        chunk_df = chunk_df.copy(deep=True)

        # Ensure 'Start' and 'Submit' columns are datetime objects
        chunk_df['Start'] = pd.to_datetime(chunk_df['Start'], errors='coerce')
        chunk_df['Submit'] = pd.to_datetime(chunk_df['Submit'], errors='coerce')

        # Drop rows where 'Start' or 'Submit' could not be converted to datetime (i.e., they are NaT)
        chunk_df.dropna(subset=['Start', 'Submit'], inplace=True)

        # Calculate the difference between 'Start' and 'Submit' in seconds
        chunk_df['WaitTime'] = (chunk_df['Start'] - chunk_df['Submit']).dt.total_seconds()

        # Only keep rows where 'WaitTime' is greater than or equal to 4 hours (14400 seconds)
        chunk_df = chunk_df[chunk_df['WaitTime'] >= 14400]

        # Convert 'WaitTime' from seconds to hours for better readability
        chunk_df['WaitTime'] = chunk_df['WaitTime'] / 3600

        # Select only the necessary columns ('JobID' and 'WaitTime')
        # This reduces memory usage when returning processed chunks
        return chunk_df[['JobID', 'WaitTime']]

    except Exception as e:
        print(f"Error processing DataFrame chunk: {e}")
        traceback.print_exc()
        return pd.DataFrame() # Return empty DataFrame on error


def plot_total_wait_time_per_jobid_multiprocessed(df, num_chunks=None, plot_dpi=150):
    """
    Plots the total wait time for each unique JobID, using multiprocessing
    for faster data preprocessing and aggregation.

    Args:
        df (pd.DataFrame): DataFrame containing 'Start', 'Submit', and 'JobID' columns.
        num_chunks (int, optional): Number of chunks to split the DataFrame into for
                                    parallel processing. If None, defaults to os.cpu_count().
        plot_dpi (int, optional): Resolution of the saved plot image in dots per inch.
                                  Lower values (e.g., 96, 150) result in smaller files and faster saving.
                                  Defaults to 150.
    """

    print("Starting data preprocessing with multiprocessing...")

    if num_chunks is None:
        num_chunks = os.cpu_count() # Use all available CPU cores by default

    # Split the DataFrame into chunks for parallel processing
    # Using numpy.array_split for more even distribution, especially for smaller DataFrames
    df_chunks = [chunk for chunk in np.array_split(df, num_chunks) if not chunk.empty]

    processed_chunks = []
    # Use ProcessPoolExecutor to parallelize chunk processing
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_chunks) as executor:
        futures = [executor.submit(_process_df_chunk, chunk) for chunk in df_chunks]

        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                processed_chunk = future.result()
                if not processed_chunk.empty:
                    processed_chunks.append(processed_chunk)
                print(f"Finished processing chunk {i+1}/{len(df_chunks)}.")
            except Exception as exc:
                print(f'A chunk processing generated an exception: {exc}')
                traceback.print_exc()

    # Concatenate all processed chunks back into a single DataFrame
    if not processed_chunks:
        print("No data remained after multiprocessing and filtering. No plot will be generated.")
        return

    combined_df = pd.concat(processed_chunks).reset_index(drop=True)

    # If no data remains after filtering in any chunk, print a message and exit
    if combined_df.empty:
        print("No data meets the criteria (WaitTime >= 4 hours) after initial filtering and combining. No plot will be generated.")
        return

    print("Finished parallel data preprocessing. Starting aggregation...")

    # Group by 'JobID' and sum the 'WaitTime'
    total_wait_time_per_jobid = combined_df.groupby('JobID').agg(
        TotalWaitTime=('WaitTime', 'sum')
    ).reset_index()

    # If total_wait_time_per_jobid is empty after aggregation, print a message and exit
    if total_wait_time_per_jobid.empty:
        print("No JobID data after aggregation to plot total wait time per JobID.")
        return

    # Order the 'JobID' categories explicitly by 'TotalWaitTime' for plotting
    total_wait_time_per_jobid = total_wait_time_per_jobid.sort_values(
        by='TotalWaitTime', ascending=True
    )
    total_wait_time_per_jobid['JobID'] = pd.Categorical(
        total_wait_time_per_jobid['JobID'],
        categories=total_wait_time_per_jobid['JobID'],
        ordered=True
    )

    # Create the directory for saving plots if it doesn't exist
    plots_dir = "plots/total_jobid_wait_times"
    os.makedirs(plots_dir, exist_ok=True)

    print(f"Generating plot with DPI: {plot_dpi}...")
    # Create the plot using plotnine (this part remains single-threaded as it's one plot)
    plot = (
        ggplot(total_wait_time_per_jobid, aes(x='JobID', y='TotalWaitTime', fill='TotalWaitTime'))
        + geom_col(width=0.7)
        + scale_fill_gradient(low="blue", high="red")
        + labs(
            x='JobID',
            y='Total Wait Time (hours)',
            title='Total Wait Time Per Unique JobID',
            subtitle='Sum of Wait Times (>= 4 hours) for Each JobID',
            fill='TotalWaitTime'
        )
        + theme(
            axis_text_x=element_text(angle=90, hjust=1),
            plot_title=element_text(hjust=0.5),
            plot_subtitle=element_text(hjust=0.5)
        )
        + guides(fill=False)
    )

    # Save the generated plot to a file, using the specified DPI
    plot_filename = os.path.join(plots_dir, "total_wait_time_per_jobid.png")
    plot.save(plot_filename, dpi=plot_dpi) # Added dpi parameter
    print(f"Successfully saved plot: {plot_filename}")

#########################


#########################

'''
Each unique user and wait time for their jobs 

'''
import pandas as pd
import numpy as np
from plotnine import (
    ggplot,
    aes,
    geom_col,
    scale_fill_gradient,
    labs,
    theme,
    element_text,
    guides,
)
import os
import concurrent.futures
import traceback

def _process_df_chunk(chunk_df):
    """
    Helper function to process a chunk of the DataFrame in parallel.
    Performs datetime conversion, wait time calculation, and initial filtering.

    Args:
        chunk_df (pd.DataFrame): A chunk of the original DataFrame.

    Returns:
        pd.DataFrame: Processed chunk with 'Account', 'JobID', 'WaitTime',
                      or an empty DataFrame if no data meets criteria.
    """
    try:
        # Explicitly create a deep copy to prevent SettingWithCopyWarning
        chunk_df = chunk_df.copy(deep=True)

        # Ensure 'Start' and 'Submit' columns are datetime objects
        chunk_df['Start'] = pd.to_datetime(chunk_df['Start'], errors='coerce')
        chunk_df['Submit'] = pd.to_datetime(chunk_df['Submit'], errors='coerce')

        # Drop rows where 'Start' or 'Submit' could not be converted to datetime (i.e., they are NaT)
        chunk_df.dropna(subset=['Start', 'Submit'], inplace=True)

        # Calculate the difference between 'Start' and 'Submit' in seconds
        chunk_df['WaitTime'] = (chunk_df['Start'] - chunk_df['Submit']).dt.total_seconds()

        # Only keep rows where 'WaitTime' is greater than or equal to 4 hours (14400 seconds)
        chunk_df = chunk_df[chunk_df['WaitTime'] >= 14400]

        # Convert 'WaitTime' from seconds to hours for better readability
        chunk_df['WaitTime'] = chunk_df['WaitTime'] / 3600

        # Select only the necessary columns ('Account', 'JobID', and 'WaitTime')
        return chunk_df[['Account', 'JobID', 'WaitTime']]

    except Exception as e:
        print(f"Error processing DataFrame chunk: {e}")
        traceback.print_exc()
        return pd.DataFrame() # Return empty DataFrame on error

def _plot_single_user_jobs(user_data_for_plot, account_name, base_plots_dir, plot_dpi):
    """
    Helper function to plot and save a single user's job wait time data.
    This function will be executed in a separate process.

    Args:
        user_data_for_plot (pd.DataFrame): DataFrame containing wait time data for a single user's jobs.
        account_name (str): The account name for the current plot.
        base_plots_dir (str): The base directory where plots should be saved.
        plot_dpi (int): Resolution of the saved plot image in dots per inch.
    """
    try:
        if user_data_for_plot.empty:
            print(f"No valid data for Account: {account_name}. Skipping plot.")
            return

        # Ensure JobID is ordered by TotalWaitTime for consistent plotting
        user_data_for_plot = user_data_for_plot.sort_values(
            by='TotalWaitTime', ascending=True
        )
        user_data_for_plot['JobID'] = pd.Categorical(
            user_data_for_plot['JobID'],
            categories=user_data_for_plot['JobID'],
            ordered=True
        )

        # Define a specific directory for each account to keep plots organized
        account_plots_dir = os.path.join(base_plots_dir, str(account_name))
        os.makedirs(account_plots_dir, exist_ok=True)

        # Create the plot using plotnine
        plot = (
            ggplot(user_data_for_plot, aes(x='JobID', y='TotalWaitTime', fill='TotalWaitTime'))
            + geom_col(width=0.7)
            + scale_fill_gradient(low="blue", high="red")
            + labs(
                x='JobID',
                y='Total Wait Time (hours)',
                title=f'Total Wait Time for Jobs by User: {account_name}',
                subtitle='Sum of Wait Times (>= 4 hours) for Each JobID',
                fill='TotalWaitTime'
            )
            + theme(
                axis_text_x=element_text(angle=90, hjust=1), # Rotate x-axis labels
                plot_title=element_text(hjust=0.5),
                plot_subtitle=element_text(hjust=0.5)
            )
            + guides(fill=False)
        )

        # Sanitize account name for filename to avoid issues with special characters
        sanitized_account_name = (
            str(account_name)
            .replace('/', '_')
            .replace('\\', '_')
            .replace(':', '_')
            .replace('*', '_')
            .replace('?', '_')
            .replace('"', '_')
            .replace('<', '_')
            .replace('>', '_')
            .replace('|', '_')
        )
        plot_filename = os.path.join(account_plots_dir, f"{sanitized_account_name}_jobs_wait_time.png")
        plot.save(plot_filename, dpi=plot_dpi)
        print(f"Successfully saved plot for User: {account_name} to {plot_filename}")

    except Exception as e:
        print(f"Error plotting for User: {account_name}: {e}")
        traceback.print_exc()

def plot_jobs_per_user_wait_times_multiprocessed(df, num_chunks=None, plot_dpi=150):
    """
    Plots the total wait time for each JobID, grouped by unique user (Account),
    using multiprocessing for faster data preprocessing and plot generation.

    Args:
        df (pd.DataFrame): DataFrame containing 'Start', 'Submit', 'Account', and 'JobID' columns.
        num_chunks (int, optional): Number of chunks to split the DataFrame into for
                                    parallel processing. If None, defaults to os.cpu_count().
        plot_dpi (int, optional): Resolution of the saved plot image in dots per inch.
                                  Defaults to 150.
    """

    print("Starting initial data preprocessing with multiprocessing...")

    if num_chunks is None:
        num_chunks = os.cpu_count()

    df_chunks = [chunk for chunk in np.array_split(df, num_chunks) if not chunk.empty]

    processed_chunks = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_chunks) as executor:
        futures = [executor.submit(_process_df_chunk, chunk) for chunk in df_chunks]

        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                processed_chunk = future.result()
                if not processed_chunk.empty:
                    processed_chunks.append(processed_chunk)
                print(f"Finished processing chunk {i+1}/{len(df_chunks)}.")
            except Exception as exc:
                print(f'A chunk processing generated an exception: {exc}')
                traceback.print_exc()

    if not processed_chunks:
        print("No data remained after multiprocessing and initial filtering. No plots will be generated.")
        return

    combined_df = pd.concat(processed_chunks).reset_index(drop=True)

    if combined_df.empty:
        print("No data meets the criteria (WaitTime >= 4 hours) after initial filtering and combining. No plots will be generated.")
        return

    print("Finished parallel data preprocessing. Starting user-wise aggregation...")

    # Group by 'Account' and 'JobID' to get total wait time per job for each user
    total_wait_time_per_user_job = combined_df.groupby(['Account', 'JobID']).agg(
        TotalWaitTime=('WaitTime', 'sum')
    ).reset_index()

    if total_wait_time_per_user_job.empty:
        print("No job data after user-wise aggregation. No plots will be generated.")
        return

    # Create the base directory for saving plots if it doesn't exist
    base_plots_dir = "plots/user_job_wait_times" # New directory for user-specific plots
    os.makedirs(base_plots_dir, exist_ok=True)

    # Get unique accounts to iterate through
    unique_accounts = total_wait_time_per_user_job['Account'].unique()
    print(f"Found {len(unique_accounts)} unique users to plot.")

    print("Generating plots for each user in parallel...")
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = []
        for account in unique_accounts:
            # Filter the aggregated data for the current user
            user_data = total_wait_time_per_user_job[
                total_wait_time_per_user_job['Account'] == account
            ].copy() # .copy() is important to avoid SettingWithCopyWarning in worker processes

            futures.append(
                executor.submit(
                    _plot_single_user_jobs, user_data, account, base_plots_dir, plot_dpi
                )
            )

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result() # Re-raise any exceptions from worker processes
            except Exception as exc:
                print(f'An error occurred during plotting a user\'s jobs: {exc}')
                traceback.print_exc()

    print("All user job plots finished generating.")





#########################










def plot_all_slurm():
    '''
    Preprocess and then plot all the raapoi user and estimated cost data.
    '''
    df = pd.read_csv('raapoi_data.csv', dtype={15: str})
    # df = pd.read_csv('raapoi_data.csv')
    df = preprocess_data(df)

    plot_unique_users_per_month(df)
    plot_unique_users_per_year(df)
    plot_costs_per_year(df)
    # plot_costs_per_month(df) # debug
    # plot_submit_start_time(df) # optimize
    # plot_total_wait_time_per_jobid_multiprocessed(df, num_chunks=os.cpu_count())
    # plot_jobs_per_user_wait_times_multiprocessed(df, num_chunks=os.cpu_count(), plot_dpi=150)
    
plot_all_slurm()
