import argparse
import inspect
from functions.process_collate import multiCollate
from functions.slurm_data_collection import merge_slurm_data, grab_slurm_data, split_data_nprocs
from functions.raapoi_plots import plot_all_slurm

# Dictionary of available functions to run
functions_dict = {
    "multiCollate": multiCollate,
    "merge_slurm_data": merge_slurm_data,
    "grab_slurm_data": grab_slurm_data,
    "split_data_nprocs": split_data_nprocs,
    "plot_all_slurm": plot_all_slurm
}

if __name__ == '__main__':
    # Create an ArgumentParser object
    parser = argparse.ArgumentParser(description="Run specified function")

    # Get docstrings for each function and add them as choices for the command line argument
    function_choices = {k: inspect.getdoc(v) for k, v in functions_dict.items()}
    parser.add_argument("function", choices=function_choices.keys(), 
                        help="Name of the function to run. The functions do the following:\n" +
                        "\n".join(f"{k}: {v}" for k, v in function_choices.items()))

    # Parse command line arguments
    args = parser.parse_args()

    # Run the specified function
    function_to_run = functions_dict[args.function]
    function_to_run()
