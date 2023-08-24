import argparse
import inspect
from functions.process_collate import multiCollate
from functions.slurm_data_collection import merge_slurm_data, grab_slurm_data, split_data_nprocs
from functions.raapoi_plots import plot_all_slurm
from functions.gen_webpage import gen_webpage

# Dictionary of available functions to run
functions_dict = {
    "multiCollate": multiCollate,
    "merge_slurm_data": merge_slurm_data,
    "grab_slurm_data": grab_slurm_data,
    "split_data_nprocs": split_data_nprocs,
    "plot_all_slurm": plot_all_slurm,
    "gen_webpage": gen_webpage
}

# Add the ordered list
recommended_order = [
    "grab_slurm_data",
    "split_data_nprocs",
    "multiCollate",
    "merge_slurm_data",
    "plot_all_slurm",
    "gen_webpage"
]

# Create the recommended order string for the description
order_str = "\nRecommended Order of Execution:\n" + "\n".join([f"{idx + 1}. {name}" for idx, name in enumerate(recommended_order)])


class CustomHelpFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    pass

if __name__ == '__main__':
    # Get docstrings for each function and add them as choices for the command line argument
    function_descriptions = "\n\n".join(f"{k}: {v}" for k, v in 
                                      {k: inspect.getdoc(v) for k, v in functions_dict.items()}.items() 
                                      if v is not None)
    # Create an ArgumentParser object with a description that includes the function descriptions
    parser = argparse.ArgumentParser(
        formatter_class=CustomHelpFormatter, 
        description=f"Run specified function.\nThe functions do the following:\n\n{function_descriptions}\n\n{order_str}"
    )

    parser.add_argument("function", choices=functions_dict.keys(), 
                        help="Name of the function to run")

    # Parse command line arguments
    args = parser.parse_args()

    # Run the specified function
    function_to_run = functions_dict[args.function]
    function_to_run()
