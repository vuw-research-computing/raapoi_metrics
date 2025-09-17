import os
import subprocess
from multiprocessing import Pool

def transfer_file(source_file, destination):
    command = ["scp", source_file, destination]
    print(f'command: {command}')
    subprocess.run(command)

def main():
    # Define source and destination information
    source_server = "admduggalro@raapoi.vuw.ac.nz"
    source_file = "/nfs/scratch/admduggalro/raapoi_metrics/raapoi_data.csv"
    destination_server = "duggalrohi@cucina-giardino.ecs.vuw.ac.nz"
    destination_dir = "/home/duggalrohi/"

    # Define number of processes for multiprocessing
    num_processes = 4  # Adjust as needed

    # Split the CSV file into chunks based on the number of processes
    
    split_command = ["split", "-n", "{}".format(num_processes), "--numeric-suffixes", source_file, "raapoi_data"]
    print(split_command)
    subprocess.run(split_command)
    print('Split command complete!')

    # Create pool of processes for multiprocessing
    with Pool(processes=num_processes) as pool:
        print(f'num_processes: {num_processes}')
        # Define function arguments
        args = [("raapoi_data0{}".format(i), "{}:{}".format(destination_server, destination_dir)) for i in range(0, num_processes)]
        print(f'args: {args}')
        # Execute file transfer function in parallel
        pool.starmap(transfer_file, args)

    # Clean up temporary files (optional)
    for i in range(0, num_processes):
        print("Removing: raapoi_data0{} file".format(i))
        os.remove("raapoi_data0{}".format(i))

    print("File transfer completed successfully.")

if __name__ == "__main__":
    main()

