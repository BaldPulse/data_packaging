import subprocess
import shlex
import os
from tqdm import tqdm
import datetime
import concurrent.futures

def run_process(command, working_directory, verbose = False):
    if verbose:
        print(f"Starting process: {command}")
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=working_directory)
    output, error = process.communicate()
    if verbose:
        # Print process output and error
        if output:
            print(f"Output: {output.strip()}")
        if error:
            print(f"Error: {error.strip()}")


def batch_process_bags(working_directory, bag_files, output_folder, compress_depth, num_workers=4, verbose=False):
    commands = []
    for bag_file in bag_files:
        command = f"python3 extract_rosbag.py {bag_file} {output_folder} \
        {'--compress_depth' if compress_depth else ''}"
        command_with_args = shlex.split(command)
        commands.append(command_with_args)
    l = len(bag_files)
    with tqdm(total=l) as pbar:
        # Use ThreadPoolExecutor to manage the pool of threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Map the run_process function to the list of commands
            futures = [executor.submit(run_process, command, working_directory, verbose) for command in commands]
            
            # Wait for all futures to complete and get results
            for future in concurrent.futures.as_completed(futures):
                return_code = future.result()
                pbar.update(1)
                if verbose:
                    print(f"Process finished with return code {return_code}")

    print("All processes have completed.")


if __name__ == "__main__":
    # get the top data folder a,d output folder from parsing arguments
    import argparse
    parser = argparse.ArgumentParser(description='Batch process ROS bags.')
    parser.add_argument('-d', '--data_folder', type=str, help='The top folder containing ROS bags.')
    parser.add_argument('-o', '--output_folder', type=str, help='The folder to store the output.')
    parser.add_argument('-b', '--buffer_folder', type=str, help='The folder to store the buffer files.')
    parser.add_argument('-n', '--num_workers', type=int, default=4, help='The number of workers to use.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Print verbose output.', default=False)
    parser.add_argument('-c', '--compress_depth', action='store_true', help='Compress depth images to JXL format.', default=False)
    args = parser.parse_args()

    data_folder = args.data_folder
    output_folder = args.output_folder
    num_workers = args.num_workers
    verbose = args.verbose
    compress_depth = args.compress_depth

    # check if the data folder exists
    if not os.path.exists(data_folder):
        raise FileNotFoundError(f"The data folder {data_folder} does not exist.")
    # check if the output folder exists
    if not os.path.exists(output_folder):
        raise FileNotFoundError(f"The output folder {output_folder} does not exist.")
    # check if the buffer folder exists
    if not os.path.exists(args.buffer_folder):
        raise FileNotFoundError(f"The buffer folder {args.buffer_folder} does not exist.")

    # retrieve the lastest bag date from the buffer folder
    # look for a .cache file in the buffer folder
    cache_file = os.path.join(args.buffer_folder, ".cache")
    latest_bag_date = None
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            latest_bag_date = f.read().strip()
    # the date is in the format YYYY-MM-DD-HH-MM-SS
    # check if the date is valid
    if latest_bag_date is not None:
        try:
            datetime.datetime.strptime(latest_bag_date, "%Y-%m-%d-%H-%M-%S")
        except ValueError:
            latest_bag_date = None


    # get all the .bag files in the data folder and its subfolders
    # return the relative path to the data folder
    bag_files = []
    for root, dirs, files in os.walk(data_folder):
        for file in files:
            if file.endswith(".bag"):
                bag_files.append(os.path.relpath(os.path.join(root, file), data_folder))

    modalities = ["motion_capture", "camera_accel", "camera_gyro", "color", "depth"]
    # create the folder structure
    for modality in modalities:
        modality_folder = os.path.join(output_folder, modality)
        if not os.path.exists(modality_folder):
            os.makedirs(modality_folder)
    import csv
    if not os.path.exists(os.path.join(output_folder, 'episodes.csv')):
        with open(os.path.join(output_folder, 'episodes.csv'), 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['episode_id', 'duration', 'num_modalities', 'metadata', 'modalities'])

    # run the batch process
    batch_process_bags(data_folder, bag_files, output_folder,compress_depth, num_workers=num_workers, verbose=verbose)