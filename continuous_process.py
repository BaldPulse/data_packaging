import subprocess
import shlex
import os
import sys
from tqdm import tqdm
import datetime
import concurrent.futures
import threading
import time
import queue

bag_queue = queue.Queue()
stop_flag = threading.Event()
thread_error_flag = threading.Event()
thread_pool = []
thread_bag_on_hand = []

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

def find_new_bags(data_folder, latest_bag_date):
    bag_files = []
    for root, dirs, files in os.walk(data_folder):
        for file in files:
            if file.endswith(".bag"):
                bag_files.append(os.path.join(root, file))
    # sort the bag files by date
    bag_files.sort()
    new_bags = []
    for bag_file in bag_files:
        bag_date = bag_file.split("/")[-1].split(".")[0]
        if bag_date > latest_bag_date:
            new_bags.append(bag_file)
    return new_bags

def consumer(consumer_id, output_folder, buffer_folder, compress_depth=False, verbose=False, log_file=None):
    while not stop_flag.is_set():
        if thread_bag_on_hand[consumer_id] is not None:
            # this indicates that there was an error in the previous run
            thread_error_flag.set() # set the error flag
            return # exit the thread
        try:
            bag_file = bag_queue.get(timeout=1)
            bag_date = "-".join(bag_file.split("/")[-1].split(".")[0].split("-")[:3])
            thread_bag_on_hand[consumer_id] = bag_file
            command = f"python3 /data_packaging/extract_rosbag.py {bag_file} {os.path.join(output_folder, bag_date)} {buffer_folder} \
            {'--compress_depth' if compress_depth else ''}"
            command_with_args = shlex.split(command)
            if verbose:
                print(f"Processing {bag_file}...")
            process = subprocess.Popen(command_with_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            process.wait()
            # if the verbose flag is set, write the output to the log file
            if verbose:
                with open(log_file, "a") as f:
                    f.write(f"------{datetime.datetime.now()}------\n")
                    f.write(f"Consumer {consumer_id} output: {process.stdout.read()}")
                    f.write(f"Consumer {consumer_id} error: {process.stderr.read()}")
            if process.returncode == 0:
                bag_queue.task_done() # mark the task as done
                thread_bag_on_hand[consumer_id] = None # this thread is free
        except queue.Empty:
            time.sleep(4)

def signal_handler(sig, frame):
    # print a line in green color
    print("\033[92m" + "Exit command recieved..." + "\033[0m")
    # set the stop flag
    stop_flag.set()
    # wait for the threads to finish for 20 seconds
    print("Waiting for threads to finish...")
    for thread in thread_pool:
        thread.join(timeout=20)
    # check if the threads are still alive
    for thread in thread_pool:
        if thread.is_alive():
            # ask if the user wants to force exit
            force_exit = input("Some threads are still running. Do you want to force exit? (y/n) ")
            if force_exit.lower() == "y":
                print("Force exiting...")
                break
            else:
                print("waiting for threads to finish to exit gracefully...")
    # write the exit state to the cache file
    print("Caching exit state...")
    with open(cache_file, "w") as f:
        # sort the thread_bag_on_hand list ignoring None values
        sorted_bag_on_hand = sorted([bag for bag in thread_bag_on_hand if bag is not None])
        for bag in sorted_bag_on_hand:
            f.write(f"{bag}\n")
        # also write all the bags in the queue
        while not bag_queue.empty():
            bag = bag_queue.get()
            f.write(f"{bag}\n")
        # as well as the latest bag date
        f.write(f"{latest_bag_date}\n")
            
    # exit the program
    print("\033[92m" + "Process exited gracefully" + "\033[0m")
    sys.exit(0)

def recover_from_cache(cache_file):
    latest_bag_date = None
    if os.path.exists(cache_file):
        # read the file line by line into a list
        with open(cache_file, "r") as f:
            lines = f.readlines()
        if lines:
            latest_bag_date = lines[-1].strip() # the last line is the latest bag date
        # add the bags to the queue
        for line in lines[:-1]:
            bag_queue.put(line.strip())
        # the date is in the format YYYY-MM-DD-HH-MM-SS
    # check if the date is valid
    if latest_bag_date is not None:
        try:
            datetime.datetime.strptime(latest_bag_date, "%Y-%m-%d-%H-%M-%S")
        except ValueError:
            latest_bag_date = None
    return latest_bag_date


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
    buffer_folder = args.buffer_folder
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
    if not os.path.exists(buffer_folder):
        raise FileNotFoundError(f"The buffer folder {buffer_folder} does not exist.")

    # recover from the last exit state 
    # look for a .cache file in the buffer folder
    cache_file = os.path.join(buffer_folder, ".cache")
    latest_bag_date = recover_from_cache(cache_file)
    
    # if the verbose flag is set, print the latest bag date
    log_file = None
    if verbose:
        print(f"Latest bag date: {latest_bag_date}")
        # also create a log file in the buffer folder for verbose output
        log_file = os.path.join(buffer_folder, "log.txt")
    
    # create the folder structures in each of the output and buffer folders
    modalities = ["motion_capture", "camera_accel", "camera_gyro", "color", "depth"]
    for folder in [output_folder, buffer_folder]:
        for modality in modalities:
            modality_folder = os.path.join(folder, modality)
            if not os.path.exists(modality_folder):
                os.makedirs(modality_folder)
    
    #TODO: get current date and only process bags that are generated today
    
    # ----------------------------- configuration complete -----------------------------
    if latest_bag_date is None:
        latest_bag_date = "0000-00-00-00-00-00"
    # create the consumer threads
    print(f'Starting {num_workers} consumer threads...')
    for i in range(num_workers):
        thread_bag_on_hand.append(None)
        thread = threading.Thread(target=consumer, args=(i, output_folder, buffer_folder, compress_depth, verbose, log_file), daemon=False)
        thread.start()
        thread_pool.append(thread)

    # set the signal handler
    import signal
    signal.signal(signal.SIGINT, signal_handler)
    # main execution loop for producer
    while True:
        # check if there is a problem with the threads
        if thread_error_flag.is_set():
            print("There was an error in one of the threads. Exiting...")
            break
        # get the list of new bags
        new_bags = find_new_bags(data_folder, latest_bag_date)
        # update the latest bag date
        if new_bags:
            latest_bag_date = new_bags[-1].split("/")[-1].split(".")[0]
        # add the new bags to the queue
        for bag in new_bags:
            bag_date = "-".join(bag.split("/")[-1].split(".")[0].split("-")[:3])
            if not os.path.exists(os.path.join(output_folder, bag_date)):
                os.makedirs(os.path.join(output_folder, bag_date))
            bag_queue.put(bag)

    stop_flag.set()
    for thread in thread_pool:
        thread.join()
    
    print("\033[91m" + "Error caught in process" + "\033[0m")
    print("Caching exit state...")
    with open(cache_file, "w") as f:
        # sort thread_bag_on_hand list ignoring None values
        sorted_bag_on_hand = sorted([bag for bag in thread_bag_on_hand if bag is not None])
        for bag in sorted_bag_on_hand:
            f.write(f"{bag}\n")
        # also write all bags in the queue
        while not bag_queue.empty():
            bag = bag_queue.get()
            f.write(f"{bag}\n")
        # as well as the latest bag date
        f.write(f"{latest_bag_date}\n")
    # print a line in red color
    print("\033[91m" + "Process exited gracefully" + "\033[0m")
    sys.exit(1)

        
