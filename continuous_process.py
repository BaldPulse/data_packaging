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
thread_pool = []

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
                bag_files.append(os.path.relpath(os.path.join(root, file), data_folder))
    # sort the bag files by date
    bag_files.sort()
    new_bags = []
    for bag_file in bag_files:
        bag_date = bag_file.split("/")[-1]
        if bag_date > latest_bag_date:
            new_bags.append(bag_file)
    return new_bags

def consumer(consumer_id, output_folder, log_file, compress_depth=False, verbose=False):
    while not stop_flag.is_set():
        try:
            bag_file = bag_queue.get(timeout=1)
            command = f"python3 extract_rosbag.py {bag_file} {output_folder} \
            {'--compress_depth' if compress_depth else ''}"
            command_with_args = shlex.split(command)
            if verbose:
                print(f"Processing {bag_file}...")
            process = subprocess.Popen(command_with_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            # wait for the process to finish
            while process.poll() is None:
                # if the stop flag is set, kill the process and exit
                if stop_flag.is_set():
                    process.kill()
                    return
                # otherwise, sleep for 1 second
                time.sleep(1)
            # if the verbose flag is set, write the output to the log file
            if verbose:
                with open(log_file, "a") as f:
                    f.write(f"Consumer {consumer_id} output: {process.stdout.read()}")
                    f.write(f"Consumer {consumer_id} error: {process.stderr.read()}")
        except queue.Empty:
            # if the queue is empty, sleep for 4 second
            time.sleep(4)




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
    # if the verbose flag is set, print the latest bag date
    if verbose:
        print(f"Latest bag date: {latest_bag_date}")
        # also create a log file in the buffer folder for verbose output
        log_file = os.path.join(args.buffer_folder, "log.txt")

    
    #TODO: get current date and only process bags that are generated today
    def signal_handler(sig, frame):
        # print a line in green color
        print("\033[92m" + "Exit command recieved..." + "\033[0m")
        # set the stop flag
        stop_flag.set()
        # scrub the buffer folder
        print("Scrubbing buffer folder...")
        subprocess.run(["rm", "-rf", f"{args.buffer_folder}/*"], check=True)
        print("Writing latest bag date to the cache file...")
        # write the latest bag date to the cache file
        with open(cache_file, "w") as f:
            f.write(latest_bag_date)
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
                    # kill the threads
                    for thread in thread_pool:
                        thread.kill()
                else:
                    print("waiting for threads to finish to exit gracefully...")
        # exit the program
        sys.exit(0)
    # ----------------------------- configuration complete -----------------------------
    if latest_bag_date is None:
        latest_bag_date = "0000-00-00-00-00-00"
    # create the consumer threads
    print(f'Starting {num_workers} consumer threads...')
    for i in range(num_workers):
        thread = threading.Thread(target=consumer, args=(i, output_folder, log_file, compress_depth, verbose), daemon=True)
        thread.start()
        thread_pool.append(thread)

    # set the signal handler
    import signal
    signal.signal(signal.SIGINT, signal_handler)
    # main execution loop for producer
    while True:
        # get the list of new bags
        new_bags = find_new_bags(data_folder, latest_bag_date)
        # update the latest bag date
        if new_bags:
            latest_bag_date = new_bags[-1].split("/")[-1].split(".")[0]
        # add the new bags to the queue
        for bag in new_bags:
            bag_queue.put(bag)


        
