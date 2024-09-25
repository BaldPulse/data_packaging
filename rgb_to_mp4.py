import subprocess
import shlex
import os
import concurrent.futures

# Function to run a process
def run_process(command, verbose = False):
    print(f"Starting process: {command}")
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output, error = process.communicate()
    if verbose:
        # Print process output and error
        if output:
            print(f"Output: {output.strip()}")
        if error:
            print(f"Error: {error.strip()}")

    return process.returncode

def rgb_to_mp4(input_folders, numworkds = 4, verbose=False):
    commands = []
    for folder_path in input_folders:
        output_file = folder_path.split('/')[-1]+".mp4"
        command = f"ffmpeg -y -framerate 30 -pattern_type glob -i '{os.path.join(folder_path, 'color')}/*.png' -c:v libx265 -pix_fmt yuv420p {output_file}"
        command_with_args = shlex.split(command)
        commands.append(command_with_args)

    # Number of worker threads
    num_workers = 4
    print(commands)
    # Use ThreadPoolExecutor to manage the pool of threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Map the run_process function to the list of commands
        futures = [executor.submit(run_process, command, verbose) for command in commands]
        
        # Wait for all futures to complete and get results
        for future in concurrent.futures.as_completed(futures):
            return_code = future.result()
            if verbose:
                print(f"Process finished with return code {return_code}")

    print("All processes have completed.")

if __name__ == "__main__":
    # get the top folder from parsing arguments
    import sys
    if len(sys.argv) > 1:
        top_folder = sys.argv[1]
    else:
        top_folder = "/media/zhaotang/Files/Data/mixed/2024-08-01"
    folders = [os.path.join(top_folder, folder) for folder in os.listdir(top_folder)]
    # time the process
    import time
    start = time.time()
    rgb_to_mp4(folders[:10], verbose=False)
    end = time.time()
    print(f"Time elapsed: {end-start} seconds")