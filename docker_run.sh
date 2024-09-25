#!/bin/bash

# Configuration
# get ip
ips=$(
  ip addr show | 
  grep "192.168.66" | 
  awk '{print $2}' | 
  cut -d '/' -f 1
)

last_octet=${ips##*.}
ID=$((last_octet - 110))

IMAGE_NAME="cyberorigin/post-process:v1"
CONTAINER_NAME="data_packager"  # Name for the container
#! input directory
HOST_DATA_FOLDER="/sxx"  #TODO: Replace with the path to your host data folder
CONTAINER_DATA_FOLDER="/data"  # Folder inside the container
#! output directory
HOST_OUTPUT_FOLDER="/sxx/parsed_file" #"/NasData_2/ID$ID" #TODO: Replace with the path to your host output folder
CONTAINER_OUTPUT_FOLDER="/output" # Folder inside the container
HOST_BUFFER_FOLDER="/home/sbtnuc/work/buffer" #TODO: Replace with the path to your host buffer folder
CONTAINER_BUFFER_FOLDER="/buffer" # Folder inside the container
HOST_REPO_FOLDER="/home/sbtnuc/lulu/data_packaging-continuous" #TODO: Replace with the path to this repo
CONTAINER_REPO_FOLDER="/data_packaging" # Folder inside the container
#! tongsih jie duoshaobao
NUM_THREADS="8" #TODO: Number of threads used by the data packager

# Start the Docker container in the background with volume mapping
# -r select specific date
docker run --rm \
    --name $CONTAINER_NAME \
    -v $HOST_DATA_FOLDER:$CONTAINER_DATA_FOLDER \
    -v $HOST_OUTPUT_FOLDER:$CONTAINER_OUTPUT_FOLDER\
    -v $HOST_BUFFER_FOLDER:$CONTAINER_BUFFER_FOLDER\
    -v $HOST_REPO_FOLDER:$CONTAINER_REPO_FOLDER\
    $IMAGE_NAME \
    python3 /data_packaging/continuous_process.py -d /data -o /output -b /buffer -r 2024-08-05 2024-08-06 2024-08-07 2024-08-08 2024-08-09 -n $NUM_THREADS -v

echo "Command executed and container cleaned up."
