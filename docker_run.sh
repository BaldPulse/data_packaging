#!/bin/bash

# Configuration
IMAGE_NAME="zhaotang/data-processing:data-packaging"
CONTAINER_NAME="data_packager"  # Name for the container
HOST_DATA_FOLDER="/media/files/bags"  #TODO: Replace with the path to your host data folder
CONTAINER_DATA_FOLDER="/data"  # Folder inside the container
HOST_OUTPUT_FOLDER="/media/files/output" #TODO: Replace with the path to your host output folder
CONTAINER_OUTPUT_FOLDER="/output" # Folder inside the container
NUM_THREADS="16" #TODO: Number of threads used by the data packager

# Start the Docker container in the background with volume mapping
docker run -it \
    --name $CONTAINER_NAME \
    -v $HOST_DATA_FOLDER:$CONTAINER_DATA_FOLDER \
    -v $HOST_OUTPUT_FOLDER:$CONTAINER_OUTPUT_FOLDER\
    $IMAGE_NAME \
    python3 /data_packaging/batch_process.py -d /data -o /output -n $NUM_THREADS -c

# Optionally, stop and remove the container
docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME

echo "Command executed and container cleaned up."
