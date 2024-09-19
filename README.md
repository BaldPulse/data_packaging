# Data Packaging

## Description
This program extracts data from rosbags and packages them into a structured data format. The data format is described below.

Directory layout 
```python
[task]
├── episodes.csv
├── [modality 0]
│   ├── [data file]
│   ├── [data file]
│   ├── [data file]
│   └── ...
├── [modality 1]
│   ├── [data file]
│   └── ...
└── ...
```
The above diagram shows the directory layout for the data of a single task. Every task directory contains an `episodes.csv` file and multiple modality directories. `episodes.csv` contains relavent information regarding the episodes. Each modality directory contains data files for that modality. The data files are named according to the episode id, which is stored in `episodes.csv`.

## Running with python3
install [ros noetic] (http://wiki.ros.org/noetic/Installation/Ubuntu) \
install [libjxl] (https://github.com/libjxl/libjxl) \
use `pip` to install `opencv-python` and `tqdm`

run `python3 continuous_process.py -h` 

## Docker 
Alternatively, you can use docker.
Modify `docker_run.sh` and run the script with `bash docker_run.sh`.
If you would like the program to stop automatically at a certain time, modify `docker_run_auto_stop.sh`an run the script with `bash docker_run_auto_stop.sh`.
