import os
import csv
import cv2
import sys
import rosbag
import subprocess
from tqdm import tqdm
from utils import Utils
import numpy as np
import pandas as pd
from cv_bridge import CvBridge

class Extractor():
    def __init__(self, compress_depth=True) -> None:
        self.utils = Utils()
        self.bridge = CvBridge()
        self.compress_depth = compress_depth
        pass

    def error_exit(self, episode_id: str, output_directory: str):
        if os.path.exists(f"{output_directory}/color/{episode_id}"):
            os.system(f"rm -r {output_directory}/color/{episode_id}")
        if os.path.exists(f"{output_directory}/depth/{episode_id}"):
            os.system(f"rm -r {output_directory}/depth/{episode_id}")
        if os.path.exists(f"{output_directory}/camera_accel/{episode_id}.csv"):
            os.system(f"rm {output_directory}/camera_accel/{episode_id}.csv")
        if os.path.exists(f"{output_directory}/camera_gyro/{episode_id}.csv"):
            os.system(f"rm {output_directory}/camera_gyro/{episode_id}.csv")
        if os.path.exists(f"{output_directory}/motion_capture/{episode_id}.csv"):
            os.system(f"rm {output_directory}/motion_capture/{episode_id}.csv")
        sys.exit(0)

    def extract_all(self, bag_name: str, episode_id: str, output_directory: str):
        '''
        input:
        bag_name: str, the name of the bag file
        episode_id: str, the episode id
        '''
        try:
            bag_file = rosbag.Bag(bag_name, 'r')
        except:
            print(f"Bag file {bag_name} is unindexed or damaged")
            self.error_exit(episode_id, output_directory)
        
        #check duration
        try:
            start_time = bag_file.get_start_time()
            end_time = bag_file.get_end_time()
            if end_time - start_time > 120:
                print(f"Episode {bag_name} duration is greater than 2 minutes")
                os.system(f"rm {bag_name}")
                self.error_exit(episode_id, output_directory)
        except Exception as e:
            print(f"Error getting bag start/end time for {bag_name}: {e}")
            self.error_exit(episode_id, output_directory)
        # create appropriate csv files
        camera_accel_csv = open(os.path.join(output_directory, 'camera_accel',f"{episode_id}.csv"), 'w')
        camera_gyro_csv = open(os.path.join(output_directory, 'camera_gyro',f"{episode_id}.csv"), 'w')
        motionCapture_csv = open(os.path.join(output_directory, 'motion_capture',f"{episode_id}.csv"), 'w')
        camera_accel_writer = csv.writer(camera_accel_csv)
        camera_gyro_writer = csv.writer(camera_gyro_csv)
        motionCapture_writer = csv.writer(motionCapture_csv)
        camera_accel_writer.writerow(['timestamp', 'linear_acceleration_x', 'linear_acceleration_y', 'linear_acceleration_z'])
        camera_gyro_writer.writerow(['timestamp', 'angular_velocity_x', 'angular_velocity_y', 'angular_velocity_z'])
        motionCapture_title_list = None

        # create a buffer directory for images
        output_color_png_subdir = "color/"+episode_id+'/'
        output_color_png_dir = os.path.join(output_directory, output_color_png_subdir)
        if not os.path.exists(output_color_png_dir):
            os.makedirs(output_color_png_dir)
        output_depth_png_subdir = "depth/"+episode_id+'/'
        output_depth_png_dir = os.path.join(output_directory, output_depth_png_subdir)
        if not os.path.exists(output_depth_png_dir):
            os.makedirs(output_depth_png_dir)

        # time stamp data for color images
        color_time_stamps = []
        mocap_time_stamps = []
        metadata = {
            "rosbag_filename": bag_name.split('/')[-1]
        }
        modalities = {
            "camera_accel": {"frames": 0, "sensor_model": "Orbbec Gemini 2 L"},
            "camera_gyro": {"frames": 0, "sensor_model": "Orbbec Gemini 2 L"},
            "color": {"frames": 0, "resolution": "1280*800", "sensor_model": "Orbbec Gemini 2 L"},
            "depth": {"frames": 0, "resolution": "1280*800", "sensor_model": "Orbbec Gemini 2 L"},
            "motion_capture": {"frames": 0, "sensor_model": "virdyn full-body inertial motion capture suit"}
        }
        episode_data = episode_id + ", " + str(int(end_time - start_time)) + ", 5, " + str(metadata).replace(",", ";") + ", "
        for topic, msg, t in bag_file.read_messages():
            timestamp_ros = f"{t.secs}.{t.nsecs:09d}"
            #todo: switch statements
            if topic == "/camera/accel/sample":
                # Extract IMU acceleration data
                camera_accel_writer.writerow([timestamp_ros, 
                                              msg.linear_acceleration.x, 
                                              msg.linear_acceleration.y, 
                                              msg.linear_acceleration.z])
                modalities["camera_accel"]["frames"] += 1

            elif topic == "/camera/gyro/sample":
                # Extract IMU gyroscope data
                camera_gyro_writer.writerow([timestamp_ros, 
                                             msg.angular_velocity.x, 
                                             msg.angular_velocity.y, 
                                             msg.angular_velocity.z])
                modalities["camera_gyro"]["frames"] += 1

            elif topic == "/vdmsg":
                # Get title name if it is not already extracted
                if motionCapture_title_list is None:
                    motionCapture_title_list = self.utils.extract_vdmsg_bag_position_msg_title_list("", "lhand_", "rhand_", msg)
                    motionCapture_writer.writerow(['timestamp'] + motionCapture_title_list)
                # Extract motion capture data
                frame_data = self.utils.extract_vdmsg_bag_position_msg_frame_data(msg)
                motionCapture_writer.writerow([timestamp_ros] + frame_data)
                modalities["motion_capture"]["frames"] += 1
                mocap_time_stamps.append(float(timestamp_ros))
            
            # and non csv data
            elif topic == "/camera/color/image_raw/compressed":
                # Extract color images
                image = cv2.imdecode(np.frombuffer(msg.data, np.uint8), cv2.IMREAD_COLOR)
                cv2.imwrite(os.path.join(output_directory, 'color', f"{episode_id}",f"{timestamp_ros}.png"), image)
                color_time_stamps.append(timestamp_ros)
                modalities["color"]["frames"] += 1
            
            elif topic == "/camera/depth/image_raw":
                # Extract depth images
                image = self.bridge.imgmsg_to_cv2(msg, '16UC1')
                png_path = os.path.join(output_directory, 'depth', f"{episode_id}",f"{timestamp_ros}.png")
                cv2.imwrite(png_path, image)
                if self.compress_depth:
                    jxl_path = os.path.join(output_directory, output_depth_png_subdir, f"{timestamp_ros}.jxl")
                    subprocess.run(
                        ["cjxl", png_path, jxl_path, "-d", "0"], 
                        check=True, 
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                    os.remove(png_path)
                modalities["depth"]["frames"] += 1
        # check if all modalities have data
        for modality in modalities:
            if modalities[modality]["frames"] == 0:
                print(f"{bag_name} no {modality} data found")
                self.error_exit(episode_id, output_directory)
        episode_data += str(modalities).replace(",", ";")
        # close csv files
        camera_accel_csv.close()
        camera_gyro_csv.close()
        motionCapture_csv.close()

        if self.check_timestamp(np.array(mocap_time_stamps)) == False:
            print(f"{bag_name} timestamp is not valid")
            self.error_exit(episode_id, output_directory)

        # compress color images to mp4
        self.utils.compress_images_to_mp4(color_time_stamps, output_color_png_dir, os.path.join(output_directory, "color"), episode_id)
        # tar depth images
        subprocess.run(
            ["tar", "-cvf",f"../{episode_id}.tar",  f"."],
            cwd= f"{output_directory}/{output_depth_png_subdir}",
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        subprocess.run(
            ["rm", "-rf", f"{output_directory}/{output_depth_png_subdir}"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        # return high level episode data 
        return episode_data
    
    def check_timestamp(self, time_stamps):
        '''
        Check if the provided timestamp array is valid based on specified criteria.

        :param time_stamps: np.array
            Array of timestamps.

        :return: bool
            Returns True if the timestamp array is valid; otherwise, returns False.
        '''
        if len(time_stamps) <= 30:
            # Not enough data to process after removing the first 30 frames
            return False

        # Remove the first 30 frames
        time_stamps = time_stamps[30:]

        # Calculate time differences
        time_diffs = np.diff(time_stamps)

        # Define thresholds
        max_gap = 0.023
        min_gap = 0.01
        min_interval = 0.001

        # Check for invalid time intervals
        too_large_gaps = np.sum(time_diffs > max_gap)
        too_small_gaps = np.sum(time_diffs < min_gap)

        # Check if any intervals are too small
        if np.any(time_diffs < min_interval):
            return False

        # Check if the number of invalid frames exceeds the threshold
        if too_large_gaps + too_small_gaps > 5:
            return False

        return True
    
    def check_motion_capture_data(self, file_path, max_speed=0.903, max_invalid_ratio=0.025):
        '''
        Check the validity of motion capture data based on a given maximum invalid data ratio.

        :param file_path: str
            Path to the CSV file containing motion capture data. The file should include columns for timestamps and hand positions.

        :param max_invalid_ratio: float
            The maximum allowable ratio of invalid data. This ratio represents the proportion of data points where the speed is below a specified threshold relative to the total number of data points.
            If the calculated invalid data ratio exceeds this value, the data is considered invalid.

        :return: bool
            Returns True if the ratio of invalid data is less than or equal to `max_invalid_ratio`, indicating the data is valid;
            otherwise, returns False, indicating the data is invalid.

        Exceptions:
        - Catches `pd.errors.EmptyDataError` if the file is empty, and returns False.
        - Catches general exceptions for any other errors during file reading or processing, and returns False.
        '''

        # Obtained from statistics
        # max_speed = 0.903
        # Determined as a percentage of the max_speed
        min_speed = 0.1 * max_speed
        # Experience value
        # max_invalid_ratio = 0.025

        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            # Ensure required columns exist
            required_columns = ['rhand_Hand_x', 'rhand_Hand_y', 'rhand_Hand_z', 'timestamp']
            if not all(col in df.columns for col in required_columns):
                print(f"File {file_path} is missing required columns.")
                return None

            # Get positions
            x_positions = df['rhand_Hand_x'].values[30:]
            y_positions = df['rhand_Hand_y'].values[30:]
            z_positions = df['rhand_Hand_z'].values[30:]
            time_stamps = df['timestamp'].values[30:]
            # Calculate the gradients
            grad_x = np.gradient(x_positions, time_stamps)
            grad_y = np.gradient(y_positions, time_stamps)
            grad_z = np.gradient(z_positions, time_stamps)
            # Stack gradients into a single array
            gradients = np.stack([grad_x, grad_y, grad_z], axis=-1)
            # Calculate the velocities
            velocities = np.linalg.norm(gradients, axis=1)

            # Data smoothing
            window_size = 5
            window_size = window_size if window_size % 2 != 0 else window_size + 1
            # Apply a sliding window average to the velocities
            padded_velocities = np.pad(velocities, (window_size // 2, window_size // 2), mode='edge')
            smoothed_velocities = np.convolve(padded_velocities, np.ones(window_size) / window_size, mode='valid')

            # Count valid frames and over-limit frames
            num_valid_frames = np.sum(smoothed_velocities > min_speed)
            num_over_max = np.sum(smoothed_velocities > max_speed)
            # Calculate ratio
            ratio = num_over_max / num_valid_frames if num_valid_frames > 0 else 0
            if ratio < max_invalid_ratio:
                return True
            else:
                return False
        except pd.errors.EmptyDataError:
            print(f"File {file_path} is empty.")
            return None
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            return None

if __name__ == "__main__":
    import uuid
    import datetime
    import time
    import argparse
    parser = argparse.ArgumentParser(description='Extract data from a ROS bag.')
    parser.add_argument('bag_name', type=str, help='The name of the bag file.')
    parser.add_argument('output_folder', type=str, help='The folder to store the output.')
    parser.add_argument('buffer_folder', type=str, help='The buffer folder for intermediat operations.')
    parser.add_argument('--compress_depth', action='store_true', help='Compress depth images to JXL format.', default=False)
    args = parser.parse_args()

    bag_name = args.bag_name
    output_folder = args.output_folder
    buffer_folder = args.buffer_folder
    compress_depth = args.compress_depth

    # tname = bag_name.split('/')[-1].split('.')[0]
    # t = datetime.datetime.strptime(tname, "%Y-%m-%d-%H-%M-%S")
    # t = time.mktime(t.timetuple())
    # epid = str(uuid.uuid1(node=uuid.getnode(),clock_seq=int(t)))
    epid = str(uuid.uuid1()) # this is probably better than the above because it considers time zones
    modalities = ["motion_capture", "camera_accel", "camera_gyro", "color", "depth"]
    extensions = ["csv", "csv", "csv", "mp4", "tar"]
    # create the folder structure
    for modality in modalities:
        modality_folder = os.path.join(output_folder, modality)
        if not os.path.exists(modality_folder):
            os.makedirs(modality_folder)
    for modality in modalities:
        modality_folder = os.path.join(buffer_folder, modality)
        if not os.path.exists(modality_folder):
            os.makedirs(modality_folder)
    # check if episodes.csv exists
    if not os.path.exists(os.path.join(output_folder, 'episodes.csv')):
        with open(os.path.join(output_folder, 'episodes.csv'), 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['episode_id', 'duration', 'num_modalities', 'metadata', 'modalities'])
    extractor = Extractor(compress_depth=compress_depth)
    episode_data =  extractor.extract_all(bag_name, epid, buffer_folder)
    with open(os.path.join(output_folder, 'episodes.csv'), 'a') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(episode_data.split(","))
    # move the files from the buffer folder to the output folder
    # when this process starts, we ignore interruptions
    import signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    for i in range(len(modalities)):
        p = subprocess.run(
            ["mv","-f", f"{buffer_folder}/{modalities[i]}/{epid}.{extensions[i]}", f"{output_folder}/{modalities[i]}/"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )