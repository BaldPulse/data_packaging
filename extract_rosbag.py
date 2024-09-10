import os
import csv
import cv2
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

    def extract_all(self, bag_name: str, episode_id: str, output_directory: str):
        '''
        input:
        bag_name: str, the name of the bag file
        episode_id: str, the episode id
        '''
        try:
            bag_file = rosbag.Bag(bag_name, 'r')
        except:
            raise ValueError(f"Bag file {bag_name} not found")
        #todo: maybe we can parameterize topic names
        # create appropriate csv files
        camera_accel_csv = open(os.path.join(output_directory, 'camera_accel',f"{episode_id}.csv"), 'w')
        camera_gyro_csv = open(os.path.join(output_directory, 'camera_gyro',f"{episode_id}.csv"), 'w')
        motionCapture_scv = open(os.path.join(output_directory, 'motion_capture',f"{episode_id}.csv"), 'w')
        camera_accel_writer = csv.writer(camera_accel_csv)
        camera_gyro_writer = csv.writer(camera_gyro_csv)
        motionCapture_writer = csv.writer(motionCapture_scv)
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
        metadata = {
            "rosbag_filename": bag_name
        }
        modalities = {
            "camera_accel": {"frames": 0, "sensor_model": "Orbbec Gemini 2 L"},
            "camera_gyro": {"frames": 0, "sensor_model": "Orbbec Gemini 2 L"},
            "color": {"frames": 0, "resolution": "1280*800", "sensor_model": "Orbbec Gemini 2 L"},
            "depth": {"frames": 0, "resolution": "1280*800", "sensor_model": "Orbbec Gemini 2 L"},
            "motion_capture": {"frames": 0, "sensor_model": "viryn full-body inertial motion capture suit"}
        }
        start_time = bag_file.get_start_time()
        end_time = bag_file.get_end_time()
        episode_data = episode_id + ", " + str(int(end_time - start_time)) + ", 5, " + str(metadata).replace(",", ";") + ", "
        for topic, msg, t in tqdm(bag_file.read_messages()):
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
        episode_data += str(modalities).replace(",", ";")
        # close csv files
        camera_accel_csv.close()
        camera_gyro_csv.close()
        motionCapture_scv.close()

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

    tname = bag_name.split('/')[-1].split('.')[0]
    t = datetime.datetime.strptime(tname, "%Y-%m-%d-%H-%M-%S")
    t = time.mktime(t.timetuple())
    epid = str(uuid.uuid1(node=uuid.getnode(),clock_seq=int(t)))
    modalities = ["motion_capture", "camera_accel", "camera_gyro", "color", "depth"]
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
    for modality in modalities:
        subprocess.run(
            ["mv","-f", f"{buffer_folder}/{modality}/{epid}.*", f"{output_folder}/{modality}"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )