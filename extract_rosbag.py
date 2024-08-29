
import os
import csv
import cv2
import rosbag
import subprocess
import pandas as pd
from tqdm import tqdm
from cv_bridge import CvBridge

from utils import Utils

def create_path_to_output(output_path):
    if not os.path.exists(output_path):
        os.makedirs(output_path)

class ExtractRosbag():
    def __init__(self, bag_name, episode_id, output_dir=None):
        print(bag_name)
        try:
            self.bag = rosbag.Bag(bag_name, "r")
        except:
            #todo write to log
            raise ValueError(f'{bag_name} Error')
        self.episode_id = episode_id
        # check the time of the rosbag
        self.extract_check_rosbag_time(0, 120)
        self.bridge = CvBridge()
        self.bag_name = bag_name
        self.bag_topics = [topic for topic in self.bag.get_type_and_topic_info().topics]
        self.output_dir = output_dir
        self.utils = Utils()

    def extract_check_rosbag_time(self, min_time, max_time):
        duration = 0
        start_time = self.bag.get_start_time()
        end_time = self.bag.get_end_time()
        self.duration = end_time - start_time
        if duration > max_time:
            # os.remove(self.bag_name)
            #todo  write to log
            with open("mybag_time.txt", "a") as f:
                f.write(f"{self.bag_name}\n")
            raise ValueError("rosbag {self.bag_name} is too long")
        if duration < min_time:
            # os.remove(self.bag_name)
            #todo  write to log
            with open("mybag_time.txt", "a") as f:
                f.write(f"{self.bag_name}\n")

    def extract_check_rosbag_frequency(self, *topics):
        if set(self.bag_topics) == set(topics):
            print("check the frequency")
        else:
            # os.remove(self.bag_name)
            #todo  write to log
            raise ValueError("rosbag {self.bag_name} is too short")

    def extract_check_rosbag_fuzz_testing(self, imagePath, max_variance):
            # Load the image and convert it to grayscale
            image = cv2.imread(imagePath)
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            laplacian = cv2.Laplacian(gray, cv2.CV_64F)
            variance = laplacian.var()
            if variance > max_variance:
                print("picture is ok")
            else:
                #todo write to log
                with open("my_fuzz.txt", "a") as f:
                    f.write(f"{imagePath}\n")
                raise ValueError("picuture {imagePath} too fuzzing")
            
    def extract_imu_accel_from_rosbag(self, topic, output_dir=None):
        """
        Extracts IMU acceleration data from a ROS bag file and saves it as a CSV file.

        Parameters:
        - output_dir (str): The directory path where the CSV file will be saved.
        - topic (str): The ROS topic name from which to extract data.

        Returns:
        - None: The function does not return a value but writes the data directly to a CSV file.
        """
        if output_dir is None and self.output_dir is None:
            # TODO: Write to log
            raise ValueError("Please indicate the output directory!")
        if topic not in self.bag_topics:
            topic_list_str = " ".join(self.bag_topics)
            # TODO: Write to log
            raise ValueError(f"Wrong ROS topic, please check again. Available topics are: {topic_list_str}")
        if output_dir is None:
            output_dir = self.output_dir
        data = []
        output_imu_accel_subdir = "imu/"
        for msg_topic, msg, t in tqdm(self.bag.read_messages(topic), desc=f'Extracting {self.bag_name}'):
            timestamp_ros = f"{t.secs}.{t.nsecs:09d}"
            linear_acceleration_x = msg.linear_acceleration.x
            linear_acceleration_y = msg.linear_acceleration.y
            linear_acceleration_z = msg.linear_acceleration.z
            data.append([
                timestamp_ros,
                linear_acceleration_x, linear_acceleration_y, linear_acceleration_z
            ])
            df = pd.DataFrame(data, columns=[
                'ROS_Timestamp',
                'Linear_Acceleration_x', 'Linear_Acceleration_y', 'Linear_Acceleration_z'
            ])
            # create_path_to_output(os.path.join(output_dir, output_imu_accel_subdir))
            df.to_csv(os.path.join(output_dir, output_imu_accel_subdir, self.episode_id+'.csv'), index=False)

    def extract_imu_gyro_from_rosbag(self, topic, output_dir=None):
        """
        Extracts IMU gyroscope data from a ROS bag file and saves it as a CSV file.

        Parameters:
        - output_dir (str): The directory path where the CSV file will be saved.
        - topic (str): The ROS topic name from which to extract data.

        Returns:
        - None: The function does not return a value but writes the data directly to a CSV file.
        """
        if output_dir is None and self.output_dir is None:
            # TODO: Write to log
            raise ValueError("Please indicate the output directory!")
        if topic not in self.bag_topics:
            topic_list_str = " ".join(self.bag_topics)
            # TODO: Write to log
            raise ValueError(f"Wrong ROS topic, please check again. Available topics are: {topic_list_str}")
        if output_dir is None:
            output_dir = self.output_dir        
        data = []
        output_imu_gyro_subdir =  "imu/"
        for msg_topic, msg, t in tqdm(self.bag.read_messages(topic), desc=f'Extracting {self.bag_name}'):
            timestamp_ros = f"{t.secs}.{t.nsecs:09d}"
            angular_velocity_x = msg.angular_velocity.x
            angular_velocity_y = msg.angular_velocity.y
            angular_velocity_z = msg.angular_velocity.z
            data.append([
                timestamp_ros,
                angular_velocity_x, angular_velocity_y, angular_velocity_z,
            ])
            df = pd.DataFrame(data, columns=[
                'ROS_Timestamp',
                'Angular_Velocity_x', 'Angular_Velocity_y', 'Angular_Velocity_z',
            ])
            # create_path_to_output(os.path.join(output_dir, output_imu_gyro_subdir))
            df.to_csv(os.path.join(output_dir, output_imu_gyro_subdir, self.episode_id+'.csv'), index=False)

    def extract_color_images_from_rosbag(self, topic, output_dir=None):
        """
        Extracts color images from a ROS bag file and saves them as PNG files.

        Parameters:
        - output_dir (str): The directory path where the PNG images will be saved.
        - topic (str): The ROS topic name from which to extract image data.

        Returns:
        - None: The function does not return a value but saves the images as PNG files.
        """
        if output_dir is None and self.output_dir is None:
            # TODO: Write to log
            raise ValueError("Please indicate the output directory!")
        if topic not in self.bag_topics:
            topic_list_str = " ".join(self.bag_topics)
            # TODO: Write to log
            raise ValueError(f"Wrong ROS topic, please check again. Available topics are: {topic_list_str}")
        if output_dir is None:
            output_dir = self.output_dir
        output_color_jpeg_subdir = "color/"
        timelist = []
        for msg_topic, msg, t in tqdm(self.bag.read_messages(topic), desc=f'Extracting {self.bag_name}'):
            timestamp_ros = f"{t.secs}.{t.nsecs:09d}"
            cv_image = self.bridge.compressed_imgmsg_to_cv2(msg, "bgr8")
            # create_path_to_output(os.path.join(output_dir, output_color_jpeg_subdir))
            cv2.imwrite(os.path.join(output_dir, output_color_jpeg_subdir, f"{timestamp_ros}.jpeg"), cv_image)
            timelist.append(f"{timestamp_ros}.jpeg")
        self.utils.compress_jpeg_to_mp4(timelist, os.path.join(output_dir, output_color_jpeg_subdir), )

    def extract_depth_images_from_rosbag(self, topic, output_dir=None):
        """
        Extracts depth images from a ROS bag file and saves them as PNG files.

        Parameters:
        - output_dir (str): The directory path where the PNG images will be saved.
        - topic (str): The ROS topic name from which to extract image data.

        Returns:
        - None: The function does not return a value but saves the images as PNG files.
        """
        if output_dir is None and self.output_dir is None:
            # TODO: Write to log
            raise ValueError("Please indicate the output directory!")
        if topic not in self.bag_topics:
            topic_list_str = " ".join(self.bag_topics)
            # TODO: Write to log
            raise ValueError(f"Wrong ROS topic, please check again. Available topics are: {topic_list_str}")
        if output_dir is None:
            output_dir = self.output_dir
        output_depth_png_subdir = "depth/"+self.episode_id+'/'
        for msg_topic, msg, t in tqdm(self.bag.read_messages(topic), desc=f'Extracting {self.bag_name}'):
            timestamp_ros = f"{t.secs}.{t.nsecs:09d}"
            cv_image = self.bridge.imgmsg_to_cv2(msg, '16UC1')
            # create_path_to_output(os.path.join(output_dir, output_depth_png_subdir))
            cv2.imwrite(os.path.join(output_dir, output_depth_png_subdir, f"{timestamp_ros}.png"), cv_image)
            input_file = os.path.join(output_dir, output_depth_png_subdir, f"{timestamp_ros}.png")
            output_file = os.path.join(output_dir, output_depth_png_subdir, f"{timestamp_ros}.jxl")
            subprocess.run(
                ["cjxl", input_file, output_file, "-d", "0"], 
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            os.remove(input_file)
            # tar the folder
            subprocess.run(
                ["tar", "-cvf", f"{output_dir}/depth/{self.episode_id}.tar.gz", f"{output_dir}/{output_depth_png_subdir}"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            # remove the folder
            subprocess.run(
                ["rm", "-rf", f"{output_dir}/{output_depth_png_subdir}"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            

    def extract_vdmsg_bag_from_rosbag(self, topic, output_dir=None):
        """
        Extracts VDMSG data from a ROS bag file and saves it to a CSV file.

        Parameters:
        - topic (str): The ROS topic from which to extract VDMSG data.
        - output_dir (str, optional): The directory where the CSV file will be saved. If not provided, it uses `self.output_dir`.

        Returns:
        - None: The function does not return a value but saves the data to a CSV file.
        """
        if output_dir is None and self.output_dir is None:
            # TODO: Write to log
            raise ValueError("Please indicate the output directory!")
        
        if topic not in self.bag_topics:
            topic_list_str = " ".join(self.bag_topics)
            # TODO: Write to log
            raise ValueError(f"Invalid ROS topic. Available topics are: {topic_list_str}")
        
        if output_dir is None:
            output_dir = self.output_dir
        
        # Create the output directory and subdirectory for VDMSG data
        output_vdmsg_subdir = os.path.join(output_dir, "motionCapture")
        os.makedirs(output_vdmsg_subdir, exist_ok=True)
        output_csv = os.path.join(output_vdmsg_subdir, self.episode_id+".csv")
        
        title_list = None
        timestamp_list = []
        
        # Open the CSV file for writing
        with open(output_csv, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            
            # Iterate through the messages in the ROS bag file
            for msg_topic, msg, t in tqdm(self.bag.read_messages(topic), desc=f'Extracting {self.bag_name}'):
                # Extract the timestamp
                timestamp_ros = f"{t.secs}.{t.nsecs:09d}"
                timestamp_list.append(timestamp_ros)
                
                # Write the CSV header
                if title_list is None:
                    title_list = self.utils.extract_vdmsg_bag_position_msg_title_list("", "lhand_", "rhand_", msg)
                    csvwriter.writerow(['timestamp'] + title_list)
                
                # Extract frame data and write to CSV
                frame_data = self.utils.extract_vdmsg_bag_position_msg_frame_data(msg)
                csvwriter.writerow([timestamp_ros] + frame_data)

    def extract_all(self, output_dir=None, *topics):
        """
        Extracts data from multiple ROS topics and saves them in appropriate formats.

        Parameters:
        - output_dir (str): The directory path where the data will be saved.
        - topics (tuple): The ROS topics from which to extract data.

        Returns:
        - None: The function does not return a value but saves the data to files.
        """
        if output_dir is None and self.output_dir is None:
            # TODO: Write to log
            raise ValueError("Please indicate the output directory!")
        
        if output_dir is None:
            output_dir = self.output_dir

        for topic in topics:
            if topic not in self.bag_topics:
                # TODO: Write to log
                raise ValueError(f"{topic} not in {self.bag_name}")
        
        output_csv, csvfile = None, None
        timelist = []
        accel_data = []
        gyro_data = []
        for msg_topic, msg, t in tqdm(self.bag.read_messages(), desc=f'Extracting {self.bag_name}'):
            timestamp_ros = f"{t.secs}.{t.nsecs:09d}"
            
            if msg_topic == "/camera/accel/sample":
                # Extract IMU acceleration data
                output_imu_accel_subdir = "camera_accel/"
                linear_acceleration_x = msg.linear_acceleration.x
                linear_acceleration_y = msg.linear_acceleration.y
                linear_acceleration_z = msg.linear_acceleration.z
                accel_data.append([
                    timestamp_ros,
                    linear_acceleration_x, linear_acceleration_y, linear_acceleration_z
                ])

            elif msg_topic == "/camera/gyro/sample":
                # Extract IMU gyroscope data
                output_imu_gyro_subdir = "camera_gyro/"
                angular_velocity_x = msg.angular_velocity.x
                angular_velocity_y = msg.angular_velocity.y
                angular_velocity_z = msg.angular_velocity.z
                gyro_data.append([
                    timestamp_ros,
                    angular_velocity_x, angular_velocity_y, angular_velocity_z,
                ])

            elif msg_topic == "/camera/color/image_raw/compressed":
                # Extract color images
                output_color_jpeg_subdir = "color/"+self.episode_id+'/'
                cv_image = self.bridge.compressed_imgmsg_to_cv2(msg, "bgr8")
                create_path_to_output(os.path.join(output_dir, output_color_jpeg_subdir))
                cv2.imwrite(os.path.join(output_dir, output_color_jpeg_subdir, f"{timestamp_ros}.jpeg"), cv_image)
                timelist.append(f"{timestamp_ros}.jpeg")
                    
            elif msg_topic == "/camera/depth/image_raw":
                # Extract depth images
                output_depth_png_subdir = "depth/"+self.episode_id+'/'
                cv_image = self.bridge.imgmsg_to_cv2(msg, '16UC1')
                create_path_to_output(os.path.join(output_dir, output_depth_png_subdir))
                cv2.imwrite(os.path.join(output_dir, output_depth_png_subdir, f"{timestamp_ros}.png"), cv_image)
                input_file = os.path.join(output_dir, output_depth_png_subdir, f"{timestamp_ros}.png")
                output_file = os.path.join(output_dir, output_depth_png_subdir, f"{timestamp_ros}.jxl")
                subprocess.run(
                    ["cjxl", input_file, output_file, "-d", "0"], 
                    check=True,            
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
                
                os.remove(input_file)

            elif msg_topic == "/vdmsg":
                # Extract motion capture data
                if output_csv is None:
                    output_vdmsg_subdir = os.path.join(output_dir, "motionCapture")
                    os.makedirs(output_vdmsg_subdir, exist_ok=True)
                    output_csv = os.path.join(output_vdmsg_subdir, self.episode_id+".csv")
                    title_list = None
                    csvfile = open(output_csv, 'w', newline='')
                    csvwriter = csv.writer(csvfile)
                timestamp_list = []
                timestamp_ros = f"{t.secs}.{t.nsecs:09d}"
                timestamp_list.append(timestamp_ros)
                if title_list is None:
                    title_list = self.utils.extract_vdmsg_bag_position_msg_title_list("", "lhand_", "rhand_", msg)
                    csvwriter.writerow(['timestamp'] + title_list)
                frame_data = self.utils.extract_vdmsg_bag_position_msg_frame_data(msg)
                csvwriter.writerow([timestamp_ros] + frame_data)

        if os.path.exists(output_csv):
            csvfile.close()
            print("Data saved to CSV")
        else:
            print("No data saved to CSV")

        df = pd.DataFrame(accel_datadata, columns=[
            'ROS_Timestamp',
            'Linear_Acceleration_x', 'Linear_Acceleration_y', 'Linear_Acceleration_z'
        ])
        df.to_csv(os.path.join(output_dir, output_imu_accel_subdir, 'camera_accel.csv'), index=False)
        df = pd.DataFrame(gyro_data, columns=[
            'ROS_Timestamp',
            'Angular_Velocity_x', 'Angular_Velocity_y', 'Angular_Velocity_z',
        ])
        df.to_csv(os.path.join(output_dir, output_imu_gyro_subdir, self.episode_id+'.csv'), index=False)
        
        # tar the folder
        # subprocess.run(
        #     ["tar", "-cvf",f"../{self.episode_id}.tar",  f"."],
        #     cwd= "{output_dir}/{output_depth_png_subdir}",
        #     check=True,
        #     stdout=subprocess.DEVNULL,
        #     stderr=subprocess.DEVNULL
        # )
        # remove the folder
        subprocess.run(
            ["rm", "-rf", f"{output_dir}/{output_depth_png_subdir}"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        output_color_jpeg_subdir = "color/"+self.episode_id+'/'
        self.utils.compress_jpeg_to_mp4(timelist, os.path.join(output_dir, output_color_jpeg_subdir), os.path.join(output_dir, "color/"), self.episode_id)

