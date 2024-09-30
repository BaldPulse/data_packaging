#!/usr/bin/python3

import os
import subprocess
import numpy as np

class Utils():
    def __init__(self):
        pass
        # print("Utils Init")  # Print a message when initializing

    # Extract position and quaternion messages and return a flattened array
    def extract_vdmsg_bag_position_msg(self, msg_position_location, msg_position_location_quaternion):
        temp = []
        # Extract position messages as a list of coordinates
        for position_msg in msg_position_location:
            temp.append([position_msg.x, position_msg.y, position_msg.z])   
        # Add quaternion messages to the corresponding coordinate list
        for index, quaternion_msg in enumerate(msg_position_location_quaternion):
            temp[index] = temp[index] + [quaternion_msg.x, quaternion_msg.y, quaternion_msg.z, quaternion_msg.w]
        # Convert the list to a 1D NumPy array
        temp = np.array(temp).reshape(1, -1)
        temp = temp[0]
        return temp

    # Generate a full list of names with suffixes based on a head name and a list of names
    def extract_vdmsg_bag_position_msg_expanded_name(self, msg_name_head, msg_name):
        suffixes = ['_x', '_y', '_z', '_qx', '_qy', '_qz', '_qw']  # List of suffixes
        msg_name_temp = list(msg_name)
        # If a head name is provided, concatenate it with the name list
        if msg_name_head is not None:
            msg_name_temp = [msg_name_head + name for name in msg_name_temp]
        # Add suffixes to each name
        expanded_name = [f"{name}{suffix}" for name in msg_name_temp for suffix in suffixes]
        return expanded_name

    # Extract frame data for body, left hand, and right hand from the message
    def extract_vdmsg_bag_position_msg_frame_data(self, msg):
        frame_data_temp = [] 
        # Extract data for body, right hand, and left hand
        body = self.extract_vdmsg_bag_position_msg(msg.position_body, msg.quaternion_body)
        rhand = self.extract_vdmsg_bag_position_msg(msg.position_rHand, msg.quaternion_rHand)
        lhand = self.extract_vdmsg_bag_position_msg(msg.position_lHand, msg.quaternion_lHand)

        # Combine data for body, left hand, and right hand
        frame_data_temp.extend(body)
        frame_data_temp.extend(lhand)
        frame_data_temp.extend(rhand)
        return frame_data_temp

    # Generate a list of titles, including expanded parameter names for body, left hand, and right hand
    def extract_vdmsg_bag_position_msg_title_list(self, expanded_body_param, expanded_lhand_param, expanded_rhand_param, msg):
        title_list_temp = None
        
        # Generate expanded name lists
        expanded_body_name = self.extract_vdmsg_bag_position_msg_expanded_name(expanded_body_param, msg.name_body)
        expanded_lhand_name = self.extract_vdmsg_bag_position_msg_expanded_name(expanded_lhand_param, msg.name_lHand)
        expanded_rhand_name = self.extract_vdmsg_bag_position_msg_expanded_name(expanded_rhand_param, msg.name_rHand)

        # Combine name lists for body, left hand, and right hand
        title_list_temp = expanded_body_name + expanded_lhand_name + expanded_rhand_name
        return title_list_temp

    def compress_images_to_mp4(self, input_timelist, input_dir, output_dir, epid, image_type='png'):
        command = (
                    f"ffmpeg -y -framerate 30 -pattern_type glob -i '{input_dir}/*.{image_type}' "
                    f"-c:v libx265 -pix_fmt yuv420p {input_dir}/data.mp4 "
                    f"-progress pipe:1 -nostats -loglevel 0"
                )
        subprocess.run(command, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        metadata_command = (
                    f"ffmpeg -i {input_dir}/data.mp4 "
                    f"-metadata comment=\"{input_timelist}\" -c copy {output_dir}/{epid}.mp4 "
                    f"-progress pipe:1 -nostats -loglevel 0"
                )
        subprocess.run(metadata_command, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        rm_command = (
            f"rm -rf {input_dir} "
        )
        subprocess.run(rm_command, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def extract_mp4_to_jpeg(self, input_mp4_dir, output_dir):
        # Create a directory for extracting JPEG images
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # 1. Extract JPEG images from the MP4 file
        print("Extracting JPEG images...")
        ffmpeg_extract_command = f"ffmpeg -i {input_mp4_dir} -vf \"fps=30\" {output_dir}/frame_%04d.jpeg"
        subprocess.run(ffmpeg_extract_command, shell=True, check=True)
        print("JPEG extraction completed.")

        # 2. Extract metadata from the MP4 file and store it in a list
        print("Extracting metadata...")
        ffmpeg_metadata_command = f"ffmpeg -i {input_mp4_dir} -f ffmetadata -"
        result = subprocess.run(ffmpeg_metadata_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Parse metadata
        metadata = result.stdout
        file_names = []

        # Parse the 'comment' field
        for line in metadata.splitlines():
            if line.startswith("comment="):
                comment = line[len("comment="):].strip()
                # Remove brackets and whitespace, then split the file names
                comment = comment.strip("[]")
                file_names = [name.strip().replace("'", "").replace(",", "") for name in comment.split()]

        # 3. Get all JPEG files in the folder and rename them
        print("Processing metadata and renaming JPEG images...")
        jpeg_files = sorted(f for f in os.listdir(output_dir) if f.endswith('.jpeg'))

        # Ensure the number of extracted JPEG files matches the number of file names in the metadata
        if len(jpeg_files) != len(file_names):
            raise ValueError("The number of JPEG files does not match the number of file names in the metadata.")

        # Iterate through JPEG files and rename them based on metadata
        for jpeg_file, new_name in zip(jpeg_files, file_names):
            old_path = os.path.join(output_dir, jpeg_file)
            new_path = os.path.join(output_dir, new_name)
            os.rename(old_path, new_path)

        print("All JPEG images have been renamed based on custom metadata.")

    def jxl_to_png(self, input_dir, output_dir):
        # Create the output directory (if not already created)
        os.makedirs(output_dir, exist_ok=True)

        # Iterate over all JPEG XL files in the input directory and decompress them to PNG files
        for filename in os.listdir(input_dir):
            if filename.endswith(".jxl"):
                # Construct the full file path
                file_path = os.path.join(input_dir, filename)
                
                # Extract the file name (excluding the extension)
                base_name = os.path.splitext(filename)[0]
                
                # Construct the output file path
                output_file = os.path.join(output_dir, f"{base_name}.png")
                
                # Execute the djxl command to decompress the file
                subprocess.run(["djxl", file_path, output_file])

        print(f"All JPEG XL files have been successfully decompressed to PNG files and stored in {output_dir}.")
