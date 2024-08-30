from extract_rosbag import ExtractRosbag
import os
import uuid

# create the folder structure
output_modalities = ["motionCapture", "camera_accel", "camera_gyro", "color", "depth"]
bags = ["2024-08-01-11-21-49", "2024-08-01-11-19-42"]
output_folder = "./output"
for modality in output_modalities:
    modality_folder = os.path.join(output_folder, modality)
    if not os.path.exists(modality_folder):
        os.makedirs(modality_folder)
tstring = "2024-08-01-11-21-49"
# convert tstring to seconds from the year 1970
import datetime
import time
for name in bags:
    t = datetime.datetime.strptime(name, "%Y-%m-%d-%H-%M-%S")
    t = time.mktime(t.timetuple())
    uuid_str = str(uuid.uuid1(int(t)))
    extractbag = ExtractRosbag(f"/media/zhaotang/Files/Tools/data_packaging/test/{name}.bag", uuid_str, './output')
    extractbag.extract_all(None,"/vdmsg","/camera/accel/sample","/camera/gyro/sample","/camera/color/image_raw/compressed","/camera/depth/image_raw")