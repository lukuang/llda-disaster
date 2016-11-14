"""
read disaster data
"""

import os
import json
import sys
import re
import argparse
import codecs

def read_single_instance(instance_dir):
    """Return data as {file_id:text}
    """
    instance_disaster_data = {}
    for date in os.walk(instance_dir).next()[1]:
        date_dir = os.path.join(instance_dir,date)
        for file_name in os.walk(date_dir).next()[2]:
            single_file = os.path.join(date_dir,file_name)
            file_id = date + "_" + file_name
            file_text = ""
            with open(single_file) as f:
                file_text = f.read()
                file_text = re.sub("\n"," ",file_text)
            instance_disaster_data[file_id] = file_text
    return instance_disaster_data


def read_single_disaster(disaster_dir):
    """Return data as disaster_name, {instance_name:{file_id:text}}
    """
    if disaster_dir[-1] == '/':
        disaster_dir = disaster_dir[:-1]
    disaster_name = os.path.basename(disaster_dir)

    single_disaster_data = {}
    for instance_name in os.walk(disaster_dir).next()[1]:
        instance_dir = os.path.join(disaster_dir,instance_name)
        single_disaster_data[instance_name] = read_single_instance(instance_dir)


    return disaster_name, single_disaster_data



def read_disaster_dirs(disaster_dirs):
    """Return data as {disaster_name:{instance_name:{file_id:text}} }
    """
    disaster_data = {}
    for single_dir in disaster_dirs:
        disaster_name, single_disaster_data = read_single_disaster(single_dir)
        disaster_data[disaster_name] = single_disaster_data
    return disaster_data


