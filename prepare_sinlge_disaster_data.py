"""
prepare for llda on single disaster. More specifically, the labels of
every document is "instance_name,disaster_name"
"""

import os
import json
import sys
import re
import argparse
import codecs
import csv
from myStemmer import pstem as stem
from myUtility.misc import Stopword_Handler
from read_disaster_data import read_single_disaster


def write_to_file(disaster_name,single_disaster_data,dest_file,process,stopword_handler):
    

    with open(dest_file,'wb') as f:
        spamwriter = csv.writer(f)
        for instance_name in single_disaster_data:
            for file_id in single_disaster_data[instance_name]:
                file_text = single_disaster_data[instance_name][file_id]
                if process:
                    file_text = stopword_handler.remove_stopwords(file_text)
                    new_text = ""
                    for term in re.findall("\w+",file_text.lower()):
                        new_text += " %s" %(stem(term))
                    file_text = new_text
                file_id = "%s_%s" %(instance_name,file_id)
                labels = " ".join([disaster_name,instance_name])
                spamwriter.writerow([file_id,labels,file_text])

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("disaster_dir")
    parser.add_argument("dest_file")
    parser.add_argument("--process","-p",action='store_true')
    args=parser.parse_args()

    disaster_name, single_disaster_data = read_single_disaster(args.disaster_dir)
    if args.process:
        stopword_handler = Stopword_Handler()
    else:
        stopword_handler = None
    
    write_to_file(disaster_name,single_disaster_data,args.dest_file,args.process,stopword_handler)


if __name__=="__main__":
    main()

