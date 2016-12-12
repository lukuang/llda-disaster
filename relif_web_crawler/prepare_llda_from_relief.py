"""
prepare llda data from noaa data
"""

import os
import json
import sys
import re
import argparse
import codecs
import csv 
from collections import namedtuple
#from myStemmer import pstem as stem
from myUtility.misc import Stopword_Handler
from stemming.porter2 import stem

reload(sys)
sys.setdefaultencoding('utf-8')

Sinlge_LLDA_Document = namedtuple('Sinlge_LLDA_Document', ['ids','text'])

def get_data_from_cell(row,field_name):
    cell_data = row[field_name.upper()]
    cell_data = re.sub("\n"," ",cell_data)
    cell_data = re.sub("\""," ",cell_data)

    return cell_data


def process_event_type_string(event_type_string):
    #origin = event_type_string
    
    event_type_string = re.sub("/ ","/",event_type_string.lower())
    event_type_string = re.sub(", ?","/",event_type_string)
    event_type_string = re.sub("tstm","thunderstorm",event_type_string)
    event_types = event_type_string.split("/")
    for i in range(len(event_types)):
        event_types[i] = re.sub("\s","_",event_types[i])

    return event_types

def read_relief_web_data(document_dir,use_disaster_cate,type_used,process,stopword_handler):
    
    relief_web_data = []
    labels = {}
    for category in type_used:
        labels[category] = {}

    label_index = 0

    if use_disaster_cate:
        labels["general_disaster"] = str(label_index)
        label_index += 1


    for year in os.walk(document_dir).next()[1]:
        year_dir = os.path.join(document_dir,year)
        print year_dir
        for document_name in os.walk(year_dir).next()[2]:
            document_file = os.path.join(year_dir,document_name)
            document = json.load(open(document_file))
            document_topic_ids = []

            for category in document['type']:
                if category in type_used:
                    for label in document['type'][category]:
                        if label not in labels[category]:
                            labels[category][label] = str(label_index)
                            label_index += 1
                        document_topic_ids.append(labels[category][label]) 

            #ignore documents without needed labels
            if not document_topic_ids:
                continue

            document_topic_ids.append("0")
            document_text = re.sub("\n"," ",document['text'])
            if process:
                document_text = stopword_handler.remove_stopwords(document['text'].lower())
                new_text = ""
                for term in re.findall("\w+",document_text):
                    new_text += " %s" %(stem(term)) 
                document_text = new_text       
            
            relief_web_data.append( Sinlge_LLDA_Document._make([document_topic_ids,document_text]) )

    # print "There are %d events and %d episodes" %(len(event_data),len(episode_data))
    print "There are %d documents" %(len(relief_web_data))
    return relief_web_data, labels




def write_llda_input(relief_web_data, labels, dest_dir):
    training_data_dest_file = os.path.join(dest_dir,'training_data')
    with open(training_data_dest_file,'wb') as f:
        spamwriter = csv.writer(f)
        for single_document in relief_web_data:
            ids_string = " ".join(single_document.ids)
            f.write('[%s]%s\n' %(ids_string,single_document.text))

    label_file = os.path.join(dest_dir,'labels')
    with open(label_file,"w") as f:
        f.write(json.dumps(labels,indent=4))
            




def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("document_dir")
    parser.add_argument("dest_dir")
    parser.add_argument("--use_disaster_cate","-ud",action='store_true')
    parser.add_argument("--process","-p",action='store_true')
    parser.add_argument("--type_used","-tu",nargs='+',
                        default=["disaster",'theme','vulnerable_groups'])
    args=parser.parse_args()

    if args.process:
        stopword_handler = Stopword_Handler()
    else:
        stopword_handler = None
    relief_web_data, labels = read_relief_web_data(args.document_dir,args.use_disaster_cate,
                                           args.type_used,args.process,
                                           stopword_handler)
    write_llda_input(relief_web_data, labels, args.dest_dir)

if __name__=="__main__":
    main()

