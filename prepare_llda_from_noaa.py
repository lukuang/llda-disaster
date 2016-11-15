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

Sinlge_LLDA_Document = namedtuple('Sinlge_LLDA_Document', ['did', 'labels','text'])

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

def read_noaa_data(noaa_dir,use_disaster_cate,document_type):
    if document_type == 0:
        return read_data_use_event(noaa_dir,use_disaster_cate)
    elif document_type == 1:
        return read_data_use_episode(noaa_dir,use_disaster_cate)
    else:
        raise NotImplementedError("The document type %d is not implemented" %(document_type) )

def read_data_use_event(noaa_dir,use_disaster_cate):
    # event_data = []
    # episode_data = []
    noaa_data = []
    episode_ids = {}
    for year_csv_file in os.walk(noaa_dir).next()[2]:
        year_csv_file = os.path.join(noaa_dir,year_csv_file)
        print year_csv_file
        with open(year_csv_file,"rb") as f:
            reader = csv.DictReader(f)
            for row in reader:
                episode_id = get_data_from_cell(row,"episode_id")
                event_id = get_data_from_cell(row,"event_id")
                event_type_string = get_data_from_cell(row,"event_type")
                event_types = process_event_type_string(event_type_string)
                
                episode_narrative = get_data_from_cell(row,"episode_narrative")
                event_narrative = get_data_from_cell(row,"event_narrative")

                if episode_id not in episode_ids:

                    if len(episode_narrative) != 0:


                        episode_ids[episode_id] = 0
                        did = episode_id

                        labels = " ".join(event_types+[episode_id])
                        if use_disaster_cate:
                            labels += " %s" %("disaster")

                        text = episode_narrative
                        noaa_data.append( Sinlge_LLDA_Document._make([did,labels,text]) )

                        # episode_data.append( Sinlge_LLDA_Document._make(did,labels,text) )
                if len(event_narrative)==0:
                    continue

                did = "%s_%s" %(episode_id,event_id)
                labels = " ".join(event_types+[episode_id])
                if use_disaster_cate:
                    labels += " %s" %("disaster") 
                text = event_narrative
                noaa_data.append( Sinlge_LLDA_Document._make([did,labels,text]) )
                # event_data.append( Sinlge_LLDA_Document._make(did,labels,text) )
                
        

    # print "There are %d events and %d episodes" %(len(event_data),len(episode_data))
    print "There are %d documents" %(len(noaa_data))
    return noaa_data

def read_data_use_episode(noaa_dir,use_disaster_cate):
    episode_data = {}
    all_lables = set()
    for year_csv_file in os.walk(noaa_dir).next()[2]:
        year_csv_file = os.path.join(noaa_dir,year_csv_file)
        print year_csv_file
        with open(year_csv_file,"rb") as f:
            reader = csv.DictReader(f)
            for row in reader:
                episode_id = get_data_from_cell(row,"episode_id")
                event_id = get_data_from_cell(row,"event_id")

                event_type_string = get_data_from_cell(row,"event_type")
                event_types = process_event_type_string(event_type_string)
                
                for e_type in event_types:
                    all_lables.add(e_type)
                episode_narrative = get_data_from_cell(row,"episode_narrative")
                event_narrative = get_data_from_cell(row,"event_narrative")
                
                if len(episode_narrative) == 0 and len(event_narrative)==0:
                    continue

                if not episode_id.isdigit():
                    continue
                if episode_id not in episode_data:

                    episode_data[episode_id]= {
                        "labels":event_types,
                        "episode":episode_narrative,
                        "event":event_narrative
                    }

                else:


                    if len(episode_narrative) != 0 and len(episode_data[episode_id]["episode"]) == 0:
                        episode_data[episode_id]["episode"] = episode_narrative
                    if len(event_narrative) != 0:
                        episode_data[episode_id]["event"] += " %s" %(event_narrative)
                    for e_type in event_types:
                        if e_type not in episode_data[episode_id]["labels"]:
                            episode_data[episode_id]["labels"].append(e_type)
                        
    noaa_data = []
    for episode_id in episode_data:


        did = episode_id
        labels = " ".join(episode_data[episode_id]["labels"])
        if use_disaster_cate:
            labels += " %s" %("disaster") 
        text = "%s %s" %(episode_data[episode_id]["episode"],
                         episode_data[episode_id]["event"])
        
        noaa_data.append( Sinlge_LLDA_Document._make([did,labels,text]) )
        # event_data.append( Sinlge_LLDA_Document._make(did,labels,text) )
                
        

    # print "There are %d events and %d episodes" %(len(event_data),len(episode_data))
    print "There are %d documents" %(len(noaa_data))
    print all_lables
    return noaa_data


def write_llda_input(noaa_data, dest_file,process,stopword_handler):
    with open(dest_file,'wb') as f:
        spamwriter = csv.writer(f)
        for single_document in noaa_data:
            if process:
                document_text = stopword_handler.remove_stopwords(single_document.text.lower())
                new_text = ""
                for term in re.findall("\w+",document_text):
                    new_text += " %s" %(stem(term))

                spamwriter.writerow(
                        [   
                            single_document.did,
                            single_document.labels,
                            new_text
                        ]
                    )
            else:
                spamwriter.writerow(
                        [   
                            single_document.did,
                            single_document.labels,
                            single_document.text
                        ]
                    )




def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("noaa_dir")
    parser.add_argument("dest_file")
    parser.add_argument("--use_disaster_cate","-ud",action='store_true')
    parser.add_argument("--process","-p",action='store_true')
    parser.add_argument("--document_type","-dt",type=int, choices=range(2),default=0,
        help="""
            Chose the type of document that will be used:
            0:event
            1:episode
        """)
    args=parser.parse_args()

    if args.process:
        stopword_handler = Stopword_Handler()
    else:
        stopword_handler = None
    noaa_data = read_noaa_data(args.noaa_dir,args.use_disaster_cate,args.document_type)
    write_llda_input(noaa_data, args.dest_file,args.process,stopword_handler)

if __name__=="__main__":
    main()

