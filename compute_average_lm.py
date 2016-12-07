"""
Compute the average language model from csv files for each topic
"""

import os
import json
import sys
import re
import argparse
import codecs
import csv
from myUtility.corpus import Model 

def read_csv_file(source_file):
    topic_models_lists = {}
    with open(source_file,"rb") as f:
        spamreader = csv.reader(f)
        for row in spamreader:
            labels = row[1].split()
            single_model = Model(remove_stopwords=False,text_string=row[2],
                                 need_stem=True, input_stemmed=True)
            single_model.to_dirichlet()
            for topic in labels:
                if topic not in topic_models_lists:
                    topic_models_lists[topic] = []
                
                topic_models_lists[topic].append(single_model)

    topic_models = {}
    for topic in topic_models_lists:
        topic_models[topic] = Model(remove_stopwords=False,
                                    need_stem=True, input_stemmed=True)
        for single_model in topic_models_lists[topic]:
            topic_models[topic] += single_model

        topic_models[topic].to_dirichlet()

    print "Finished Reading models"
    return topic_models


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_file")
    parser.add_argument("topic_dest_file")
    parser.add_argument("--top_term_count","-tc",type=int,default=100)
    args=parser.parse_args()

    topic_models = read_csv_file(args.source_file)

    with open(args.topic_dest_file,"w") as f:
        for topic in topic_models:
            f.write("Topic %s:\n" %(topic) )
            for w,score in topic_models[topic].model.most_common(args.top_term_count):
                f.write("\t %s: %f\n" %(w,score) )

if __name__=="__main__":
    main()

