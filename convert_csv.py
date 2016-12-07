"""
convert csv file to the format used by other llda implementations
"""

import os
import json
import sys
import re
import argparse
import codecs
import csv

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_file")
    parser.add_argument("dest_file")
    parser.add_argument("label_file")
    parser.add_argument("-s","--seperator",type=str,default=" ")
    args=parser.parse_args()

    csv.field_size_limit(sys.maxsize)
    all_labels = {}
    k = 0
    with open(args.dest_file,"w") as of:
        with open(args.source_file,"rb") as f:
            spamreader = csv.reader(f)
            for row in spamreader:
                labels = row[1].split()
                label_indices = []
                for l in labels:
                    if l not in all_labels:
                        all_labels[l] = str(k)
                        k += 1
                    label_indices.append(all_labels[l])
                label_string = args.seperator.join(label_indices)
                text = row[2]
                document = " ".join(re.findall("\w+",text))
                of.write('[%s]%s\n' %(label_string,document))

    with open(args.label_file,"w") as lf:
        lf.write(json.dumps(all_labels,indent=2))

if __name__=="__main__":
    main()

