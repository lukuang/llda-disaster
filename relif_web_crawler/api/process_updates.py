"""
process the crawled updates
"""

import os
import json
import sys
import re
import argparse
import codecs
import html2text
from myUtility.misc import split_list 


def get_update_file_list(list_file):
    update_file_list = []
    with open(list_file) as f:
        for line in f:
            update_file_list.append(line.rstrip())
    return update_file_list


def process_update_file(update_file):
    data = json.load(open(update_file))['data']

    documents = []

    for single_update in data:
        single_document = {}
        single_document['id'] = single_update['id']
        fields = single_update['fields']
        
        # get text
        if "body-html" not in fields:
            #remove updates without body field
            continue
        else:
            body = fields['body-html'] 
            body = html2text.html2text(body)
            single_document['text'] = "%s\n%s" %(fields['title'],body)
        
        try:
            time_string = fields['date']['original'] 
        except KeyError:
            single_document['date'] = 'NA' 
        else:
            m = re.search("^\d+",time_string)
            single_document['date'] = m.group(0)




        single_document['type'] = {}
        

        # get disaster types
        if "disaster_type" in fields:
            single_document['type']['disaster'] = []

            for disaster_type_struct in fields['disaster_type']:
                single_document['type']['disaster'].append(disaster_type_struct['name'])


        # get theme
        if "theme" in fields:
            single_document['type']['theme'] = []

            for theme_struct in fields['theme']:
                single_document['type']['theme'].append(theme_struct['name'])

        # get content_formats
        if "format" in fields:
            single_document['type']['format'] = []

            for format_struct in fields['format']:
                single_document['type']['format'].append(format_struct['name'])

        # get vulnerable_groups
        if "vulnerable_groups" in fields:
            single_document['type']['vulnerable_groups'] = []

            for vulnerable_groups_struct in fields['vulnerable_groups']:
                single_document['type']['vulnerable_groups'].append(vulnerable_groups_struct['name'])

        documents.append(single_document)


    return documents


def get_article_path(update_file,ariticles_dir):
    dir_path, file_name = os.path.split(update_file)
    date_string = os.path.basename(dir_path)
    article_date_dir = os.path.join(ariticles_dir,date_string)
    if not os.path.exists(article_date_dir):
        os.mkdir(article_date_dir)

    article_path = os.path.join(article_date_dir,file_name)
    return article_path


def process_updates(sub_update_file_list,ariticles_dir):
    for update_file in sub_update_file_list:
        print "process file %s" %(update_file)
        documents = process_update_file(update_file)
        for document in documents:

            date_dir = os.path.join(ariticles_dir,document['date'])
            if not os.path.exists(date_dir):
                os.mkdir(date_dir)

            article_path = os.path.join(date_dir,document['id'])

            with open(article_path,"w") as f:
                f.write(json.dumps(document))



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("list_file")
    parser.add_argument("ariticles_dir")
    parser.add_argument("--num_of_runs",type=int,default=1)
    parser.add_argument("run_id",type=int)
    args=parser.parse_args()

    update_file_list = get_update_file_list(args.list_file)
    sub_update_file_list = split_list(update_file_list,args.num_of_runs,args.run_id)
    process_updates(sub_update_file_list,args.ariticles_dir)
    


if __name__=="__main__":
    main()

