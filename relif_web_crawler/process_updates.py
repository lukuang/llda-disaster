"""
process the crawled updates
"""

import os
import json
import sys
import re
import argparse
import codecs
from myUtility.misc import split_list 
from bs4 import BeautifulSoup

from myUtility.parser import Html_parser

def get_update_file_list(list_file):
    update_file_list = []
    with open(list_file) as f:
        for line in f:
            update_file_list.append(line.rstrip())
    return update_file_list


def process_update_file(update_file,parser):
    content = open(update_file).read()
    soup = BeautifulSoup(content,'lxml')
    document = {}
    document['types'] = {}

    # get disaster types
    disaster_type_block = soup.find('div',class_="views-field-field-disaster-type")
    if disaster_type_block:
        disaster_types = []
        for disaster_item in disaster_type_block.find('div',class_='field-content').find('ul').findAll('li'):
            disaster_type_string = re.sub("[^\w]+","_",disaster_item.a.text.lower())
            disaster_types.append(disaster_type_string)

        document['types']['disaster_types'] = disaster_types


    # get theme
    theme_block = soup.find('div',class_="views-field-field-theme")
    if theme_block:
        # get disaster types
        themes = []
        for theme_item in theme_block.find('div',class_='field-content').find('ul').findAll('li'):
            theme_string = re.sub("[^\w]+","_",theme_item.a.text.lower())
            themes.append(theme_string)

        document['types']['themes'] = themes

    # get content_formats
    content_format_block = soup.find('div',class_="views-field-field-content-format")
    if content_format_block:
        # get disaster types
        content_formats = []
        for content_format_item in content_format_block.find('div',class_='field-content').find('ul').findAll('li'):
            content_format_string = re.sub("[^\w]+","_",content_format_item.a.text.lower())
            content_formats.append(content_format_string)

        document['types']['content_formats'] = content_formats

    # get vulnerable_groups
    vulnerable_group_block = soup.find('div',class_="views-field-field-vulnerable-groups")
    if vulnerable_group_block:
        # get disaster types
        vulnerable_groups = []
        for vulnerable_group_item in vulnerable_group_block.find('div',class_='field-content').find('ul').findAll('li'):
            vulnerable_group_string = re.sub("[^\w]+","_",vulnerable_group_item.a.text.lower())
            vulnerable_groups.append(vulnerable_group_string)

        document['types']['vulnerable_groups'] = vulnerable_groups


    # get text
    document['text'] = parser.get_text(update_file)


    return document


def get_article_path(update_file,ariticles_dir):
    dir_path, file_name = os.path.split(update_file)
    date_string = os.path.basename(dir_path)
    article_date_dir = os.path.join(ariticles_dir,date_string)
    if not os.path.exists(article_date_dir):
        os.mkdir(article_date_dir)

    article_path = os.path.join(article_date_dir,file_name)
    return article_path


def process_updates(sub_update_file_list,ariticles_dir,parser):

    for update_file in sub_update_file_list:
        document = process_update_file(update_file,parser)
        if not document['text']:
            print "Did not find text in %s" %(update_file )

        article_path = get_article_path(update_file,ariticles_dir)

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
    parser = Html_parser(False)
    sub_update_file_list = split_list(update_file_list,args.num_of_runs,args.run_id)
    process_updates(sub_update_file_list,args.ariticles_dir,parser)
    


if __name__=="__main__":
    main()

