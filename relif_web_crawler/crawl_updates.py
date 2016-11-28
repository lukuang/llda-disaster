"""
get updates from update list
"""

import os
import json
import sys
import re
import argparse
import codecs
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime

reload(sys)
sys.setdefaultencoding('utf-8')


class Update_Crawler(object):
    def __init__(self,update_list_dir,updates_dir,sleep_time=5,report_count=100):
        self._update_list_dir,self._updates_dir, self._sleep_time, self._report_count =\
            update_list_dir,updates_dir,sleep_time,report_count

        self._page_count = 0
        self._load_records()

    def _load_records(self):
        self._record_file = os.path.join(self._updates_dir,"records")
        self._records = {}

        if os.path.exists(self._record_file):
            self._records = json.load(open(self._record_file))

    def get_update_list(self):
        self._links = {}
        link_count = 0
        for update_list_file in os.walk(self._update_list_dir).next()[2]:
            
            if update_list_file.isdigit():
                print "process file %s" %(update_list_file)
                update_list_file = os.path.join(self._update_list_dir,update_list_file)
                content = open(update_list_file).read()
                soup = BeautifulSoup(content,'lxml')
                for item in soup.find('div', class_='river-list').findAll('div',class_='item'):
                    raw_date_string = item.span.text
                    date_string = datetime.strptime(raw_date_string,"%d %b %Y").strftime("%Y-%m-%d") 
                    title = item.find("div",class_="title")
                    divs = title.findAll("div")
                    [div.extract() for div in divs] 
                    if date_string not in self._links:
                            self._links[date_string] = []
                    self._links[date_string].append(title.a["href"])
                    link_count += 1
        print "Finish get update list"
        print "There are %d updates" %(link_count)
        #print self._links



    def crawl_updates(self):
        for date_string in self._links:
            file_index = 0
            for link in self._links[date_string]:
                url = "http://reliefweb.int" +link
                content = self._crawl(url,[])
                if content:
                    self._save_content(file_index,date_string,content)
                    file_index += 1
                    if link not in self._records:
                        self._records[link] = 0
                    else:
                        print "Failed to crawl %s" %(url)
        
        self._stop()



    def _crawl(self,url_now,params):
        #print "Crawling %s" %(url_now)
        time.sleep(5)
        r = requests.get(url_now, params=params)
        content = r.content
        self._page_count += 1

        if (self._page_count%self._report_count == 0):
            print "Crawled %d pages" %(self._page_count)
            self._save_records()

        return content

    def _stop(self):
        print "-"*20
        print "Finished crawling"
        print "Crawled %d pages" %(self._page_count)


    def _save_content(self,file_index,date_string,content):
        date_dir = os.path.join(self._updates_dir,date_string)
        if not os.path.exists(date_dir):
            os.mkdir(date_dir)
        dest_file = os.path.join(date_dir,str(file_index))
        with open(dest_file,"w") as f:
            f.write(content)

    def  _save_records(self):
        with open(self._record_file,"w") as f:
            f.write(json.dumps(self._records))



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("update_list_dir")
    parser.add_argument("updates_dir")
    args=parser.parse_args()

    update_crawler = Update_Crawler(args.update_list_dir,args.updates_dir)
    update_crawler.get_update_list()
    update_crawler.crawl_updates()


if __name__=="__main__":
    main()

