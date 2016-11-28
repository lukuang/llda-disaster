"""
crawl update list from releif web http://reliefweb.int/
"""

import os
import json
import sys
import re
import argparse
import codecs
from bs4 import BeautifulSoup

from iterative_crawler import Iterative_Crawler

reload(sys)
sys.setdefaultencoding('utf-8')


STARTING_URL = 'http://reliefweb.int/updates/no-thumb?language=267'

class Relief_Web_Crawler(Iterative_Crawler):
    """Iteratively crawl lists of reports
    """
    # def __init__(self,starting_url,dest_dir,sleep_time=5,report_count=100):
    #     super(Relief_Web_Crawler,self).__init__(starting_url,dest_dir,sleep_time,report_count)
    #     self._load_records()

    # def _load_records(self):
    #     self._record_file = os.path.join(self._dest_dir,"records")
    #     self._records = {}

    #     if os.path.exists(self._record_file):
    #         self._records = json.load(open(self._record_file))

    def _process_content(self,content,params):
        self._save_content(content)
        page = BeautifulSoup(content,'lxml')
        next_params = params
        next_url = None
        if page.find('li', class_='pager-next').find('a'):
            next_url = page.find('li', class_='pager-next').a['href']
            # m = re.search("page=(\d+)",last_url)
            # if m:
            #     biggest_page_id = int(m.group(1))
            # if self._page_count <  biggest_page_id:
            #     next_url = "http://reliefweb.int/updates/no-thumb?language=267&page=%d" %(self._page_count)
            print next_url
        return next_url, next_params


    def _save_content(self,content):
        result_file = os.path.join(self._dest_dir,str(self._file_index))
        with open(result_file,"w") as f:
            f.write(content)
        self._file_index += 1
        if (self._page_count==10):
            self._stop()




def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dest_dir")
    args=parser.parse_args()

    relief_web_crawler = Relief_Web_Crawler(STARTING_URL,args.dest_dir)    
    relief_web_crawler.start_crawling()




if __name__=="__main__":
    main()

