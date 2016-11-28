"""
abstract class for iteratively crawl
"""

import os
import json
import sys
import re
import argparse
import codecs
import requests
import time
from abc import ABCMeta,abstractmethod


class Iterative_Crawler(object):
    """Abstract class for iteratively crawl the webpages from
    a starting url.
    """
    __metaclass__ = ABCMeta

    def __init__(self,starting_url,dest_dir,sleep_time=5,report_count=100):
        self._starting_url, self._dest_dir, self._sleep_time, self._report_count =\
            starting_url,dest_dir,sleep_time,report_count
        self._file_index = 0
        self._page_count = 0


    def start_crawling(self,params=[]):
        self._crawl(self._starting_url,params)


    def _crawl(self,url_now,params):
        #print "Crawling %s" %(url_now)
        r = requests.get(url_now, params=params)
        content = r.content
        self._page_count += 1
        next_url , next_params = self._process_content(content,params)
        if (self._page_count%self._report_count == 0):
            print "Crawled %d pages" %(self._page_count)
        if next_url:
            time.sleep(5)
            self._crawl(next_url , next_params)
        else:
            self._stop()

    def _stop(self):
            print "-"*20
            print "Finished crawling"
            print "Crawled %d pages" %(self._page_count)


    @abstractmethod
    def _process_content(self, content, params):
        self._save_content(content)

    @abstractmethod
    def  _save_content(self, content):
        pass









if __name__=="__main__":
    main()

