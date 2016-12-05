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
import subprocess
from abc import ABCMeta,abstractmethod


class Iterative_Crawler(object):
    """Abstract class for iteratively crawl the webpages from
    a starting url.
    """
    __metaclass__ = ABCMeta

    def __init__(self,starting_url,dest_dir,sleep_time=2,report_count=100):
        self._starting_url, self._dest_dir, self._sleep_time, self._report_count =\
            starting_url,dest_dir,sleep_time,report_count
        self._file_index = 0
        self._page_count = 0


    def start_crawling(self,params=[]):
        self._crawl(self._starting_url,params)


    def _crawl(self,url_now,params):
        print "Crawling %s" %(url_now)
        try:
            r = requests.get(url_now, params=params)
        except Exception as e:
            print e
            print time.ctime()
            if sys.platform.find('linux')!=-1:
                # if it is not linux system, just wait for 40 mins
                print "Wait 40 mins and try again"
                time.sleep(2400)
            else:
                # if it is linux system, check if wifi is on
                command = ["nmcli",'nm','wifi']
                p1 = subprocess.Popen(command,stdout=subprocess.PIPE)
                output=p1.communicate()[0]
                if output.find('enabled')!=-1:
                    # if wifi is on, turn it off/on and try again
                    print "Turn off wifi"
                    os.system('nmcli nm wifi off')
                    time.sleep(300)
                    print "Turn on wifi" 
                    os.system('nmcli nm wifi on')
                    time.sleep(300)
                else:
                    time.sleep(2400)
                

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

