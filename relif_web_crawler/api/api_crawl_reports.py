"""
crawl reports using api
"""

import os
import json
import sys
import re
import argparse
import codecs

from iterative_crawler import Iterative_Crawler

reload(sys)
sys.setdefaultencoding('utf-8')


STARTING_URL = 'http://api.reliefweb.int/v1/reports?appname=udel_infolab&limit=1000'
PARA_SUBFIX = '&fields[include][]=disaster_type&fields[include][]=theme&fields[include][]=vulnerable_groups&fields[include][]=format&fields[include][]=body'

class Relief_Web_Api_Crawler(Iterative_Crawler):    
    """Iteratively crawl lists of reports using api
    """
    def __init__(self,starting_url,dest_dir,limit=1000,sleep_time=5,report_count=100):
        super(Relief_Web_Api_Crawler,self).__init__(starting_url,dest_dir,sleep_time,report_count)
        self._limit = limit

    def shift_start(self):
        """Change the starting page
        """
        crawled_files = [int(x) for x in os.walk(self._dest_dir).next()[2]]
        start_from = max(crawled_files) + 1000
        print "Start from %d" %start_from
        self._file_index = start_from
        self._starting_url = "%s%s&offset=%d" %(self._starting_url,PARA_SUBFIX,start_from)


    def _process_content(self, content, params):
        data = json.loads(content)
        now_url_prefix = data['links']['self'].values()[0]
        m = re.search("offset=(\d+)",now_url_prefix)
        page_id = str(m.group(1))
        self._save_content(data,page_id)

        if "next" not in data['links']:
            return None, params
        else:
            next_url_prefix = data['links']['next'].values()[0]
            next_url = "%s%s" %(next_url_prefix,PARA_SUBFIX)
            if (self._page_count%self._limit==0 and self._page_count!=0):
                print "Sleep a day to not exceed the quota"
                time.sleep(3600*24)
            return next_url, params


    def  _save_content(self, data,page_id):
        dest_file = os.path.join(self._dest_dir,page_id)
        with open(dest_file,'w') as f:
            f.write(json.dumps(data))



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dest_dir")
    parser.add_argument("--start_from",'-sf',action='store_true')
    args=parser.parse_args()

    relief_web_api_crawler = Relief_Web_Api_Crawler(STARTING_URL,args.dest_dir)    

    if args.start_from:
        relief_web_api_crawler.shift_start()

    relief_web_api_crawler.start_crawling()


if __name__=="__main__":
    main()

