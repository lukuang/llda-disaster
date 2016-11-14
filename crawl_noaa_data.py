"""
Crawl the data from noaa. First, retrieve the site contains all data and crawl individual storm detail files in the site
"""

import os
import json
import sys
import re
import argparse
import codecs
# from myUtility.misc import crawl_url
from ftplib import FTP

def crawl_storm_events(site_addr,site_event_dir,dest_dir):
    event_list = []

    ftp = FTP(site_addr)
    ftp.login()
    ftp.cwd(site_event_dir)
    file_list = ftp.nlst()
    for single_file in file_list:
        if single_file.find("details") != -1:
            year_event_file = os.path.join(dest_dir,single_file)
            try:
                with open(year_event_file,"wb") as f:
                    ftp.retrbinary('RETR %s' % single_file, f.write)
            except Exception as e:
                print "Erro when retrieving %s" %year_event_file
                print e
            else:
                print "retrieved %s" %single_file
    




def crawl_storm_events(event_list,dest_dir):
    pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site_addr","-sa",default="ftp.ncdc.noaa.gov")
    parser.add_argument("--site_event_dir","-sed",default="/pub/data/swdi/stormevents/csvfiles/")
    parser.add_argument("dest_dir")
    args=parser.parse_args()

    event_list = crawl_storm_events(args.site_addr,args.site_event_dir,args.dest_dir)



if __name__=="__main__":
    main()

