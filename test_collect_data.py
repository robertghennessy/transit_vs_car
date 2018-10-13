"""
Description: This file contains a function to write the ouput of query function
    to a json file for further processing.

@author: Robert Hennessy (robertghennessy@gmail.com)

"""

import datetime as dt
import json
import os

import config
import data_collection_functions as dcf


def write_data_to_json():
    dateTag = dt.datetime.now().strftime("%Y-%b-%d_%H-%M-%S")       
    siriFileName = 'siri-' + dateTag + '.json'
    siriFileName = os.path.join(config.test_file_dir, siriFileName)
    with open(siriFileName, 'w') as outfile:
        json.dump(dcf.query_siri(), outfile)
    gtfs_rtFileName = 'gtfs-rt-' + dateTag + '.json'
    gtfs_rtFileName = os.path.join(config.test_file_dir, gtfs_rtFileName)
    with open(gtfs_rtFileName, 'w') as outfile:
        json.dump(dcf.query_gtfs_rt(), outfile)



def main():
    write_data_to_json()
      
if __name__ == '__main__':
    main()