"""
Description: This file contains a function to write the ouput of query function
    to a json file for further processing.

@author: Robert Hennessy (robertghennessy@gmail.com)

"""

import file_functions as ff


import config
import data_collection_functions as dcf

def main():
        
    ff.create_directories([config.siri_json_dir, config.gtfs_rt_json_dir])
    dcf.write_transit_data_to_json(config.siri_json_dir, 'siri-',
                                   dcf.query_siri())
    dcf.write_transit_data_to_json(config.gtfs_rt_json_dir,'gtfs-rt-',
                               dcf.query_gtfs_rt())
      
if __name__ == '__main__':
    main()