"""
Description: This file is used to test sql_functions.

@author: Robert Hennessy (robertghennessy@gmail.com)
"""


import datetime as dt
import json
import os
import pandas as pd

import config
import data_collection_functions as dcf
import file_functions as ff
import sql_functions as sf
import test_table_def as ttd

# columns to compare
columns_to_compare = ['TrainStartDate', 'trip_id', 'stop_id']

# delete test database
ff.remove_files([config.test_siri_data_sql])

# create the siri table
sf.create_transit_data_siri_table(config.siri_table_name, 
                                  config.test_siri_data_sql)
# determine the files to import
file_to_import = ff.find_files_that_filename_contain(config.test_file_dir, 
                                                     'siri')
schedule_monitor = pd.read_csv(config.schedule_monitor_csv, 
                               index_col=0)
schedule_monitor.set_index(['trip_id','stop_id'], inplace=True)
time_index = 0
for file in file_to_import:
    # import the json file    
    with open(file, 'r') as f:
        data = json.load(f)
    # convert json to pandas
    parsed_data = dcf.parse_siri_transit_data(data, time_index)
    parsed_data_with_delays = dcf.compare_actual_to_schedule(parsed_data, 
                                                         schedule_monitor)
    # try updating entries
    sf.update_entries(config.test_siri_data_sql,
                      config.siri_table_name, parsed_data_with_delays, 
                      columns_to_compare)
    time_index = time_index + 1
    

conn = sf.create_connection(config.test_siri_data_sql)
cursor = conn.cursor()
parsed_data_with_delays =  sf.prepare_pandas_to_sql(parsed_data_with_delays, 
                                                    ttd.gtfs_dict)
# copy the data to a temporary table
parsed_data_with_delays.to_sql('temp', conn, if_exists='replace', index=False)