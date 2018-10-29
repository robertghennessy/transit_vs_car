# -*- coding: utf-8 -*-
"""
Created on Sun Oct 28 10:55:10 2018

@author: Robert
"""

import os
import sqlite3

import config
import file_functions as ff
import sql_functions as sf


columns_to_compare = ['TrainStartDate', 'trip_id', 'stop_id', 'AimedDepartureTime_utc']   

# create temp file
cmp_transit_data_sql = os.path.join(config.test_file_dir, 
                                  'compare_transit_data.sqlite')
ff.remove_files([cmp_transit_data_sql])

sf.create_transit_data_siri_table(config.siri_table_name, 
                                  cmp_transit_data_sql)

sf.create_transit_data_gtfs_rt_table(config.gfts_rt_table_name,
                                     cmp_transit_data_sql)
# copy the data into the temp table        
sql_attach = ("ATTACH DATABASE '%s' AS %s" % (config.siri_data_sql, 'siri_data'))
sql_copy = ('INSERT INTO %s SELECT * FROM %s.%s' %(config.siri_table_name,
                                                   'siri_data',
                                                   config.siri_table_name))
sql_attach2 = ("ATTACH DATABASE '%s' AS %s" % (config.gtfs_rt_data_sql, 'gtfs_rt_data'))
sql_copy2 = ('INSERT INTO %s SELECT * FROM %s.%s' %(config.gfts_rt_table_name,
                                                   'gtfs_rt_data',
                                                   config.gfts_rt_table_name))
sql_cmd = sf.delete_entries_in_common(config.siri_table_name, 
                                      config.gfts_rt_table_name, 
                                      columns_to_compare)

                            
try:
    conn = sf.create_connection(cmp_transit_data_sql)
    cursor = conn.cursor()
    cursor.execute(sql_attach)
    cursor.execute(sql_copy)
    cursor.execute(sql_attach2)
    cursor.execute(sql_copy2)
    cursor.execute(sql_cmd)
finally:
    if conn:
        conn.close()