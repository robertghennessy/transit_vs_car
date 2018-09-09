"""
Description: This program is used to test the scheduler. It contains a dummy
function to replace the query google maps and a program to create a subset 
of the trip csv file

@author: Robert Hennessy (robertghennessy@gmail.com)
"""

import datetime as dt
import sys
import sql_functions as sf
import os
import config
import pandas as pd
import scheduler_functions as sched
import logging
import push_notification as pn
import data_collection_functions as dcf
import other_functions as of

# set up the root logger
logger = logging.getLogger('')
logger.setLevel(logging.INFO)
dateTag = dt.datetime.now().strftime("%Y-%b-%d_%H-%M-%S")
path = config.test_transit_log_file % dateTag
# add a rotating handler, rotate the file every 10 mb
handler = logging.handlers.RotatingFileHandler(path, maxBytes=10*1024*1024,
                              backupCount=5)
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def create_tst_csv(csv_file_loc,test_csv_file_loc):
    """
    Creates a test csv file to be used to test the scheduler. The program
    samples the first twenty rows. To ensure that the scheduler starts quickly,
    the current weekday is set to true and the deparature time is set to the
    current time plus a small delta. 
    
    param: csv_file_loc: location of the csv file generated by parse_gtfs
    :type: csv_file_loc: string
    
    param: test_csv_file_loc: location of the modified csv file generated
    by this program
    :type: csv_file_loc: string  
    
    return: None
    """
    # number of rows in the tst csv
    num_of_rows_in_tst_csv = 5
    # determine the current day of the week
    day_of_week = dt.datetime.today().weekday()
    # determine the current time
    current_time = dt.datetime.now()
    # read in the schedule trips csv
    schedule_trips = pd.read_csv(config.trips_csv, index_col=0)
    # select the number of rows    
    rows_to_drop = schedule_trips.index[range(num_of_rows_in_tst_csv,
                                              len(schedule_trips))]   
    tst_schedule_trips = schedule_trips.drop(rows_to_drop) 
    # set the current day to 1 for the scheduler    
    tst_schedule_trips[config.weekday_names[day_of_week]] = 1
    # convert the deparature time timedeltas  
    tst_schedule_trips['departure_time_timedelta_start'] = pd.to_timedelta(
        tst_schedule_trips['departure_time_timedelta_start'])
    # subtract off the timedelta of the first item
    tst_schedule_trips['departure_time_timedelta_start'] = (
        tst_schedule_trips['departure_time_timedelta_start'] -  
        tst_schedule_trips['departure_time_timedelta_start'][0])
    # add the current time and a buffer
    tst_schedule_trips['departure_time_timedelta_start'] = (
        tst_schedule_trips['departure_time_timedelta_start'] + 
        dt.timedelta(minutes=1))
    # change the deparature time column
    tst_schedule_trips['departure_time_start']  = (current_time + 
        tst_schedule_trips['departure_time_timedelta_start'])
    tst_schedule_trips['departure_time_start'] = tst_schedule_trips[
        'departure_time_start'].dt.strftime('%H:%M:%S')   
    tst_schedule_trips.to_csv(test_csv_file_loc)
    return None

def main():    
    # remove the test files
    of.remove_files([config.test_traffic_data_sql, 
                     config.test_process_monitor_sql,
                     config.test_scheduler_sql,
                     config.test_transit_data_sql])    
    # create the sql tables
    sf.create_traffic_data_table(config.test_traffic_data_sql)
    sf.create_transit_data_siri_table(config.test_transit_data_sql)
    sf.create_proc_monitor_table(config.test_process_monitor_sql)
    sf.create_push_monitor_table(config.test_process_monitor_sql)
    # create the tst csv
    create_tst_csv(config.trips_csv,config.test_trips_csv)
    ### add test google functions
    sched.add_traffic_jobs(dcf.dummy_function, config.test_trips_csv, 
                        config.test_scheduler_sql, 
                        config.test_traffic_data_sql)    
    ### test add periodic job
    datetime_now = dt.datetime.now()
    collect_transit_day_code = config.day_of_week_codes[datetime_now.weekday()]
    collect_transit_time = [datetime_now.hour + (datetime_now.minute+2)/60,
                            datetime_now.hour + (datetime_now.minute+7)/60]
    time_df = sched.create_collect_time(collect_transit_time,
                                    1,
                                    collect_transit_day_code,
                                    config.test_periodic_jobs_csv) 
    # read in the scheduler monitor
    schedule_monitor = pd.read_csv(config.schedule_monitor_csv, 
                               index_col=0)
    schedule_monitor.set_index(['trip_id','stop_id'], inplace=True)
    # add in the siri periodic jobs
    sched.add_periodic_job(config.test_scheduler_sql, 
                          dcf.query_transit_data_siri, time_df, 
                          config.sched_id_dict['siri'], 
                          [config.test_transit_data_sql , schedule_monitor])
    # add in the gtfs-rt periodic jobs
    sched.add_periodic_job(config.test_scheduler_sql, 
                          dcf.query_transit_data_gtfs_rt, time_df, 
                          config.sched_id_dict['gtfs-rt'], 
                          [config.test_transit_data_sql , schedule_monitor])
    # save the process data monitor
    #pn.restart_push_notify(test_output_database,new_logfile_name)
    sched.run_tasks(config.test_scheduler_sql) 

if __name__ == '__main__':
    main()


