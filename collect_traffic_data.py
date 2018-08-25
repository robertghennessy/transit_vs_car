# -*- coding: utf-8 -*-
"""
@author: Robert Hennessy (rghennessy@gmail.com)

Description: This program was written to collect traffic data from google maps.
    The time and routes are stored in sqlite database.
"""
import datetime as dt
import os
import config
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
import sql_functions as sf
import logging
import googlemaps
import error_handling as eh

week_names_sched_trips = ['monday', 'tuesday', 'wednesday', 'thursday', 
                          'friday', 'saturday', 'sunday']
day_of_week_codes = ['mon','tue','wed','thu','fri','sat','sun']


def query_google_traffic(trip_index, trip_id, start_station, end_station, 
                         start_loc, end_loc, sql_db_loc):
    """
    Queries google maps for duration in traffic. Stores the results in sqlite
    database.
    
    :param: trip_index: index for the trip. The index is assigned when parsing
        the gfts
    :type: trip_index: int
    
    :param: trip_id: train number
    :type trip_id: int
    
    :param: start_station: name of the start station
    :type: start_station: string
    
    :param: end_station: name of the end station
    :type: end_station: string
    
    :param: start_loc: dict that contains the latitude and longitude of the
        start station
    :type: start_loc: dictionary
        
    :param: end_loc: dict that contains the latitude and longitude of the
        start station
    :type: end_loc: dictionary
    
    :param: sql_db_loc: location of the sql database where to store the results
        from queryinh google
    :type: sql_db_loc: string
    
    return: None
    """
    gmaps = googlemaps.Client(key=config.google_transit_api_key)
    # construct time objects
    date_str = dt.datetime.now().date().isoformat()
    time_str = dt.datetime.now().time().isoformat()
    day_of_week = dt.datetime.now().isoweekday()
    now = dt.datetime.now()
    #query google maps for the results
    directions_result = gmaps.directions(start_loc,
                                     end_loc,
                                     mode="driving",
                                     departure_time=now)
    # duration in traffic in seconds                          
    duration_in_traffic = directions_result[0]['legs'][0]['duration_in_traffic']['value']
    utc_time = dt.datetime.utcnow().timestamp()
    # create the tuple that is inserted into the database. Ensure that all
    # parameters are the right data type
    data_tuple = (str(date_str), str(time_str), float(utc_time), 
                  int(day_of_week), int(trip_index), int(trip_id),
                  str(start_station), str(end_station), str(start_loc), 
                  str(end_loc), str(directions_result), 
                  float(duration_in_traffic))
    # insert the data into the database
    sf.insert_trip_data(sql_db_loc, data_tuple)
    # log the task that was just completed
    print_str = (str(trip_index) + ': ' + start_station + ' to ' + end_station 
                + ' on ' + date_str + ' at ' + time_str)
    logging.info(print_str)
    return None   

def run_tasks(sql_loc):
    """
    Runs the tasks stored in the task database
    
    :param: sql_loc: location of the sql task database
    :type: sql_loc: string

    : return: None
    """
    jobstores = {
        'default': SQLAlchemyJobStore(url='sqlite:///%s' % sql_loc)
    }
    scheduler = BlockingScheduler(jobstores=jobstores)
    scheduler.start()    
    scheduler.print_jobs()
    return None

def main():   
    # rename the old logger
    new_logfile_name = ''
    if os.path.isfile(config.log_file):
        new_logfile_name = 'SchedulerLog-{date:%Y-%m-%d_%H-%M-%S}.txt'.format(
            date=dt.datetime.now())    
        os.rename(config.log_file, os.path.join(config.logs_dir,
                                                new_logfile_name))
    # save the process data monitor
    eh.restart_push_notify(config.output_database,new_logfile_name)
    # create a new logger 
    logging.basicConfig(filename=config.log_file, level=logging.INFO, 
                        format = '%(asctime)s - %(levelname)s - %(message)s')
    run_tasks(config.scheduler_sql_loc) 
    
if __name__ == '__main__':
    main()
