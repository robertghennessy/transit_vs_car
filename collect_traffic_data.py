"""
Description: This program was written to collect traffic data from google maps.
    The time and routes are stored in sqlite database.
    
@author: Robert Hennessy (robertghennessy@gmail.com)
"""
import datetime as dt
import config
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
import sql_functions as sf
import logging
import googlemaps
import error_handling as eh
import tenacity as ten

week_names_sched_trips = ['monday', 'tuesday', 'wednesday', 'thursday', 
                          'friday', 'saturday', 'sunday']
day_of_week_codes = ['mon','tue','wed','thu','fri','sat','sun']

# set up the root logger
logger = logging.getLogger('')
logger.setLevel(logging.INFO)
dateTag = dt.datetime.now().strftime("%Y-%b-%d_%H-%M-%S")
log_filename = config.log_file % dateTag
# add a rotating handler, rotate the file every 10 mb. Keep the last 5 logfiles
handler = logging.handlers.RotatingFileHandler(log_filename, 
                                    maxBytes=10*1024*1024, backupCount=5)
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def query_google_traffic(trip_index, trip_id, start_station, end_station, 
                         start_loc, end_loc, sql_db_loc):
    """
    Calls function to query google maps for duration in traffic. Stores the 
        results in sqlite database.
    
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
    
    :return: None
    """
    # query google api    
    (duration_in_traffic, directions_result) = query_google_api(start_loc,
                                                                    end_loc)
    # construct time objects
    date_str = dt.datetime.now().date().isoformat()
    time_str = dt.datetime.now().time().isoformat()
    day_of_week = dt.datetime.now().isoweekday()
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


@ten.retry(wait=ten.wait_random_exponential(multiplier=1, max=10),
       reraise=True, stop=ten.stop_after_attempt(5),
       before_sleep=ten.before_sleep_log(logger, logging.DEBUG))
def query_google_api(start_loc,end_loc):
    """
    This program queries the google api to determine the driving time between
        the start and location. The retry wrapper will retry this function if 
        an error occurs. The method is exponential backoff with jitter and it 
        will retry 5 times before raising an exception.
    
    :param: start_loc: dict that contains the latitude and longitude of the
        start station
    :type: start_loc: dictionary
        
    :param: end_loc: dict that contains the latitude and longitude of the
        start station
    :type: end_loc: dictionary
    
    :return: duration_in_traffic: extracted duration in traffic in seconds
    :type: float
    
    :return: directions_result: the results returned by querying googlemaps
    :type: dictionary
    
    """
    # timeout after 5 seconds    
    gmaps = googlemaps.Client(key=config.google_transit_api_key,timeout=5)
    now = dt.datetime.now()
    #query google maps for the results
    directions_result = gmaps.directions(start_loc,
                                     end_loc,
                                     mode="driving",
                                     departure_time=now)
    # duration in traffic in seconds                          
    duration_in_traffic = (directions_result[0]['legs'][0]
                                ['duration_in_traffic']['value'])
    return (duration_in_traffic, directions_result)


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
    # save the process data monitor and send a push notification
    eh.restart_push_notify(config.output_database,log_filename)
    # run the task 
    run_tasks(config.scheduler_sql_loc) 
    
if __name__ == '__main__':
    main()
