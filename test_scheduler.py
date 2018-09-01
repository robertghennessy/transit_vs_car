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
import collect_traffic_data as ctd
import prepare_to_collect_data as ptcd
import logging
import error_handling as eh
import random
import tenacity
from tenacity import retry

week_names_sched_trips = ['monday', 'tuesday', 'wednesday', 'thursday', 
                          'friday', 'saturday', 'sunday']
day_of_week_codes = ['mon','tue','wed','thu','fri','sat','sun']
test_output_database = os.path.join(config.file_dir,'test_data_db.sqlite')
test_log_file = os.path.join(config.logs_dir,'test_SchedulerLog-%s.txt')
test_output_database = os.path.join(config.file_dir,'test_data_db.sqlite')

# set up the root logger
logger = logging.getLogger('')
logger.setLevel(logging.INFO)
dateTag = dt.datetime.now().strftime("%Y-%b-%d_%H-%M-%S")
path = test_log_file % dateTag
# add a rotating handler, rotate the file every 10 mb
handler = logging.handlers.RotatingFileHandler(path, maxBytes=10*1024*1024,
                              backupCount=5)
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

#logging.basicConfig(filename="tst_%s.log" % dateTag, level=logging.INFO, 
#                    format = '%(asctime)s - %(levelname)s - %(message)s')

def dummy_function(trip_index, trip_id, start_station, end_station, start_loc, 
                   end_loc, sql_db_loc):
    """
    Dummy function that is used to test the scheduler.
    
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
    # construct time objects
    date_str = dt.datetime.now().date().isoformat() # string
    time_str = dt.datetime.now().time().isoformat() # string
    day_of_week = dt.datetime.now().isoweekday() # integer
    utc_time = dt.datetime.utcnow().timestamp()
    # print out the task that you are calling
    print_str = (str(trip_index) + ': ' + start_station + ' to ' + end_station 
                + ' on ' + date_str + ' at ' + time_str)
    print(print_str)
    logger.info(print_str)
    (duration_in_traffic, directions_result) = dummy_query_google_api()
    # create the tuple that is inserted into the database. Ensure that all
    # parameters are the right data type
    data_tuple = (str(date_str), str(time_str), float(utc_time), 
                  int(day_of_week), int(trip_index), int(trip_id),
                  str(start_station), str(end_station), str(start_loc), 
                  str(end_loc), str(directions_result), 
                  float(duration_in_traffic))
    # insert the data into the database
    sf.insert_trip_data(sql_db_loc, data_tuple)

    return None

@retry(wait=tenacity.wait_random_exponential(multiplier=0.1, max=5),
       reraise=True, stop=tenacity.stop_after_attempt(5),
       before_sleep=tenacity.before_sleep_log(logger, logging.DEBUG))
def dummy_query_google_api():
    if random.uniform(0,1) <= 0.13:
        directions_result = [{'warnings': [], 'copyrights': 'Map data ©2018 Google', 'summary': 'US-101 N', 'legs': [{'steps': [{'distance': {'text': '318 ft', 'value': 97}, 'polyline': {'points': 'isbdFhdmiVVSFGHGRQJKLMNMRSFEFI'}, 'duration': {'text': '1 min', 'value': 14}, 'travel_mode': 'DRIVING', 'html_instructions': 'Head <b>southeast</b> on <b>Pacific Blvd</b>', 'end_location': {'lat': 37.5372658, 'lng': -122.296523}, 'start_location': {'lat': 37.53796639999999, 'lng': -122.2971724}}, {'distance': {'text': '0.8 mi', 'value': 1239}, 'polyline': {'points': '}nbdFf`miVRORQO]EICGACIOGIGEICKAI?C?IBGBEDC@CBABIDEDE@C@C@E?A?GAKEYi@A?AAOEo@kAgEsHmDqGw@uAm@aA?AA?ES?AACQ[Ya@SQ_@U{@k@SMc@Wy@_@OKg@U]UQMe@_@c@]a@c@QSEEMSMOIMi@y@S_@{@yAk@iAi@kAMUMW'}, 'duration': {'text': '3 mins', 'value': 166}, 'travel_mode': 'DRIVING', 'html_instructions': 'Turn <b>left</b> to merge onto <b>E Hillsdale Blvd</b>', 'end_location': {'lat': 37.54478460000001, 'lng': -122.2874522}, 'start_location': {'lat': 37.5372658, 'lng': -122.296523}}, {'distance': {'text': '6.7 mi', 'value': 10751}, 'polyline': {'points': '{}cdFpgkiVM_ACW?U@QDQDKBILUFEHCFCJAH@H?D@HBHDBDBDBBBD@DBF@F@H?F@D?DAFAHAHGLILGHoCbD{AdB]z@cB|AwBlBaCxBwApAqBdByBnBi@f@wBlB_GjF}GfGaCvBsAnAiAfAmBjBoBhBoAjAm@h@cCxBuAlAyAtAgDtCiA`AcBzA}@v@{@t@k@h@_@^o@l@q@r@o@n@q@r@UVY\\e@f@u@z@aBhBi@j@aFtFyA`BgBpB{AdByB`CkCxCaBhBeBlBwBbCsAxAoBxBq@r@YZqEdF_BdBoAvAe@h@wGpHmD|DkHdIyFlGKLkBrBu@|@yEhFsQhSyHrImBtBw@~@{@~@_A`Ac@f@i@n@g@j@a@d@]`@Y`@QVOTQXOXOXQ\\O\\Yp@Sn@Sj@Qn@Oj@Kj@Kb@Id@Ij@Gj@Ir@En@El@Ex@Ad@CdAAbAAfBAn@E|Gi@|o@SzXCvBA`CGfHE~DAjAAt@Cn@E~@MpBEp@G|@In@Gj@Ir@Il@[pBQz@Q`AWbAYjA[fAQh@Sh@_@`Ai@vAm@vAa@|@Yl@Yd@cAjBc@t@aJ`PyAhC{@xAaBvCW`@[l@oDjGeDzFaGhKgCnEqBlD{@xA}H`NaBtC'}, 'duration': {'text': '7 mins', 'value': 408}, 'travel_mode': 'DRIVING', 'html_instructions': 'Merge onto <b>US-101 N</b> via the ramp to <b>San Francisco</b>', 'end_location': {'lat': 37.6000361, 'lng': -122.3763451}, 'start_location': {'lat': 37.54478460000001, 'lng': -122.2874522}}, {'distance': {'text': '0.3 mi', 'value': 495}, 'polyline': {'points': 'gwndFds|iVK@I@EDEFqAlBw@jA]f@c@r@S^m@hA[d@UZKNIHKHKFMDMBK@I?K?GCKAMI}@m@UOGCGAGAGAK?C?G@G@E@MHWR'}, 'maneuver': 'ramp-right', 'travel_mode': 'DRIVING', 'html_instructions': 'Take exit <b>421</b> for <b>Millbrae Ave</b>', 'duration': {'text': '1 min', 'value': 38}, 'end_location': {'lat': 37.6034845, 'lng': -122.378707}, 'start_location': {'lat': 37.6000361, 'lng': -122.3763451}}, {'distance': {'text': '0.4 mi', 'value': 580}, 'polyline': {'points': 'wlodF|a}iVKJ`BjDh@bAVd@P`@Vf@Rd@l@nAjAnCz@jBdAxBvBpE'}, 'maneuver': 'turn-left', 'travel_mode': 'DRIVING', 'html_instructions': 'Turn <b>left</b> onto <b>E Millbrae Ave</b>', 'duration': {'text': '1 min', 'value': 85}, 'end_location': {'lat': 37.6005514, 'lng': -122.3840443}, 'start_location': {'lat': 37.6034845, 'lng': -122.378707}}, {'distance': {'text': '249 ft', 'value': 76}, 'polyline': {'points': 'mzndFfc~iVsB~A'}, 'maneuver': 'turn-right', 'travel_mode': 'DRIVING', 'html_instructions': 'Turn <b>right</b> onto <b>Rollins Rd</b>', 'duration': {'text': '1 min', 'value': 21}, 'end_location': {'lat': 37.6011264, 'lng': -122.3845203}, 'start_location': {'lat': 37.6005514, 'lng': -122.3840443}}, {'distance': {'text': '0.1 mi', 'value': 197}, 'polyline': {'points': 'a~ndFff~iVNV|A|C~@nBDFHP@BDHLV'}, 'maneuver': 'turn-left', 'travel_mode': 'DRIVING', 'html_instructions': 'Turn <b>left</b> at the 1st cross street onto <b>Camino Millennia</b>', 'duration': {'text': '1 min', 'value': 80}, 'end_location': {'lat': 37.600067, 'lng': -122.3863147}, 'start_location': {'lat': 37.6011264, 'lng': -122.3845203}}], 'end_location': {'lat': 37.600067, 'lng': -122.3863147}, 'traffic_speed_entry': [], 'end_address': 'Camino Millennia, Millbrae, CA 94030, USA', 'duration_in_traffic': {'text': '15 mins', 'value': 880}, 'duration': {'text': '14 mins', 'value': 812}, 'start_address': '3259 Pacific Blvd, San Mateo, CA 94403, USA', 'distance': {'text': '8.3 mi', 'value': 13435}, 'start_location': {'lat': 37.53796639999999, 'lng': -122.2971724}, 'via_waypoint': []}], 'bounds': {'southwest': {'lat': 37.5370749, 'lng': -122.3863147}, 'northeast': {'lat': 37.6035433, 'lng': -122.286424}}, 'waypoint_order': [], 'overview_polyline': {'points': 'isbdFhdmiVzBqBv@q@[s@QYQIUAMBUNWPOBSG[i@QGwF_KeFgJm@cAIYk@}@s@g@oAy@}Aw@w@a@o@c@iA}@s@w@k@w@}@yAgBcDw@aBMWM_ACm@@QJ]P_@PIREb@FTTJZB\\CPIVQVkFhG]z@cB|AyFfFmJnIwWxU{JlJwI|HoLhKeEvDcEdEkCxCq[`^aXdZme@xh@kn@nr@uDdE_AfAk@x@aAbBa@z@m@`Be@zA[vAg@`DObBKfBIvGyAhkBC`CInBSbDQlBQ~Ae@~Cc@|Bq@nCm@pBs@jBwAnD{@jBcNhVoG|KsR~\\sRx\\aBtCK@OFmDhFaCbEw@~@YLYDU?SEiBkAc@EYDe@\\KJ`BjD`AhBP`@dDlH`CdFvBpEsB~AlBtDdAvB^v@'}}]    
    else:
        directions_result = 0
    duration_in_traffic = (directions_result[0]['legs'][0]
        ['duration_in_traffic']['value'])
    return (duration_in_traffic, directions_result)

#  Create a tst csv to be used with the scheduler
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
    schedule_trips = pd.read_csv(config.trips_csv_path_in, index_col=0)
    # select the number of rows    
    rows_to_drop = schedule_trips.index[range(num_of_rows_in_tst_csv,
                                              len(schedule_trips))]   
    tst_schedule_trips = schedule_trips.drop(rows_to_drop) 
    # set the current day to 1 for the scheduler    
    tst_schedule_trips[week_names_sched_trips[day_of_week]] = 1
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
    ## If file exists, delete it ##
    if os.path.isfile(test_output_database):
        os.remove(test_output_database)
    else:    ## Show an error ##
        print("Error: %s file not found" % test_output_database)
    
    # rename the old logger
    new_logfile_name = ''
    if os.path.isfile(test_log_file):
        new_logfile_name = 'SchedulerLog-{date:%Y-%m-%d_%H-%M-%S}.txt'.format(
            date=dt.datetime.now())    
        os.rename(test_log_file, os.path.join(config.logs_dir,
                                                     new_logfile_name))
    create_tst_csv(config.trips_csv_path_in,config.trips_tst_csv_path)
    ptcd.create_job_database(dummy_function, config.trips_tst_csv_path, 
                             config.scheduler_sql_test_loc, 
                             test_output_database)
    # create the tables
    sf.create_trip_data_table(test_output_database)
    sf.create_proc_monitor_table(test_output_database)
    # save the process data monitor
    #eh.restart_push_notify(test_output_database,new_logfile_name)
    ctd.run_tasks(config.scheduler_sql_test_loc) 

if __name__ == '__main__':
    main()


