# -*- coding: utf-8 -*-
"""
Created on Fri Jul  6 17:33:38 2018

@author: Robert Hennessy (rghennessy@gmail.com)
"""

from datetime import datetime, timedelta
import sys
import os
import config
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
import json
from pandas.io.json import json_normalize

week_names_sched_trips = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
weekday_names_sched_trips = week_names_sched_trips #['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
day_of_week_codes = ['mon','tue','wed','thu','fri','sat','sun']
test_output_file = 'test_json.txt'
csv_file_loc = config.trips_csv_path_in
test_csv_file_loc = config.trips_tst_csv_path

## If file exists, delete it ##
if os.path.isfile(test_output_file):
    os.remove(test_output_file)
else:    ## Show an error ##
    print("Error: %s file not found" % test_output_file)


# This function will be eventually be replaced by query google. It will take
# Inputs = trip_id (train number), start_loc and end_loc
# Writes to a json file 
def dummy_function(trip_index, trip_id, start_station, end_station, start_loc, end_loc):
    time_now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    #directions_result = start_station + ' to ' + end_station
    directions_result = [{'warnings': [], 'copyrights': 'Map data ©2018 Google', 'summary': 'US-101 N', 'legs': [{'steps': [{'distance': {'text': '318 ft', 'value': 97}, 'polyline': {'points': 'isbdFhdmiVVSFGHGRQJKLMNMRSFEFI'}, 'duration': {'text': '1 min', 'value': 14}, 'travel_mode': 'DRIVING', 'html_instructions': 'Head <b>southeast</b> on <b>Pacific Blvd</b>', 'end_location': {'lat': 37.5372658, 'lng': -122.296523}, 'start_location': {'lat': 37.53796639999999, 'lng': -122.2971724}}, {'distance': {'text': '0.8 mi', 'value': 1239}, 'polyline': {'points': '}nbdFf`miVRORQO]EICGACIOGIGEICKAI?C?IBGBEDC@CBABIDEDE@C@C@E?A?GAKEYi@A?AAOEo@kAgEsHmDqGw@uAm@aA?AA?ES?AACQ[Ya@SQ_@U{@k@SMc@Wy@_@OKg@U]UQMe@_@c@]a@c@QSEEMSMOIMi@y@S_@{@yAk@iAi@kAMUMW'}, 'duration': {'text': '3 mins', 'value': 166}, 'travel_mode': 'DRIVING', 'html_instructions': 'Turn <b>left</b> to merge onto <b>E Hillsdale Blvd</b>', 'end_location': {'lat': 37.54478460000001, 'lng': -122.2874522}, 'start_location': {'lat': 37.5372658, 'lng': -122.296523}}, {'distance': {'text': '6.7 mi', 'value': 10751}, 'polyline': {'points': '{}cdFpgkiVM_ACW?U@QDQDKBILUFEHCFCJAH@H?D@HBHDBDBDBBBD@DBF@F@H?F@D?DAFAHAHGLILGHoCbD{AdB]z@cB|AwBlBaCxBwApAqBdByBnBi@f@wBlB_GjF}GfGaCvBsAnAiAfAmBjBoBhBoAjAm@h@cCxBuAlAyAtAgDtCiA`AcBzA}@v@{@t@k@h@_@^o@l@q@r@o@n@q@r@UVY\\e@f@u@z@aBhBi@j@aFtFyA`BgBpB{AdByB`CkCxCaBhBeBlBwBbCsAxAoBxBq@r@YZqEdF_BdBoAvAe@h@wGpHmD|DkHdIyFlGKLkBrBu@|@yEhFsQhSyHrImBtBw@~@{@~@_A`Ac@f@i@n@g@j@a@d@]`@Y`@QVOTQXOXOXQ\\O\\Yp@Sn@Sj@Qn@Oj@Kj@Kb@Id@Ij@Gj@Ir@En@El@Ex@Ad@CdAAbAAfBAn@E|Gi@|o@SzXCvBA`CGfHE~DAjAAt@Cn@E~@MpBEp@G|@In@Gj@Ir@Il@[pBQz@Q`AWbAYjA[fAQh@Sh@_@`Ai@vAm@vAa@|@Yl@Yd@cAjBc@t@aJ`PyAhC{@xAaBvCW`@[l@oDjGeDzFaGhKgCnEqBlD{@xA}H`NaBtC'}, 'duration': {'text': '7 mins', 'value': 408}, 'travel_mode': 'DRIVING', 'html_instructions': 'Merge onto <b>US-101 N</b> via the ramp to <b>San Francisco</b>', 'end_location': {'lat': 37.6000361, 'lng': -122.3763451}, 'start_location': {'lat': 37.54478460000001, 'lng': -122.2874522}}, {'distance': {'text': '0.3 mi', 'value': 495}, 'polyline': {'points': 'gwndFds|iVK@I@EDEFqAlBw@jA]f@c@r@S^m@hA[d@UZKNIHKHKFMDMBK@I?K?GCKAMI}@m@UOGCGAGAGAK?C?G@G@E@MHWR'}, 'maneuver': 'ramp-right', 'travel_mode': 'DRIVING', 'html_instructions': 'Take exit <b>421</b> for <b>Millbrae Ave</b>', 'duration': {'text': '1 min', 'value': 38}, 'end_location': {'lat': 37.6034845, 'lng': -122.378707}, 'start_location': {'lat': 37.6000361, 'lng': -122.3763451}}, {'distance': {'text': '0.4 mi', 'value': 580}, 'polyline': {'points': 'wlodF|a}iVKJ`BjDh@bAVd@P`@Vf@Rd@l@nAjAnCz@jBdAxBvBpE'}, 'maneuver': 'turn-left', 'travel_mode': 'DRIVING', 'html_instructions': 'Turn <b>left</b> onto <b>E Millbrae Ave</b>', 'duration': {'text': '1 min', 'value': 85}, 'end_location': {'lat': 37.6005514, 'lng': -122.3840443}, 'start_location': {'lat': 37.6034845, 'lng': -122.378707}}, {'distance': {'text': '249 ft', 'value': 76}, 'polyline': {'points': 'mzndFfc~iVsB~A'}, 'maneuver': 'turn-right', 'travel_mode': 'DRIVING', 'html_instructions': 'Turn <b>right</b> onto <b>Rollins Rd</b>', 'duration': {'text': '1 min', 'value': 21}, 'end_location': {'lat': 37.6011264, 'lng': -122.3845203}, 'start_location': {'lat': 37.6005514, 'lng': -122.3840443}}, {'distance': {'text': '0.1 mi', 'value': 197}, 'polyline': {'points': 'a~ndFff~iVNV|A|C~@nBDFHP@BDHLV'}, 'maneuver': 'turn-left', 'travel_mode': 'DRIVING', 'html_instructions': 'Turn <b>left</b> at the 1st cross street onto <b>Camino Millennia</b>', 'duration': {'text': '1 min', 'value': 80}, 'end_location': {'lat': 37.600067, 'lng': -122.3863147}, 'start_location': {'lat': 37.6011264, 'lng': -122.3845203}}], 'end_location': {'lat': 37.600067, 'lng': -122.3863147}, 'traffic_speed_entry': [], 'end_address': 'Camino Millennia, Millbrae, CA 94030, USA', 'duration_in_traffic': {'text': '15 mins', 'value': 880}, 'duration': {'text': '14 mins', 'value': 812}, 'start_address': '3259 Pacific Blvd, San Mateo, CA 94403, USA', 'distance': {'text': '8.3 mi', 'value': 13435}, 'start_location': {'lat': 37.53796639999999, 'lng': -122.2971724}, 'via_waypoint': []}], 'bounds': {'southwest': {'lat': 37.5370749, 'lng': -122.3863147}, 'northeast': {'lat': 37.6035433, 'lng': -122.286424}}, 'waypoint_order': [], 'overview_polyline': {'points': 'isbdFhdmiVzBqBv@q@[s@QYQIUAMBUNWPOBSG[i@QGwF_KeFgJm@cAIYk@}@s@g@oAy@}Aw@w@a@o@c@iA}@s@w@k@w@}@yAgBcDw@aBMWM_ACm@@QJ]P_@PIREb@FTTJZB\\CPIVQVkFhG]z@cB|AyFfFmJnIwWxU{JlJwI|HoLhKeEvDcEdEkCxCq[`^aXdZme@xh@kn@nr@uDdE_AfAk@x@aAbBa@z@m@`Be@zA[vAg@`DObBKfBIvGyAhkBC`CInBSbDQlBQ~Ae@~Cc@|Bq@nCm@pBs@jBwAnD{@jBcNhVoG|KsR~\\sRx\\aBtCK@OFmDhFaCbEw@~@YLYDU?SEiBkAc@EYDe@\\KJ`BjD`AhBP`@dDlH`CdFvBpEsB~AlBtDdAvB^v@'}}]    
    duration_in_traffic = trip_id / 10
    data = {'time_now': time_now_str,
            'trip_index': trip_index,
            'trip_id': trip_id,
            'start_station': start_station,
            'end_station': end_station,
            'start_loc': start_loc,
            'end_loc': end_loc,
            'directions_result': directions_result,
            'duration_in_traffic': duration_in_traffic}
    with open(test_output_file, 'a') as outfile:
        json.dump(data, outfile)
    print_str = str(trip_index) + ': ' + start_station + ' to ' + end_station + ' at ' + time_now_str
    print(print_str)

start_loc = {'lat': 37.537868000000003, 'lng': -122.297349}
end_loc =  {'lat': 37.599879999999999, 'lng': -122.386647}
dummy_function(1, 385, 'San Jose', 'San Francisco', start_loc, end_loc)

#tst_df = pd.read_json(test_output_file, orient='records')

with open(test_output_file) as f:
    d = json.load(f)

# It flattens the coordinates into two separate columns 
tst_df = json_normalize(d)


#  Create a tst csv to be used with the scheduler
def create_tst_csv(csv_file_loc,test_csv_file_loc):
    # number of rows in the tst csv
    num_of_rows_in_tst_csv = 20
    # determine the current day of the week
    day_of_week = datetime.today().weekday()
    # determine the current time
    current_time = datetime.now().time()
    current_time_delta = timedelta(hours=current_time.hour, minutes=current_time.minute)
    # read in the schedule trips csv
    schedule_trips = pd.read_csv(config.trips_csv_path_in, index_col=0)
    # select the number of rows    
    rows_to_drop = schedule_trips.index[range(num_of_rows_in_tst_csv,len(schedule_trips))]   
    tst_schedule_trips = schedule_trips.drop(rows_to_drop) 
    # reset the index to start at 0
    tst_schedule_trips = tst_schedule_trips.reset_index(drop=True)
    # set the current day to 1 for the scheduler    
    tst_schedule_trips[week_names_sched_trips[day_of_week]] = 1
    # convert the deparature time timedeltas  
    tst_schedule_trips['departure_time_timedelta_start'] = pd.to_timedelta(tst_schedule_trips['departure_time_timedelta_start'])
    # subtract off the timedelta of the first item
    tst_schedule_trips['departure_time_timedelta_start'] = tst_schedule_trips['departure_time_timedelta_start'] - tst_schedule_trips['departure_time_timedelta_start'][0]
    # add the current time and a buffer
    tst_schedule_trips['departure_time_timedelta_start'] = tst_schedule_trips['departure_time_timedelta_start'] + current_time_delta +   timedelta(minutes=1)
    tst_schedule_trips.to_csv(test_csv_file_loc)
    

create_tst_csv(csv_file_loc,test_csv_file_loc)

# create the scheduler
def create_job_database(csv_path_in, sql_loc):
    schedule_trips = pd.read_csv(csv_path_in, index_col=0)
    schedule_trips = schedule_trips.sort_values(['departure_time_timedelta_start', 'arrival_time_timedelta_stop'])
    schedule_trips_index = schedule_trips.index
    
    ## If the scheduler file exists, delete it ##
    if os.path.isfile(sql_loc):
        os.remove(sql_loc)
    else:    ## Show an error ##
        print("Error: %s file not found" % sql_loc)
    # create the scheduler object
    scheduler = BackgroundScheduler()
    scheduler.add_jobstore('sqlalchemy', url='sqlite:///%s' % sql_loc)
    
    for sInd in range(len(schedule_trips)):
            trip = schedule_trips.loc[sInd]
            trip_index = schedule_trips_index[sInd]
            trip_id = trip['trip_id']
            start_station = trip['short_stop_name_start']
            end_station = trip['short_stop_name_stop']
            
            start_loc = {
                "lat" : trip['stop_lat_start'],
                "lng" : trip['stop_lon_start']
            }
            end_loc = {
                "lat" : trip['stop_lat_stop'],
                "lng" : trip['stop_lon_stop']
            }        
            
            sched_time = datetime.strptime(trip['departure_time_start'],"%H:%M:%S")
            day_code =''
            for day_ind in range(len(weekday_names_sched_trips)):
                if trip[weekday_names_sched_trips[day_ind]]:
                    if day_code == '':
                        day_code = day_code + day_of_week_codes[day_ind]
                    else :
                        day_code = day_code + ',' + day_of_week_codes[day_ind]
            # misfire_grace_time - seconds after the designated runtime that the job is still allowed to be run
            #dummy_function(trip_index, trip_id, start_station, end_station, start_loc, end_loc)
            scheduler.add_job(dummy_function,'cron',day_of_week=day_code, hour=sched_time.hour, minute=sched_time.minute, 
                              misfire_grace_time=120,id=str(trip_index),args=[trip_index, trip_id, start_station, end_station, start_loc, end_loc])
    scheduler.print_jobs()
    scheduler.start()
    scheduler.shutdown()
    

create_job_database(test_csv_file_loc, config.scheduler_sql_test_loc)

def run_tasks(sql_loc):
    print(sql_loc)
    jobstores = {
        'default': SQLAlchemyJobStore(url='sqlite:///%s' % sql_loc)
    }
    scheduler = BackgroundScheduler(jobstores=jobstores)
    scheduler.start()    
    scheduler.print_jobs()
    
    
    
run_tasks(config.scheduler_sql_test_loc) 
