"""
Created on Thu Jul 12 14:12:05 2018

@author: Robert Hennessy (robertghennessy@gmail.com)
"""

"""
Steps:
1. create an sql database with 90 days worth of fake data.
2. read the fake data in via pandas
3. create histograms and cdfs

"""

import datetime as dt
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import shutil

import config
import data_analysis as da
import sql_functions as sf 
import test_scheduler as ts

# Take train if train fraction is greater than this number
take_train_fract = 0.5


def create_test_data_database(csv_path_in,test_data_db):
    # delete the database if it exists
    if os.path.isfile(test_data_db):
        os.remove(test_data_db)
    else:    ## Show an error ##
        print("Error: %s file not found" % test_data_db)
    sf.create_trip_data_table(test_data_db)
    # read in the trip database
    schedule_trips = pd.read_csv(csv_path_in, index_col=0)
    # parameters used to create the test database
    start_date = dt.datetime(year=2018,month=7,day=12)
    numb_of_days = 1e2
    # create a vector that contains the mean of the distribution
    sim_means = 60*np.random.uniform(0,50,len(schedule_trips))
    sim_stds = 60*np.random.uniform(0,10,len(schedule_trips))
    # create the test databse
    for day_ind in range(int(numb_of_days)):
        cur_sim_date = start_date + dt.timedelta(days=day_ind)
        day_of_week = cur_sim_date.isoweekday() # integer
        # skip the weekends    
        if day_of_week in [6, 7]:
            continue
        for trip_ind in range(len(schedule_trips)):
                sim_trip = schedule_trips.loc[trip_ind]
                trip_index = sim_trip['trip_index']
                trip_id = sim_trip['trip_id']
                start_station = sim_trip['short_stop_name_start']
                end_station = sim_trip['short_stop_name_stop']
                
                start_loc = {
                    "lat" : sim_trip['stop_lat_start'],
                    "lng" : sim_trip['stop_lon_start']
                }
                end_loc = {
                    "lat" : sim_trip['stop_lat_stop'],
                    "lng" : sim_trip['stop_lon_stop']
                }  
                
                date_str = cur_sim_date.date().isoformat() # string
                time_str = sim_trip.departure_time_start # string
                time_dt = dt.datetime.strptime(time_str,"%H:%M:%S").time()         
                cur_sim_time = dt.datetime.combine(cur_sim_date,time_dt)
                utc_time = cur_sim_time.timestamp()
                directions_result = [{'warnings': [], 'copyrights': 'Map data ©2018 Google', 'summary': 'US-101 N', 'legs': [{'steps': [{'distance': {'text': '318 ft', 'value': 97}, 'polyline': {'points': 'isbdFhdmiVVSFGHGRQJKLMNMRSFEFI'}, 'duration': {'text': '1 min', 'value': 14}, 'travel_mode': 'DRIVING', 'html_instructions': 'Head <b>southeast</b> on <b>Pacific Blvd</b>', 'end_location': {'lat': 37.5372658, 'lng': -122.296523}, 'start_location': {'lat': 37.53796639999999, 'lng': -122.2971724}}, {'distance': {'text': '0.8 mi', 'value': 1239}, 'polyline': {'points': '}nbdFf`miVRORQO]EICGACIOGIGEICKAI?C?IBGBEDC@CBABIDEDE@C@C@E?A?GAKEYi@A?AAOEo@kAgEsHmDqGw@uAm@aA?AA?ES?AACQ[Ya@SQ_@U{@k@SMc@Wy@_@OKg@U]UQMe@_@c@]a@c@QSEEMSMOIMi@y@S_@{@yAk@iAi@kAMUMW'}, 'duration': {'text': '3 mins', 'value': 166}, 'travel_mode': 'DRIVING', 'html_instructions': 'Turn <b>left</b> to merge onto <b>E Hillsdale Blvd</b>', 'end_location': {'lat': 37.54478460000001, 'lng': -122.2874522}, 'start_location': {'lat': 37.5372658, 'lng': -122.296523}}, {'distance': {'text': '6.7 mi', 'value': 10751}, 'polyline': {'points': '{}cdFpgkiVM_ACW?U@QDQDKBILUFEHCFCJAH@H?D@HBHDBDBDBBBD@DBF@F@H?F@D?DAFAHAHGLILGHoCbD{AdB]z@cB|AwBlBaCxBwApAqBdByBnBi@f@wBlB_GjF}GfGaCvBsAnAiAfAmBjBoBhBoAjAm@h@cCxBuAlAyAtAgDtCiA`AcBzA}@v@{@t@k@h@_@^o@l@q@r@o@n@q@r@UVY\\e@f@u@z@aBhBi@j@aFtFyA`BgBpB{AdByB`CkCxCaBhBeBlBwBbCsAxAoBxBq@r@YZqEdF_BdBoAvAe@h@wGpHmD|DkHdIyFlGKLkBrBu@|@yEhFsQhSyHrImBtBw@~@{@~@_A`Ac@f@i@n@g@j@a@d@]`@Y`@QVOTQXOXOXQ\\O\\Yp@Sn@Sj@Qn@Oj@Kj@Kb@Id@Ij@Gj@Ir@En@El@Ex@Ad@CdAAbAAfBAn@E|Gi@|o@SzXCvBA`CGfHE~DAjAAt@Cn@E~@MpBEp@G|@In@Gj@Ir@Il@[pBQz@Q`AWbAYjA[fAQh@Sh@_@`Ai@vAm@vAa@|@Yl@Yd@cAjBc@t@aJ`PyAhC{@xAaBvCW`@[l@oDjGeDzFaGhKgCnEqBlD{@xA}H`NaBtC'}, 'duration': {'text': '7 mins', 'value': 408}, 'travel_mode': 'DRIVING', 'html_instructions': 'Merge onto <b>US-101 N</b> via the ramp to <b>San Francisco</b>', 'end_location': {'lat': 37.6000361, 'lng': -122.3763451}, 'start_location': {'lat': 37.54478460000001, 'lng': -122.2874522}}, {'distance': {'text': '0.3 mi', 'value': 495}, 'polyline': {'points': 'gwndFds|iVK@I@EDEFqAlBw@jA]f@c@r@S^m@hA[d@UZKNIHKHKFMDMBK@I?K?GCKAMI}@m@UOGCGAGAGAK?C?G@G@E@MHWR'}, 'maneuver': 'ramp-right', 'travel_mode': 'DRIVING', 'html_instructions': 'Take exit <b>421</b> for <b>Millbrae Ave</b>', 'duration': {'text': '1 min', 'value': 38}, 'end_location': {'lat': 37.6034845, 'lng': -122.378707}, 'start_location': {'lat': 37.6000361, 'lng': -122.3763451}}, {'distance': {'text': '0.4 mi', 'value': 580}, 'polyline': {'points': 'wlodF|a}iVKJ`BjDh@bAVd@P`@Vf@Rd@l@nAjAnCz@jBdAxBvBpE'}, 'maneuver': 'turn-left', 'travel_mode': 'DRIVING', 'html_instructions': 'Turn <b>left</b> onto <b>E Millbrae Ave</b>', 'duration': {'text': '1 min', 'value': 85}, 'end_location': {'lat': 37.6005514, 'lng': -122.3840443}, 'start_location': {'lat': 37.6034845, 'lng': -122.378707}}, {'distance': {'text': '249 ft', 'value': 76}, 'polyline': {'points': 'mzndFfc~iVsB~A'}, 'maneuver': 'turn-right', 'travel_mode': 'DRIVING', 'html_instructions': 'Turn <b>right</b> onto <b>Rollins Rd</b>', 'duration': {'text': '1 min', 'value': 21}, 'end_location': {'lat': 37.6011264, 'lng': -122.3845203}, 'start_location': {'lat': 37.6005514, 'lng': -122.3840443}}, {'distance': {'text': '0.1 mi', 'value': 197}, 'polyline': {'points': 'a~ndFff~iVNV|A|C~@nBDFHP@BDHLV'}, 'maneuver': 'turn-left', 'travel_mode': 'DRIVING', 'html_instructions': 'Turn <b>left</b> at the 1st cross street onto <b>Camino Millennia</b>', 'duration': {'text': '1 min', 'value': 80}, 'end_location': {'lat': 37.600067, 'lng': -122.3863147}, 'start_location': {'lat': 37.6011264, 'lng': -122.3845203}}], 'end_location': {'lat': 37.600067, 'lng': -122.3863147}, 'traffic_speed_entry': [], 'end_address': 'Camino Millennia, Millbrae, CA 94030, USA', 'duration_in_traffic': {'text': '15 mins', 'value': 880}, 'duration': {'text': '14 mins', 'value': 812}, 'start_address': '3259 Pacific Blvd, San Mateo, CA 94403, USA', 'distance': {'text': '8.3 mi', 'value': 13435}, 'start_location': {'lat': 37.53796639999999, 'lng': -122.2971724}, 'via_waypoint': []}], 'bounds': {'southwest': {'lat': 37.5370749, 'lng': -122.3863147}, 'northeast': {'lat': 37.6035433, 'lng': -122.286424}}, 'waypoint_order': [], 'overview_polyline': {'points': 'isbdFhdmiVzBqBv@q@[s@QYQIUAMBUNWPOBSG[i@QGwF_KeFgJm@cAIYk@}@s@g@oAy@}Aw@w@a@o@c@iA}@s@w@k@w@}@yAgBcDw@aBMWM_ACm@@QJ]P_@PIREb@FTTJZB\\CPIVQVkFhG]z@cB|AyFfFmJnIwWxU{JlJwI|HoLhKeEvDcEdEkCxCq[`^aXdZme@xh@kn@nr@uDdE_AfAk@x@aAbBa@z@m@`Be@zA[vAg@`DObBKfBIvGyAhkBC`CInBSbDQlBQ~Ae@~Cc@|Bq@nCm@pBs@jBwAnD{@jBcNhVoG|KsR~\\sRx\\aBtCK@OFmDhFaCbEw@~@YLYDU?SEiBkAc@EYDe@\\KJ`BjD`AhBP`@dDlH`CdFvBpEsB~AlBtDdAvB^v@'}}]    
                duration_in_traffic = np.random.normal(sim_means[trip_ind], 
                                                       sim_stds[trip_ind])
                # create the tuple that is inserted into the database. Ensure 
                #that all parameters are the right data type
                data_tuple = (str(date_str), str(time_str), float(utc_time), 
                              int(day_of_week), int(trip_index), int(trip_id),
                              str(start_station), str(end_station),
                              str(start_loc), str(end_loc), 
                              str(directions_result), 
                              float(duration_in_traffic))
                # insert the deata into the database
                sf.insert_trip_data(test_data_db, data_tuple)
    
def main():
    # Delete the files in the plot dir
    if os.path.exists(config.test_plot_dir):
        try:
            shutil.rmtree(config.test_plot_dir)
        except OSError as e:
            print ("Error: %s - %s." % (e.filename, e.strerror))
    # create the plots directory
    if not os.path.exists(config.test_plot_dir):
        os.makedirs(config.test_plot_dir)
    # create the plot subdirectories
    ecdf_dir = os.path.join(config.test_plot_dir, 'ecdf')
    if not os.path.exists(ecdf_dir):
        os.makedirs(ecdf_dir)
    hist_dir = os.path.join(config.test_plot_dir, 'hist')
    if not os.path.exists(hist_dir):
        os.makedirs(hist_dir)
    time_dir = os.path.join(config.test_plot_dir, 'time')
    if not os.path.exists(time_dir):
        os.makedirs(time_dir)
    # turn off the interactive mode for pyplot
    plt.ioff()
        # create the results database
    if os.path.exists(config.test_results_database):
        os.remove(config.test_results_database)
    sf.create_results_table(config.test_results_database)
    if not os.path.exists(config.trips_tst_csv_path):
        ts.create_tst_csv(config.trips_csv,config.trips_tst_csv_path)   
    create_test_data_database(config.trips_tst_csv_path, config.test_data_db)
    da.create_plots(config.trips_tst_csv_path, config.test_data_db, 
                    config.test_results_database, ecdf_dir, hist_dir, time_dir)
    
if __name__ == '__main__':
    main()
