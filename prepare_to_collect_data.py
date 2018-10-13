"""
Description: This program parses a gfts file.
    
@author: Robert Hennessy (robertghennessy@gmail.com)
"""

import datetime as dt
from itertools import permutations
import logging, logging.handlers
import numpy as np
import pandas as pd

import partridge as ptg

import config
import data_collection_functions as dcf
import file_functions as ff
import scheduler_functions as sched
import sql_functions as sf

# set up the root logger
logger = logging.getLogger('')
logger.setLevel(logging.INFO)
dateTag = dt.datetime.now().strftime("%Y-%b-%d_%H-%M-%S")
log_filename = config.prepare_log_file % dateTag
# add a rotating handler, rotate the file every 10 mb. Keep the last 5 logfiles
handler = logging.handlers.RotatingFileHandler(log_filename, 
                                    maxBytes=10*1024*1024, backupCount=5)
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# determine which days that data is collected
day_of_interest_start = 0
# + 1 - slice does not include end
day_of_interest_end = 4 + 1

####### variables for collecting transit data
# Only trips within this window will be collected
morning_commute_hours = [6.5,9.5]
evening_commute_hours = [17,20.25]
days_of_interest = config.weekday_names[day_of_interest_start:
    day_of_interest_end]

# top 10 stations come from http://www.caltrain.com/Assets/_Marketing/
# caltrain/pdf/2016/2017+Annual+Count+Key+Findings+Report.pdf
# Menlo Park excluded to reduce the number of google maps requests 
# and the station is very close to Palo Alto
# the outputted trips will be between these stations
stations = ['San Francisco',
                   'Palo Alto',
                   'San Jose Diridon',
                   'Mountain View',
                   'Redwood City',
                   'Millbrae',
                   'Sunnyvale',
                   'Hillsdale',
                   'San Mateo']
# The following line is used to exclude the bus shuttle stations
stop_id_upper_limit = 70500


####### variables for collecting traffic data
# added extra to catch delays, time in hours
collect_transit_time = [morning_commute_hours[0],
                         evening_commute_hours[1]+2.25]
# frequency that the data is collected in minutes
collect_transit_frequency = 5
# create the day code to use by scheduler
collect_transit_day_code = ','.join(config.day_of_week_codes[
    day_of_interest_start:day_of_interest_end])


###### Start of functions

def trip_list_df(trip_list, stops_df):
    """
    Create dataframe that includes the trip permutations and the direction
        of the trip
    
    :param: trip_list: a list of tuples that contain the trips of interest
    :type: list of tuples
    
    :param: stops_df: dataframe produced by reading in the stops file in the
        GFTS
    :type: pandas dataframe
    
    """    
    trip_df = pd.DataFrame(trip_list,columns=['start_station', 'end_station']) 
    # add direction infomration 
    # the stops dataframe is ordered so that north stations have lower number
    for fInd in range(len(trip_df)):
        start_station_name = trip_df.iloc[fInd]['start_station']
        end_station_name = trip_df.iloc[fInd]['end_station']
        start_station_stop_id = stops_df.loc[stops_df['short_stop_name']
                                    ==start_station_name,'stop_id'].max()
        end_station_stop_id = stops_df.loc[stops_df['short_stop_name']
                                            ==end_station_name,'stop_id'].max()
        if(end_station_stop_id < start_station_stop_id):
            trip_df.ix[fInd, 'train_direction'] = 0
        else:
            trip_df.ix[fInd, 'train_direction'] = 1
    # convert the index into a multindex
    trip_df = trip_df.set_index(['start_station','end_station'])
    trip_df.index.names = ['short_stop_name_start','short_stop_name_stop']
    return trip_df


def parse_gfts(stations, zip_path, csv_out_path, schedule_monitor_csv):
    """
    Parases the gfts file and outputs a csv file containing information for 
        selected trips
    
    :param: stations: A list of tuples of the stations that you want the trips 
        to be between 
    :type: list of tuples
    
    :param: zip_path: file location of the gfts 
    :type: string
    
    :param: csv_out_path: file location of the output csv file
    :type: string
    
    :param: schedule_monitor_csv: file location of the schedule monitor
        csv file
    :type: string
    
    """ 
    # read in the GFTS file to pandas
    feed = ptg.raw_feed(zip_path)
    #schedule contains the schedule. Convert values to numbers if possible
    schedule = feed.stop_times.apply(pd.to_numeric, errors='ignore')
    # stops contain information about the stations. Convert values to numbers 
    # if possible
    stops = feed.stops.apply(pd.to_numeric, errors='ignore')
    # trips contains information on the train type, direction and frequency of 
    # service code
    trips = feed.trips.apply(pd.to_numeric, errors='ignore')
    # calendar converts the frequency code to dates
    calendar = feed.calendar.apply(pd.to_numeric, errors='ignore')
    # remove the shuttles and special trains
    train_numbers = list(set(schedule.trip_id))
    # normal train are numbers only
    train_numbers = [numb for numb in train_numbers if(str.isnumeric(numb))]
    # create short stop name - remove caltrain from the end of the name
    stops['short_stop_name'] = stops['stop_name'].str.split(
                                    'Caltrain',1).str[0].str.strip()    
    # create the schedule monitor
    create_schedule_monitor_csv(schedule,stops,schedule_monitor_csv)
    # remove the special trains
    schedule = schedule[schedule['trip_id'].isin(train_numbers)] 
    # convert the trip id to a number
    #schedule['trip_id'] = pd.to_numeric(schedule['trip_id'])
    # Create new columns with seconds since midnight of the first day because 
    # some of the trains arrive on the following day
    schedule['arrival_time_timedelta'] = dcf.trip_timedelta(
                                            schedule['arrival_time'])
    schedule['departure_time_timedelta'] = dcf.trip_timedelta(
                                            schedule['departure_time'])
    # create a list of stations from north to south
    # drop the shuttle station, drop the duplicates, reset thse index 
    station_series = stops[stops['stop_id'] < stop_id_upper_limit][
                            'short_stop_name'].drop_duplicates().reset_index(
                            drop=True)
   # create a combination of the schedule
    schedule_trips = pd.merge(schedule,schedule,on='trip_id',
                                  suffixes=('_start','_stop'))
    # remove trips going the wrong direction
    schedule_trips = schedule_trips.drop(schedule_trips[schedule_trips
        ['stop_sequence_start'] >= schedule_trips['stop_sequence_stop']].index)
    # add the station location to the schedule dataframe
    schedule_trips = pd.merge(schedule_trips, stops, left_on='stop_id_start',
                              right_on='stop_id')
    schedule_trips = pd.merge(schedule_trips, stops, left_on='stop_id_stop',
                              right_on='stop_id', suffixes=('_start', '_stop'))
    # add the service_id (what type of service is it?)
    schedule_trips = pd.merge(schedule_trips, trips, on='trip_id')
    # convert the service code to days of week
    schedule_trips = pd.merge(schedule_trips, calendar, on='service_id')  
    schedule_trips = schedule_trips.set_index(['short_stop_name_start',
                                              'short_stop_name_stop'])
    # create station pairs 
    trip_list = list(permutations(stations,2))     
    trip_df = trip_list_df(trip_list, stops)
    schedule_trips = pd.merge(trip_df,schedule_trips,left_index=True,
                              right_index=True)
    # Remove weekend only trips
    schedule_trips = schedule_trips[schedule_trips[days_of_interest].sum(
                        axis=1)>0]
    # filter in the commute only trains
    morning_commute_hours_td = [dt.timedelta(hours=x) for x 
                                    in morning_commute_hours]
    evening_commute_hours_td = [dt.timedelta(hours=x) for x 
                                    in evening_commute_hours]
    schedule_trips = schedule_trips[
        (((schedule_trips['departure_time_timedelta_start'] > 
            morning_commute_hours_td[0]) &
        (schedule_trips['arrival_time_timedelta_stop'] <
            morning_commute_hours_td[1]))) |
        (((schedule_trips['departure_time_timedelta_start'] > 
            evening_commute_hours_td[0]) &
        (schedule_trips['arrival_time_timedelta_stop'] < 
            evening_commute_hours_td[1])))]
    # reset the index to numbers
    schedule_trips = schedule_trips.reset_index()
    #  remove suboptimal trains (slower to take the train than wait for 
    # the next train
    # rank the deparature time and arrival time for the city combinations
    schedule_trips['deparature_time_rank'] = schedule_trips.groupby([
        'short_stop_name_start', 'short_stop_name_stop'])[
        'departure_time_timedelta_start'].rank(method='dense').astype(int)
    schedule_trips['arrival_time_rank'] = schedule_trips.groupby([
        'short_stop_name_start', 'short_stop_name_stop'])[
        'arrival_time_timedelta_stop'].rank(method='dense').astype(int)
    # remove the trains where the deparature rank is less than arrival rank
    # because this means that a later train will arrive before this one
    schedule_trips = schedule_trips[schedule_trips['deparature_time_rank'] >= 
        schedule_trips['arrival_time_rank']]
    # order the trains based on deperature time
    schedule_trips = schedule_trips.sort_values([
        'departure_time_timedelta_start', 'arrival_time_timedelta_stop'])
    schedule_trips = schedule_trips.reset_index(drop=True)
    schedule_trips['trip_index'] = schedule_trips.index
    schedule_trips['sched_trip_duration_secs'] = (schedule_trips[
        'arrival_time_timedelta_stop'] - schedule_trips[
        'departure_time_timedelta_start']).dt.total_seconds()
    # output to csv
    schedule_trips.to_csv(csv_out_path)
    return None


def create_schedule_monitor_csv(schedule, stops, csv_out_path):
    """ 
    Parses the schedule and creates a csv file to use to determine on time 
        performance
    
    :param: schedule: Dataframe that contains the scheduler information
    :type: pandas dataframe

    :param: csv_out_path: file location of the output csv file
    :type: string   
    
    :return: None
    """
    schedule['scheduled_arrival_time_seconds'] = dcf.seconds_from_midnight(
                                            schedule['arrival_time'])
    schedule['scheduled_departure_time_seconds'] = dcf.seconds_from_midnight(
                                            schedule['departure_time'])
    schedule['trip_start_date_delta'] = -(np.floor(
        schedule['scheduled_departure_time_seconds'] /(60*60*24)))
    schedule = pd.merge(schedule, stops, on='stop_id', how='inner')
    schedule.to_csv(csv_out_path, columns  = ['trip_id', 'stop_id', 
                                      'short_stop_name',
                                      'scheduled_arrival_time_seconds', 
                                      'scheduled_departure_time_seconds',
                                      'trip_start_date_delta'])
    return None


def main():
    # remove the  files
    ff.remove_files([config.trips_csv, 
                     config.scheduler_sql,
                     config.process_monitor_sql])
    # parse the gfts
    parse_gfts(stations,config.gtfs_zip_path, config.trips_csv, 
               config.schedule_monitor_csv)       
    #  Add the traffic jobs
    sched.add_traffic_jobs(dcf.query_google_traffic, config.trips_csv, 
                        config.scheduler_sql, config.traffic_data_sql)
    # Add the transit jobs
    time_df = sched.create_collect_time(collect_transit_time,
                                    collect_transit_frequency,
                                    collect_transit_day_code,
                                    config.periodic_jobs_csv)
    # read in the scheduler monitor
    schedule_monitor = pd.read_csv(config.schedule_monitor_csv, 
                               index_col=0)
    schedule_monitor.set_index(['trip_id','stop_id'], inplace=True)
    # add in the siri periodic jobs
    sched.add_periodic_job(config.scheduler_sql, 
                          dcf.query_transit_data_siri, time_df, 
                          config.sched_id_dict['siri'], 
                          [config.transit_data_sql , schedule_monitor])
    # add in the gtfs-rt periodic jobs
    sched.add_periodic_job(config.scheduler_sql, 
                          dcf.query_transit_data_gtfs_rt, time_df, 
                          config.sched_id_dict['gtfs-rt'], 
                          [config.transit_data_sql , schedule_monitor])
    
    
    # create the trip data table
    sf.create_traffic_data_table(config.traffic_data_sql)
    sf.create_proc_monitor_table(config.process_monitor_sql)
    sf.create_push_monitor_table(config.process_monitor_sql)
    sf.create_transit_data_siri_table(config.siri_table_name, 
                                      config.transit_data_sql)
    sf.create_transit_data_gtfs_rt_table(config.gfts_rt_table_name,
                                         config.transit_data_sql)
      
if __name__ == '__main__':
    main()
