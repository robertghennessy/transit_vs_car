"""
Description: This program parses a gfts file and creates a task database which 
    will be used by collect_traffic_data
    
@author: Robert Hennessy (robertghennessy@gmail.com)
"""

import datetime as dt
import os
import partridge as ptg
import pandas as pd
from itertools import permutations
import config
from apscheduler.schedulers.background import BackgroundScheduler
import collect_traffic_data as ctd
import sql_functions as sf

# Commute hours in hours in 24 hour format. Only trips within this window 
# will be outputted
morning_commute_hours = [6.5,9.5]
evening_commute_hours = [17,20.25]
weekday_names = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 
                 'saturday', 'sunday']
days_of_interest = weekday_names[0:5]
day_of_week_codes = ['mon','tue','wed','thu','fri','sat','sun']

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

def parse_timeseries(time_series):
    """    
    Splits the time_series into hours, minutes and seconds
    
    :param: time_series: a series that contains the time strings
    :type: time_series: series
    
    :return: hours: hours
    :type: int
    
    :return: mins: minutes
    :type: int
    
    :return: secs: seconds
    :type: int
    
    """    
    split_time = time_series.str.split(':')
    hours = pd.to_numeric(split_time.str[0].str.strip())
    mins = pd.to_numeric(split_time.str[1].str.strip())
    secs = pd.to_numeric(split_time.str[2].str.strip())
    return (hours, mins, secs)


def trip_timedelta(time_series):
    """
    Converts the time series to a timedelta object
    
    :param: time_series: a series that contains the time strings
    :type: time_series: series

    : return: pandas timedelta object
    """
    (hours, mins, secs) = parse_timeseries(time_series)
    return pd.to_timedelta(60*60*hours+60*mins+secs, unit='s')
    
def seconds_from_midnight(time_series):
    """
    Converts the time series to seconds from midnight
    
    :param: time_series: a series that contains the time strings
    :type: time_series: series

    : return: pandas timedelta object
    """
    (hours, mins, secs) = parse_timeseries(time_series)
    return pd.Series(60*60*hours+60*mins+secs)


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


def parse_gfts(stations, zip_path, csv_out_path, schedule_monitor_csv_path):
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
    
    :param: schedule_monitor_csv_path: file location of the schedule monitor
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
    # remove the special trains
    schedule = schedule[schedule['trip_id'].isin(train_numbers)] 
    # convert the trip id to a number
    #schedule['trip_id'] = pd.to_numeric(schedule['trip_id'])
    # create the schedule monitor
    create_schedule_monitor_csv(schedule,schedule_monitor_csv_path)
    # Create new columns with seconds since midnight of the first day because 
    # some of the trains arrive on the following day
    schedule['arrival_time_timedelta'] = trip_timedelta(
                                            schedule['arrival_time'])
    schedule['departure_time_timedelta'] = trip_timedelta(
                                            schedule['departure_time'])
    # create short stop name - remove caltrain from the end of the name
    stops['short_stop_name'] = stops['stop_name'].str.split(
                                    'Caltrain',1).str[0].str.strip()
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


def create_schedule_monitor_csv(schedule,csv_out_path):
    """ 
    Parses the schedule and creates a csv file to use to determine on time 
        performance
    
    :param: schedule: Dataframe that contains the scheduler information
    :type: pandas dataframe

    :param: csv_out_path: file location of the output csv file
    :type: string   
    
    :return: None
    """
    schedule['arrival_time_seconds'] = seconds_from_midnight(
                                            schedule['arrival_time'])
    schedule['departure_time_seconds'] = seconds_from_midnight(
                                            schedule['departure_time'])
    schedule.to_csv(csv_out_path, columns  = ['trip_id', 'stop_id', 
                                          'arrival_time_seconds', 
                                          'departure_time_seconds'])
    return None

def create_job_database(function_to_run, csv_path_in, sched_sql_loc, 
                        out_sql_loc):
    """
    Create the job database that the scheduler uses.
    
    :param: function_to_run: The function that is being scheduled
    :type: csv_path_in: function   
    
    :param: csv_path_in: The path to the csv file that contains the trip
        information. 
    :type: csv_path_in: string
    
    :param: sched_sql_loc: location of the sql job database generated by 
        this program and used by the scheduler
    :type: sched_sql_loc: string
    
    :param: out_sql_loc: location of the sql job database that the scheduled
        task stors its results
    :type: sched_sql_loc: string
    
    :return: None
    """
    schedule_trips = pd.read_csv(csv_path_in, index_col=0)
    schedule_trips = schedule_trips.sort_values([
        'departure_time_timedelta_start', 'arrival_time_timedelta_stop'])
    schedule_trips_index = schedule_trips.index
    # If the scheduler file exists, delete it
    if os.path.isfile(sched_sql_loc):
        os.remove(sched_sql_loc)
    else:    ## Show an error ##
        print("Error: %s file not found" % sched_sql_loc)
    # create the scheduler object and associate the job database with it
    scheduler = BackgroundScheduler()
    scheduler.add_jobstore('sqlalchemy', url='sqlite:///%s' % sched_sql_loc)
    # loop through all of the trips and add them to the jobs database
    for sInd in range(len(schedule_trips)):
            trip = schedule_trips.loc[sInd]
            trip_index = schedule_trips_index[sInd]
            trip_id = trip['trip_id']
            start_station = trip['short_stop_name_start']
            end_station = trip['short_stop_name_stop']
            # create the location dictionaries
            start_loc = {
                "lat" : trip['stop_lat_start'],
                "lng" : trip['stop_lon_start']
            }
            end_loc = {
                "lat" : trip['stop_lat_stop'],
                "lng" : trip['stop_lon_stop']
            }         
            sched_time = dt.datetime.strptime(trip['departure_time_start'],
                                           "%H:%M:%S")
            day_code =''
            for day_ind in range(len(weekday_names)):
                if trip[weekday_names[day_ind]]:
                    if day_code == '':
                        day_code = day_code + day_of_week_codes[day_ind]
                    else :
                        day_code = day_code + ',' + day_of_week_codes[day_ind]
            # misfire_grace_time - seconds after the designated runtime that 
                # the job is still allowed to be run
            scheduler.add_job(function_to_run,'cron',day_of_week=day_code, 
                              hour=sched_time.hour, minute=sched_time.minute, 
                              misfire_grace_time=120,id=str(trip_index),
                              args=[trip_index, trip_id, start_station, 
                                    end_station, start_loc, end_loc, 
                                    out_sql_loc])
    scheduler.print_jobs()
    scheduler.start()
    scheduler.shutdown()
    return None

def main():
    if os.path.isfile(config.trips_csv_path_out):
        os.remove(config.trips_csv_path_out)
    else:    # Show an error #
        print("Error: %s file not found" % config.trips_csv_path_out)
    if os.path.isfile(config.scheduler_sql_loc):
        os.remove(config.scheduler_sql_loc)
    else:    # Show an error #
        print("Error: %s file not found" % config.scheduler_sql_loc)
    parse_gfts(stations,config.gtfs_zip_path, config.trips_csv_path_out, 
               config.schedule_monitor_csv_path)
    create_job_database(ctd.query_google_traffic, config.trips_csv_path_out, 
                        config.scheduler_sql_loc, config.output_database)
    # create the trip data table
    sf.create_trip_data_table(config.output_database)
    sf.create_proc_monitor_table(config.output_database)
      
if __name__ == '__main__':
    main()
