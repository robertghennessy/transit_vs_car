"""
This program parses a GFTS file and creates the trips of interest. The trips of 
interest are saved in a csv file. The location of the csv file is config.trips_csv_path_out.

@author: Robert (rghennessy@gmail.com)
"""


import datetime
import os
import partridge as ptg
import numpy as np
import pandas as pd
from datetime import timedelta
import config

# Commute hours in hours in 24 hour format. Only trips within this window 
# will be outputte
morning_commute_hours = [6.5,9.5]
evening_commute_hours = [17,20.25]
weekday_names = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']

# top 10 stations come from http://www.caltrain.com/Assets/_Marketing/caltrain/pdf/2016/2017+Annual+Count+Key+Findings+Report.pdf
# Menlo Park excluded to reduce the number of google maps requests and the station is very close to Palo Alto
# the oututted trips will be between these stations
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


trip_list = []
for sInd in range(len(stations)):
    for eInd in range(sInd+1,len(stations)):
        trip_list.append((stations[sInd],stations[eInd]))
# append on the reverse trips        
trip_list = trip_list + [(end,start) for (start,end) in trip_list]

# read in the GFTS file to pandas
feed = ptg.raw_feed(config.gtfs_zip_path)
#schedule contains the schedule. Convert values to numbers if possible
schedule = feed.stop_times.apply(pd.to_numeric, errors='ignore')
# stops contain information about the stations. Convert values to numbers if possible
stops = feed.stops.apply(pd.to_numeric, errors='ignore')
# trips contains information on the toure type, direction and frequency of service code
trips = feed.trips.apply(pd.to_numeric, errors='ignore')
# calendar converts the frequency code to dates
calendar = feed.calendar.apply(pd.to_numeric, errors='ignore')

# calendar contains conversion from calendar code to days
# trips contain the calendar code for the different trips

def trip_timedelta(series):
    split_time = series.str.split(':')
    hours = pd.to_numeric(split_time.str[0].str.strip())
    mins = pd.to_numeric(split_time.str[1].str.strip())
    secs = pd.to_numeric(split_time.str[2].str.strip())
    return pd.to_timedelta(60*60*hours+60*mins+secs, unit='s')

# Create new columns with seconds since midnight of the first day
# Reason: There are a couple of trains that arrive the next day
schedule['arrival_time_timedelta'] = trip_timedelta(schedule['arrival_time'])
schedule['departure_time_timedelta'] = trip_timedelta(schedule['departure_time'])

# create short stop name - remove caltrain from the end of the name
stops['short_stop_name'] = stops['stop_name'].str.split('Caltrain',1).str[0].str.strip()

# create a list of stations from north to south
# drop the shuttle station, drop the duplicates, reset the index 
station_series = stops[stops['stop_id'] < stop_id_upper_limit]['short_stop_name'].drop_duplicates().reset_index(drop=True)


# create dataframe for the direction of the trip list
trip_list_df = pd.DataFrame(trip_list,columns=['start_station', 'end_station']) 
# add direction infomration 
# the stops dataframe is ordered so that north stations have lower number
for fInd in range(len(trip_list_df)):
    start_station_name = trip_list_df.iloc[fInd]['start_station']
    end_station_name = trip_list_df.iloc[fInd]['end_station']
    start_station_stop_id = stops.loc[stops['short_stop_name']==start_station_name, 'stop_id'].max()
    end_station_stop_id = stops.loc[stops['short_stop_name']==end_station_name, 'stop_id'].max()
    if(end_station_stop_id < start_station_stop_id):
        trip_list_df.ix[fInd, 'train_direction'] = 0
    else:
        trip_list_df.ix[fInd, 'train_direction'] = 1

# remove the shuttles and special trains
train_numbers = list(set(schedule.trip_id))
# normal train are numbers only
train_numbers = [numb for numb in train_numbers if(str.isnumeric(numb))]


# create a combination of the schedule
schedule_trips = pd.merge(schedule,schedule,on='trip_id', suffixes=('_start', '_stop'))
# remove trips going the wrong direction
schedule_trips = schedule_trips.drop(schedule_trips[schedule_trips['stop_sequence_start'] >= schedule_trips['stop_sequence_stop']].index)
# add the station location to the schedule dataframe
schedule_trips = pd.merge(schedule_trips, stops, left_on='stop_id_start', right_on='stop_id')
schedule_trips = pd.merge(schedule_trips, stops, left_on='stop_id_stop', right_on='stop_id', suffixes=('_start', '_stop'))
# add the service_id (what type of service is it?)
schedule_trips = pd.merge(schedule_trips, trips, on='trip_id')
# convert the service code to days of week
schedule_trips = pd.merge(schedule_trips, calendar, on='service_id')
# remove the special trains
schedule_trips = schedule_trips[schedule_trips['trip_id'].isin(train_numbers)]
# convert the trip id to a number
schedule_trips['trip_id'] = pd.to_numeric(schedule_trips['trip_id'])

# select the correct trips by location
trip_list_df = trip_list_df.set_index(['start_station', 'end_station'])
trip_list_df.index.names = ['short_stop_name_start', 'short_stop_name_stop']
schedule_trips = schedule_trips.set_index(['short_stop_name_start', 'short_stop_name_stop'])
schedule_trips = pd.merge(trip_list_df,schedule_trips,left_index=True, right_index=True)
# Remove weekend only trips
schedule_trips = schedule_trips[schedule_trips[weekday_names].sum(axis=1)>0]

# filter in the commute only trains
# departure_time_timedelta_start
# arrival_time_timedelta_stop
morning_commute_hours_td = [timedelta(hours=x) for x in morning_commute_hours]
evening_commute_hours_td = [timedelta(hours=x) for x in evening_commute_hours]

schedule_trips = schedule_trips[
    (((schedule_trips['departure_time_timedelta_start'] > morning_commute_hours_td[0]) &
    (schedule_trips['arrival_time_timedelta_stop'] < morning_commute_hours_td[1]))) |
    (((schedule_trips['departure_time_timedelta_start'] > evening_commute_hours_td[0]) &
    (schedule_trips['arrival_time_timedelta_stop'] < evening_commute_hours_td[1])))]

# reset the index to numbers
schedule_trips = schedule_trips.reset_index()

#  remove suboptimal trains (slower to take the train than wait for the next train)
# rank the deparature time and arrival time for the city combinations
schedule_trips['deparature_time_rank'] = schedule_trips.groupby(['short_stop_name_start', 'short_stop_name_stop'])['departure_time_timedelta_start'].rank(method='dense').astype(int)
schedule_trips['arrival_time_rank'] = schedule_trips.groupby(['short_stop_name_start', 'short_stop_name_stop'])['arrival_time_timedelta_stop'].rank(method='dense').astype(int)
# remove the trains where the deparature rank is less than arrival rank
# because this means that a later train will arrive before this one
schedule_trips = schedule_trips[schedule_trips['deparature_time_rank'] >= schedule_trips['arrival_time_rank']]
# order the trains based on deperature time
schedule_trips = schedule_trips.sort_values(['departure_time_timedelta_start', 'arrival_time_timedelta_stop'])
schedule_trips = schedule_trips.reset_index(drop=True)
schedule_trips['trip_index'] = schedule_trips.index
#schedule_trips = schedule_trips.rename('trip_order')
# output to csv
schedule_trips.to_csv(config.trips_csv_path_out)

### debug code
#day_code = 'monday'
#direction_code = 0
#day_schedule = schedule_trips[(schedule_trips['direction_id'] == direction_code) & (schedule_trips[day_code] == 1)]
#day_schedule = day_schedule.sort_values(['trip_id', 'stop_sequence_start', 'stop_sequence_stop'])
#day_schedule = day_schedule[['trip_id', 'short_stop_name_start','departure_time_start', 'short_stop_name_stop', 'arrival_time_stop']]
#day_schedule.to_csv('day_schedule.csv')
