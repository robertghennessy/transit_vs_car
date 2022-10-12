"""
Description: This file contains the functions that are used to collect data.
    
@author: Robert Hennessy (robertghennessy@gmail.com)
"""
import datetime as dt

import dateutil.parser as dp
import json
import logging
import os
import pandas as pd
import random
import requests

import googlemaps
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import tenacity as ten

import config
import push_notification as pn
import sql_functions as sf
import table_def

agency = 'CT'
gtfs_rt_api = 'http://api.511.org/Transit/TripUpdates?api_key='
siri_api = 'http://api.511.org/Transit/StopMonitoring?api_key='
# send push notification is greater than x minutes
warn_delay_threshold = 5 * 60

# access the root logger
logger = logging.getLogger('')

siri_columns = ['time_index', 'recorded_at_time_date', 'recorded_at_time_time',
                'recorded_at_time_utc', 'station_name', 'stop_id', 'trip_id',
                'vehicle_at_stop', 'aimed_arrival_time_date',
                'aimed_arrival_time_time', 'aimed_arrival_time_utc',
                'aimed_departure_time_date', 'aimed_departure_time_time',
                'aimed_departure_time_utc']
gfts_columns = ['time_index', 'recorded_at_time_date', 'recorded_at_time_time',
                'recorded_at_time_utc', 'stop_id', 'trip_id',
                'aimed_departure_time_date', 'aimed_departure_time_time',
                'aimed_departure_time_utc']
columns_to_compare = ['train_start_date', 'trip_id', 'stop_id']

RETRY_PARAMS = dict(wait=ten.wait_random_exponential(multiplier=1, max=10),
                    reraise=True, stop=ten.stop_after_attempt(5),
                    before_sleep=ten.before_sleep_log(logger, logging.DEBUG))

""" Google Traffic Functions """


def query_google_traffic(trip_index, trip_id, start_station, end_station,
                         start_loc, end_loc, sql_db_loc):
    """
    Calls function to query google maps for duration in traffic. Stores the 
        results in sqlite database.
    
    :paramtrip_index: index for the trip. The index is assigned when parsing
        the gfts
    :typetrip_index: int
    
    :paramtrip_id: train number
    :typetrip_id: int
    
    :paramstart_station: name of the start station
    :typestart_station: string
    
    :paramend_station: name of the end station
    :typeend_station: string
    
    :paramstart_loc: dict that contains the latitude and longitude of the
        start station
    :typestart_loc: dictionary
        
    :paramend_loc: dict that contains the latitude and longitude of the
        start station
    :typeend_loc: dictionary
    
    :paramsql_db_loc: location of the sql database where to store the results
        from query google
    :typesql_db_loc: string
    
    :returnNone
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
    sf.insert_traffic_data(sql_db_loc, data_tuple)
    # log the task that was just completed
    print_str = (str(trip_index) + ': ' + start_station + ' to ' + end_station
                 + ' on ' + date_str + ' at ' + time_str)
    logging.info(print_str)
    return None


@ten.retry(**RETRY_PARAMS)
def query_google_api(start_loc, end_loc):
    """
    This program queries the google api to determine the driving time between
        the start and location. The retry wrapper will retry this function if 
        an error occurs. The method is exponential back off with jitter and it
        will retry 5 times before raising an exception.
    
    :param start_loc: dict that contains the latitude and longitude of the
        start station
    :type start_loc: dictionary
        
    :param end_loc: dict that contains the latitude and longitude of the
        start station
    :type end_loc: dictionary
    
    :return duration_in_traffic: extracted duration in traffic in seconds
    :type float
    
    :return directions_result: the results returned by querying google maps
    :type dictionary
    
    """
    # timeout after 5 seconds    
    gmaps = googlemaps.Client(key=config.google_transit_api_key, timeout=5)
    now = dt.datetime.now()
    # query google maps for the results
    directions_result = gmaps.directions(start_loc,
                                         end_loc,
                                         mode="driving",
                                         departure_time=now)
    # duration in traffic in seconds                          
    duration_in_traffic = (directions_result[0]['legs'][0][
        'duration_in_traffic']['value'])
    return duration_in_traffic, directions_result


""" Transit Functions """

""" Siri Functions """


def query_transit_data_siri(data_db_location, schedule_monitor, time_index):
    """
    :param data_db_location: location of the sql database to store the results
    :type string
    
    :param schedule_monitor: data frame that contains schedule information
    :type schedule_monitor: pandas data frame
    
    :param time_index: time index for when the data is collected
    :type integer
    
    :return None:
    """
    monitored_stops = query_siri()
    write_transit_data_to_json(config.siri_json_dir, 'siri-', monitored_stops)
    parsed_data = parse_siri_transit_data(monitored_stops, time_index)
    parsed_data_with_delays = compare_actual_to_schedule(parsed_data,
                                                         schedule_monitor)
    save_transit_data(parsed_data_with_delays, 'siri', data_db_location)
    # Save to task monitor database    
    sf.insert_periodic_task_monitor(data_db_location, time_index)
    # determine the delayed trains
    (max_departure_delay, delayed_trains) = determine_delayed_trains(
        parsed_data_with_delays)
    # send a push notification if the trains are delayed significantly
    if max_departure_delay >= warn_delay_threshold:
        pn.delay_push_notify(config.push_notification_sql, delayed_trains)
    return None


@ten.retry(**RETRY_PARAMS)
def query_siri():
    """
    Query the 511 api to collect stop monitoring information. Convert the json
        to a dict
    
    :param None
    
    :return list with the stop monitoring information
    """
    url = (siri_api + config.transit_511_api_key + '&agency=' + agency +
           '&Format=JSON')
    json_url = requests.get(url)
    data = json.loads(json_url.content.decode('utf-8-sig'))
    return (data['ServiceDelivery']['StopMonitoringDelivery']
            ['MonitoredStopVisit'])


@ten.retry(**RETRY_PARAMS)
def parse_siri_transit_data(monitored_stops, time_index):
    """
    Parses the siri transit data provided by the query command.

    :param monitored_stops: dictionary returned by the query siri function
    :type dictionary
    
    :param time_index: time index for when the data is collected
    :type integer
    
    :return data: pandas data frame that contains the parsed data
    :type data: pandas data frame
    """
    data_list = []
    for sInd in range(0, len(monitored_stops)):
        # parse the dictionary
        try:
            recorded_at_time = monitored_stops[sInd]['RecordedAtTime']
            monitored_vehicle_journey = (monitored_stops[sInd][
                'MonitoredVehicleJourney'])
            trip_id = (monitored_vehicle_journey['FramedVehicleJourneyRef'][
                'DatedVehicleJourneyRef'])
            monitored_call = monitored_vehicle_journey['MonitoredCall']
            station_name = monitored_call['StopPointName']
            stop_id = int(monitored_call['StopPointRef'])
            aimed_arrival_time = monitored_call['AimedArrivalTime']
            aimed_departure_time = monitored_call['AimedDepartureTime']
            vehicle_at_stop = monitored_call['VehicleAtStop']
        except Exception:
            logging.exception('')
            continue
        # create recorded time objects
        (recorded_at_time_utc, recorded_at_time_date,
         recorded_at_time_time) = create_time_objects(
            dp.parse(recorded_at_time),
            config.to_zone)
        # convert special train to format used in gfts
        if trip_id[0] == 'S':
            date_str = dt.datetime.now().strftime('%m%d%Y')
            trip_id = trip_id + '_' + date_str
        # arrival time objects
        (aimed_arrival_time_utc, aimed_arrival_time_date,
         aimed_arrival_time_time) = create_time_objects(
            dp.parse(aimed_arrival_time), config.to_zone)
        # departure time objects
        (aimed_departure_time_utc, aimed_departure_time_date,
         aimed_departure_time_time) = create_time_objects(
            dp.parse(aimed_departure_time), config.to_zone)
        # append the results to the data list
        cur_data = [time_index, recorded_at_time_date, recorded_at_time_time,
                    recorded_at_time_utc, station_name, stop_id, trip_id,
                    vehicle_at_stop, aimed_arrival_time_date,
                    aimed_arrival_time_time, aimed_arrival_time_utc,
                    aimed_departure_time_date, aimed_departure_time_time,
                    aimed_departure_time_utc]
        data_list.append(cur_data)
    data = pd.DataFrame(data_list, columns=siri_columns)
    data_columns = ordered_unique_list(siri_columns + gfts_columns)
    data = data.reindex(columns=data_columns)
    return data


""" GTFS-RT Functions"""


def query_transit_data_gtfs_rt(data_db_location, schedule_monitor, time_index):
    """
    :param data_db_location: location of the sql database to store the results
    :type string
    
    :param schedule_monitor: data frame that contains schedule information
    :type schedule_monitor: pandas data frame
    
    :param time_index: time index for when the data is collected
    :type integer
    
    :return None:
    """
    monitored_stops = query_gtfs_rt()
    parsed_data = parse_gtfs_rt_transit_data(monitored_stops, time_index)
    parsed_data_with_delays = compare_actual_to_schedule(parsed_data,
                                                         schedule_monitor)
    save_transit_data(parsed_data_with_delays, 'gtfs-rt', data_db_location)
    # Save to task monitor database    
    sf.insert_periodic_task_monitor(data_db_location, time_index)
    # determine the delayed trains
    (max_departure_delay, delayed_trains) = determine_delayed_trains(
        parsed_data_with_delays)
    # send a push notification if the trains are delayed significantly
    if max_departure_delay >= warn_delay_threshold:
        pn.delay_push_notify(config.push_notification_sql, delayed_trains)
    return None


@ten.retry(**RETRY_PARAMS)
def parse_gtfs_rt_transit_data(transit_data, time_index):
    """
    Parses the gtfs-rt transit data provided by the query command.

    :param transit_data: dictionary returned by the query gtfs-rt function
    :type dictionary
    
    :param time_index: time index for when the data is collected
    :type integer
    
    :return data: pandas data frame that contains the parsed data
    :type data: pandas data frame
    """
    data_list = []
    recorded_at_time = transit_data['header']['timestamp']
    entities = transit_data['entity']
    # create the recorded at time objects
    recorded_at_time_dt = dt.datetime.utcfromtimestamp(float(recorded_at_time))
    recorded_at_time_dt = recorded_at_time_dt.replace(tzinfo=config.from_zone)
    (recorded_at_time_utc, recorded_at_time_date,
     recorded_at_time_time) = create_time_objects(recorded_at_time_dt,
                                                  config.to_zone)
    for entity in entities:
        try:
            trip_id = entity['tripUpdate']['trip']['tripId']
            stop_time_update = entity['tripUpdate']['stopTimeUpdate']
        except Exception:
            logging.exception('')
            continue
        # loop through the stop time updates
        for stop_time in stop_time_update:
            try:
                stop_id = int(stop_time['stopId'])
                aimed_departure_time = stop_time['departure']['time']
            except Exception:
                logging.exception('')
                continue
            # created the aimed departure time objects
            aimed_departure_time_dt = dt.datetime.utcfromtimestamp(
                float(aimed_departure_time))
            aimed_departure_time_dt = aimed_departure_time_dt.replace(
                tzinfo=config.from_zone)
            (aimed_departure_time_utc, aimed_departure_time_date,
             aimed_departure_time_time) = create_time_objects(
                aimed_departure_time_dt, config.to_zone)
            cur_data = [time_index, recorded_at_time_date,
                        recorded_at_time_time,
                        recorded_at_time_utc, stop_id, trip_id,
                        aimed_departure_time_date, aimed_departure_time_time,
                        aimed_departure_time_utc]
            data_list.append(cur_data)
    data = pd.DataFrame(data_list, columns=gfts_columns)
    data_columns = ordered_unique_list(gfts_columns + siri_columns)
    data = data.reindex(columns=data_columns)
    return data


@ten.retry(**RETRY_PARAMS)
def query_gtfs_rt():
    """
    Query the 511 api to collect trip update information. Convert the json
        to a dict
    
    :param None
    
    :return dictionary with the stop monitoring information
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    url = (gtfs_rt_api + config.transit_511_api_key + '&agency=' + agency)
    response = requests.get(url)
    feed.ParseFromString(response.content)
    return MessageToDict(feed)


""" Transit Helper Functions """


def compare_actual_to_schedule(data, schedule_monitor):
    """
    Compares the parsed data to the schedule. Determines which trains are on 
        time.
    
    :param data: data frame that contains the parsed information from siri or
        gtfs-rt
    :type data: pandas data frame
    
    :param schedule_monitor: data frame that contains schedule information
    :type schedule_monitor: pandas data frame
    
    :return data: pandas data frame with the information regarding on time
        performance joined to it
    :type pandas data frame
    """
    # create time in seconds from midnight
    data['aimed_arrival_time_seconds'] = seconds_from_midnight(
        data['aimed_arrival_time_time'])
    data['aimed_departure_time_seconds'] = seconds_from_midnight(
        data['aimed_departure_time_time'])
    # create multi-indexes so that they can be merged
    data.set_index(['trip_id', 'stop_id'], inplace=True)
    # merge the two data frames on the index, only keep records in both
    # data frames
    data = pd.merge(data, schedule_monitor, right_index=True, left_index=True,
                    how='inner')
    data.reset_index(inplace=True)
    # determine the start date of the train
    data['train_start_date'] = \
        ((pd.to_datetime(data['aimed_departure_time_date'])
          + pd.to_timedelta(data['trip_start_date_delta'],
                            unit='D')).dt.strftime("%Y-%m-%d"))
    # drop the column trip_start_date_delta because they are not needed
    data.drop(columns=['trip_start_date_delta'], inplace=True)
    # Determine if the train is on time
    data['departure_on_time'] = (data['aimed_departure_time_seconds'] <=
                                 data['scheduled_departure_time_seconds'])
    data['arrival_on_time'] = (data['aimed_arrival_time_seconds'] <=
                               data['scheduled_arrival_time_seconds'])
    # calculate the delay
    data['departure_delay'] = (data['aimed_departure_time_seconds'] -
                               data['scheduled_departure_time_seconds'])
    data['arrival_delay'] = (data['aimed_arrival_time_seconds'] -
                             data['scheduled_arrival_time_seconds'])
    return data


def determine_delayed_trains(data):
    """    
    Creates a data frame with the delayed trains and determines the maximum
        departure delay.
    
    :param data: pandas data frame that contains the parsed real time data
    :type data: pandas data frame
    
    :return delayed_trains: data frame that contains the delayed trains
    :type delayed_trains: pandas data frame
    
    :return max_departure_delay: maximum departure delay
    :type max_departure_delay: float
    """
    # select trains with delay greater than 2 minutes
    delayed_trains = data[data['departure_delay'] >= 0].copy(deep=True)
    delayed_trains['departure_delay'] = delayed_trains['departure_delay'] / 60
    # sort by departure time, ascending
    delayed_trains.sort_values('aimed_departure_time_seconds', inplace=True)
    # group the data by trip_id and return the first one
    # this is the next stop that the train will be going to
    delayed_trains = delayed_trains.groupby('trip_id').first()
    delayed_trains.reset_index(inplace=True)
    delayed_trains = delayed_trains[['trip_id', 'short_stop_name',
                                     'departure_delay']]
    delayed_trains = delayed_trains.sort_values(by='departure_delay',
                                                ascending=False,
                                                na_position='last')
    delayed_trains = delayed_trains.loc[
        delayed_trains['departure_delay'] > warn_delay_threshold]
    max_departure_delay = delayed_trains['departure_delay'].max()
    return max_departure_delay, delayed_trains


@ten.retry(**RETRY_PARAMS)
def save_transit_data(data, type_switch, db_location):
    """
    Saves the transit data to sql database.
    
    :param data: pandas databased that contains the data that should be saved
        in sql database
    :type pandas data frame
    
    :param type_switch: string that is used to switch between siri and
        gtfs-rt
    :type type_switch: string
    
    :param db_location: location of the sql file that the data is stored in
    :type string
    """
    # select the appropriate table names
    if type_switch == 'siri':
        table_name = config.siri_table_name
        data_format_dict = table_def.siri_dict
    elif type_switch == 'gtfs-rt':
        table_name = config.gfts_rt_table_name
        data_format_dict = table_def.gtfs_dict
    else:
        raise Exception('The type_switch ({}) is not supported'.format(
            type_switch))
    # save the results to sql database
    # prepare the pandas data to upload to sql
    prepared_data = sf.prepare_pandas_to_sql(data, data_format_dict)
    sf.update_entries(db_location, table_name,
                      prepared_data, columns_to_compare)


""" Time Functions """


def create_time_objects(input_dt, to_time_zone):
    """
    Parses a utc time string and returns different time objects.

    :paraminput_dt: input datetime object with timezone information
    :typeinput_dt: datetime
    
    :paramto_time_zone: time zone object that is used to convert the utc time
        to the to_time_zone
    :typeto_time_zone: time zone object
    
    :returnutc:
    :typeutc: float
    
    :returndate: Year, month and date for the utc_time_str converted to the
        to_time_zone
    :typedate: string

    :returntime: hour, minute and second for the utc_time_str converted to
        the to_time_zone
    :typetime: string

    """
    new_dt = input_dt.astimezone(to_time_zone)
    utc = input_dt.timestamp()
    date = new_dt.strftime('%Y-%m-%d')
    time = new_dt.strftime('%H:%M:%S')
    return utc, date, time


def seconds_from_midnight(time_series):
    """
    Converts the time series to seconds from midnight
    
    :param time_series: a series that contains the time strings
    :type time_series: series

    :return pandas timedelta object
    """
    (hours, minutes, secs) = parse_time_series(time_series)
    return pd.Series(60 * 60 * hours + 60 * minutes + secs)


def parse_time_series(time_series):
    """    
    Splits the time_series into hours, minutes and seconds
    
    :param time_series: a series that contains the time strings
    :type time_series: series
    
    :return hours: hours
    :type int
    
    :return minutes: minutes
    :type int
    
    :return secs: seconds
    :type int
    
    """
    time_series = time_series.astype('object')
    split_time = time_series.str.split(':')
    hours = pd.to_numeric(split_time.str[0].str.strip())
    minutes = pd.to_numeric(split_time.str[1].str.strip())
    secs = pd.to_numeric(split_time.str[2].str.strip())
    return hours, minutes, secs


def trip_timedelta(time_series):
    """
    Converts the time series to a timedelta object
    
    :param time_series: a series that contains the time strings
    :type time_series: series

    :return pandas timedelta object
    """
    (hours, minutes, secs) = parse_time_series(time_series)
    return pd.to_timedelta(60 * 60 * hours + 60 * minutes + secs, unit='s')


""" Dummy Functions Used to Test Functionality """


def dummy_function(trip_index, trip_id, start_station, end_station, start_loc,
                   end_loc, sql_db_loc):
    """
    Dummy function that is used to test the scheduler. Designed to mimic
        google traffic.
    
    :paramtrip_index: index for the trip. The index is assigned when parsing
        the gfts
    :typetrip_index: int
    
    :paramtrip_id: train number
    :typetrip_id: int
    
    :paramstart_station: name of the start station
    :typestart_station: string
    
    :paramend_station: name of the end station
    :typeend_station: string
    
    :paramstart_loc: dict that contains the latitude and longitude of the
        start station
    :typestart_loc: dictionary
        
    :paramend_loc: dict that contains the latitude and longitude of the
        start station
    :typeend_loc: dictionary
    
    :paramsql_db_loc: location of the sql database where to store the results
        from querying google
    :typesql_db_loc: string
    
    return: None
    """
    # construct time objects
    date_str = dt.datetime.now().date().isoformat()  # string
    time_str = dt.datetime.now().time().isoformat()  # string
    day_of_week = dt.datetime.now().isoweekday()  # integer
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
    sf.insert_traffic_data(sql_db_loc, data_tuple)

    return None


@ten.retry(**RETRY_PARAMS)
def dummy_query_google_api():
    """
    This program is a dummy function used to test various aspects. The retry 
        wrapper will retry this function if an error occurs. The dummy function
        will randomly produce a key error. The probability was set so that the
        key error will propagate after the five attempts. The method is 
        exponential back off with jitter and it will retry 5 times before
        raising an exception.
    
    :return directions_result: the results returned by querying google maps
    :type dictionary
    
    """
    directions_result = 'example results'
    duration_in_traffic = 15
    if random.uniform(0, 1) >= 0.13:
        raise ValueError('Dummy Query Function: mimic error during query')
    return duration_in_traffic, directions_result


""" Support Functions """


def periodic_dummy_function(tst_arg_0, tst_arg_1):
    """
    This program is a dummy function to test periodic functions.
   
    :param tst_arg_0: string that is appended to beginning of the printed
        statement 
    :type tst_arg_0: string
    
    :param tst_arg_1: string that is appended to end of the printed statement
    :type tst_arg_1: string
    
    return: None
    """
    date_str = dt.datetime.now().isoformat()
    print(tst_arg_0 + date_str + tst_arg_1)


def ordered_unique_list(seq):
    """
    Reduces a list to unique values only while maintaining order.
    
    :param seq: list that will be reduced to unique values only
    :type seq: list
    
    return: list
    """
    seen = set()
    return [x for x in seq if x not in seen and not seen.add(x)]


def write_transit_data_to_json(file_directory, file_prefix, json_data):
    """
    Writes a json to a file
    
    :param file_directory: the name of the table to be deleted
    :type file_directory: string
    
    :param file_prefix: the prefix that is added to beginning of the
        filename
    :type file_prefix: string
    
    :param json_data: the data to be stored in json
    :type json_data: string
    
    :return None
    """

    date_tag = dt.datetime.now().strftime("%Y-%b-%d_%H-%M-%S")
    file_name = file_prefix + date_tag + '.json'
    file_name = os.path.join(file_directory, file_name)
    with open(file_name, 'w') as outfile:
        json.dump(json_data, outfile)
    return None
