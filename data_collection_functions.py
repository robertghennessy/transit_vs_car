"""
Description: This file contains the functions that are used to collect data.
    
@author: Robert Hennessy (robertghennessy@gmail.com)
"""
import datetime as dt
import dateutil.parser as dp
import json
import logging
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
import test_table_def as ttd

agency = 'CT'
gtfs_rt_api = 'http://api.511.org/Transit/TripUpdates?api_key='
siri_api = 'http://api.511.org/Transit/StopMonitoring?api_key='
# send push notification is greater than x minutes
warn_delay_threhold = 5*60

# access the root logger
logger = logging.getLogger('')

siri_columns = ['time_index', 'RecordedAtTime_date', 'RecordedAtTime_time', 
                'RecordedAtTime_utc', 'StationName', 'stop_id', 'trip_id', 
                'VehicleAtStop', 'AimedArrivalTime_date', 
                'AimedArrivalTime_time', 'AimedArrivalTime_utc', 
                'AimedDepartureTime_date', 'AimedDepartureTime_time', 
                'AimedDepartureTime_utc']
gfts_columns = ['time_index', 'RecordedAtTime_date', 'RecordedAtTime_time', 
                'RecordedAtTime_utc', 'stop_id', 'trip_id', 
                'AimedDepartureTime_date', 'AimedDepartureTime_time', 
                'AimedDepartureTime_utc']
columns_to_compare = ['TrainStartDate', 'trip_id', 'stop_id']             


""" Google Traffic Functions """

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
    sf.insert_traffic_data(sql_db_loc, data_tuple)
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


""" Transit Functions """

""" Siri Functions """

def query_transit_data_siri(data_db_location, schedule_monitor, time_index):
    """
    :param: data_db_location: location of the sql database to store the results
    :type: string
    
    :param: schedule_monitor: dataframe that contains schedule information
    :type: schedule_monitor: pandas dataframe
    
    :param: time_index: time index for when the data is collected
    :type: integer
    
    :return: None: 
    """
    MonitoredStops = query_siri()
    parsed_data = parse_siri_transit_data(MonitoredStops, time_index)
    parsed_data_with_delays = compare_actual_to_schedule(parsed_data, 
                                                         schedule_monitor)  
    save_transit_data(parsed_data_with_delays, 'siri', data_db_location)
    # Save to task monitor database    
    sf.insert_periodic_task_monitor(data_db_location, time_index)
    # determine the delayed trains
    (maxDeperatureDelay, delayed_trains) = determine_delayed_trains(
                                                parsed_data_with_delays)
    # send a push notification if the trains are delayed significantly
    if maxDeperatureDelay >= warn_delay_threhold:
        pn.delay_push_notify(config.push_notification_sql, delayed_trains)
    return None


@ten.retry(wait=ten.wait_random_exponential(multiplier=1, max=10),
       reraise=True, stop=ten.stop_after_attempt(5),
       before_sleep=ten.before_sleep_log(logger, logging.DEBUG))
def query_siri():
    """
    Query the 511 api to collect stop monitoring information. Convert the json
        to a dict
    
    :param: None
    
    :return: list with the stop monitoring information
    """  
    url = (siri_api + config.transit_511_api_key + '&agency=' + agency + 
        '&Format=JSON')
    json_url = requests.get(url)
    data = json.loads(json_url.content.decode('utf-8-sig'))
    return (data['ServiceDelivery']['StopMonitoringDelivery']
                                ['MonitoredStopVisit'])

@ten.retry(wait=ten.wait_random_exponential(multiplier=1, max=10),
       reraise=True, stop=ten.stop_after_attempt(5),
       before_sleep=ten.before_sleep_log(logger, logging.DEBUG))
def parse_siri_transit_data(MonitoredStops, time_index):
    """
    Parses the siri transit data provided by the query command.

    :param: MonitoredStops: dictionary returned by the query siri function
    :type: dictionary
    
    :param: time_index: time index for when the data is collected
    :type: integer
    
    :return: data: pandas dataframe that contains the parsed data
    :type: data: pandas dataframe
    """
    data_list = []
    for sInd in range(0,len(MonitoredStops)):
        # parse the dictionary
        try:      
            RecordedAtTime = MonitoredStops[sInd]['RecordedAtTime']
            MonitoredVehicleJourney = (MonitoredStops[sInd]
                                            ['MonitoredVehicleJourney'])
            trip_id = (MonitoredVehicleJourney['FramedVehicleJourneyRef']
                        ['DatedVehicleJourneyRef'])
            MonitoredCall = MonitoredVehicleJourney['MonitoredCall']
            StationName = MonitoredCall['StopPointName']
            stop_id = int(MonitoredCall['StopPointRef'])
            AimedArrivalTime = MonitoredCall['AimedArrivalTime']
            AimedDepartureTime = MonitoredCall['AimedDepartureTime']
            VehicleAtStop = MonitoredCall['VehicleAtStop']
        except Exception :
            logging.exception('')
            continue
        # create recorded time objects
        (RecordedAtTime_utc, RecordedAtTime_date, 
             RecordedAtTime_time) = create_time_objects(
                                        dp.parse(RecordedAtTime),
                                        config.to_zone)
        # convert special train to format used in gfts
        if trip_id[0] == 'S':
            date_str = dt.datetime.now().strftime('%m%d%Y')
            trip_id = trip_id + '_' + date_str
        #VehicleAtStopBool = (VehicleAtStop == 'true')
        # arrival time objects
        (AimedArrivalTime_utc, AimedArrivalTime_date, 
             AimedArrivalTime_time) = create_time_objects(
                 dp.parse(AimedArrivalTime), config.to_zone)
        # deperature time objects
        (AimedDepartureTime_utc, AimedDepartureTime_date, 
             AimedDepartureTime_time) = create_time_objects(
                 dp.parse(AimedDepartureTime), config.to_zone)  
        # append the results to the data list
        cur_data = [time_index, RecordedAtTime_date, RecordedAtTime_time, 
                RecordedAtTime_utc, StationName, stop_id, trip_id, 
                VehicleAtStop, AimedArrivalTime_date, 
                AimedArrivalTime_time, AimedArrivalTime_utc, 
                AimedDepartureTime_date, AimedDepartureTime_time, 
                AimedDepartureTime_utc]        
        data_list.append(cur_data)  
        RecordedAtTime_date
    data = pd.DataFrame(data_list, columns=siri_columns)
    data_columns = ordered_unique_list(siri_columns+gfts_columns) 
    data = data.reindex(columns=data_columns)    
    return data


""" GTFS-RT Functions"""

def query_transit_data_gtfs_rt(data_db_location, schedule_monitor, time_index):
    """
    :param: data_db_location: location of the sql database to store the results
    :type: string
    
    :param: schedule_monitor: dataframe that contains schedule information
    :type: schedule_monitor: pandas dataframe
    
    :param: time_index: time index for when the data is collected
    :type: integer
    
    :return: None: 
    """
    MonitoredStops = query_gtfs_rt()
    parsed_data = parse_gtfs_rt_transit_data(MonitoredStops, time_index)
    parsed_data_with_delays = compare_actual_to_schedule(parsed_data, 
                                                         schedule_monitor)  
    save_transit_data(parsed_data_with_delays, 'gtfs-rt', data_db_location)
    # Save to task monitor database    
    sf.insert_periodic_task_monitor(data_db_location, time_index)
    # determine the delayed trains
    (maxDeperatureDelay, delayed_trains) = determine_delayed_trains(
                                                parsed_data_with_delays)
    # send a push notification if the trains are delayed significantly
    if maxDeperatureDelay >= warn_delay_threhold:
        pn.delay_push_notify(config.push_notification_sql, delayed_trains)
    return None


@ten.retry(wait=ten.wait_random_exponential(multiplier=1, max=10),
       reraise=True, stop=ten.stop_after_attempt(5),
       before_sleep=ten.before_sleep_log(logger, logging.DEBUG))
def parse_gtfs_rt_transit_data(transit_data, time_index):
    """
    Parses the gtfs-rt transit data provided by the query command.

    :param: transit_data: dictionary returned by the query gtfs-rt function
    :type: dictionary
    
    :param: time_index: time index for when the data is collected
    :type: integer
    
    :return: data: pandas dataframe that contains the parsed data
    :type: data: pandas dataframe
    """ 
    data_list = []
    RecordedAtTime = transit_data['header']['timestamp']
    entities = transit_data['entity']
    # create the recorded at time objects
    RecordedAtTime_dt = dt.datetime.utcfromtimestamp(float(RecordedAtTime))
    RecordedAtTime_dt = RecordedAtTime_dt.replace(tzinfo=config.from_zone)
    (RecordedAtTime_utc, RecordedAtTime_date, 
             RecordedAtTime_time) = create_time_objects(RecordedAtTime_dt, 
                config.to_zone)  
    for entity in entities:
        try:
            trip_id = entity['tripUpdate']['trip']['tripId']
            stopTimeUpdate = entity['tripUpdate']['stopTimeUpdate']
        except:
            logging.exception('')
            continue
        # loop through the stop time updates
        for stopTime in stopTimeUpdate:
            try:
                stop_id = int(stopTime['stopId'])
                AimedDepartureTime = stopTime['departure']['time']
            except Exception:
                logging.exception('')
                continue
            # created the aimeed departue time objects
            AimedDepartureTime_dt = dt.datetime.utcfromtimestamp(
                float(AimedDepartureTime))
            AimedDepartureTime_dt = AimedDepartureTime_dt.replace(
                tzinfo=config.from_zone) 
            (AimedDepartureTime_utc, AimedDepartureTime_date, 
                 AimedDepartureTime_time) = create_time_objects(
                     AimedDepartureTime_dt, config.to_zone) 
            cur_data = [time_index, RecordedAtTime_date, RecordedAtTime_time, 
                RecordedAtTime_utc, stop_id, trip_id, 
                AimedDepartureTime_date, AimedDepartureTime_time, 
                AimedDepartureTime_utc]        
            data_list.append(cur_data)   
    data = pd.DataFrame(data_list, columns=gfts_columns)
    data_columns = ordered_unique_list(gfts_columns+siri_columns) 
    data = data.reindex(columns=data_columns) 
    return data


@ten.retry(wait=ten.wait_random_exponential(multiplier=1, max=10),
       reraise=True, stop=ten.stop_after_attempt(5),
       before_sleep=ten.before_sleep_log(logger, logging.DEBUG))	
def query_gtfs_rt():
    """
    Query the 511 api to collect trip update information. Convert the josn 
        to a dict
    
    :param: None
    
    :return: dictionary with the stop monitoring information
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
    
    :param: data: dataframe that contains the parsed information from siri or
        gtfs-rt
    :type: data: pnadas dataframe
    
    :param: schedule_monitor: dataframe that contains schedule information
    :type: schedule_monitor: pandas dataframe
    
    :return: data: pandas dataframe with the information regarding on time 
        performance joined to it
    :type: pandas dataframe    
    """
    # create time in seconds from midnight
    data['AimedArrivalTime_seconds'] = seconds_from_midnight(
                                                data['AimedArrivalTime_time'])
    data['AimedDepartureTime_seconds'] = seconds_from_midnight(
                                            data['AimedDepartureTime_time'])
    # create multiindexes so that they can be merged
    data.set_index(['trip_id','stop_id'], inplace=True)
    # merge the two dataframes on the index, only keep records in both dataframes
    data = pd.merge(data, schedule_monitor, right_index=True, left_index=True, 
                    how='inner')
    data.reset_index(inplace=True)
    # determine the start date of the train
    data['TrainStartDate'] = ((pd.to_datetime(data['AimedDepartureTime_date'])
                            + pd.to_timedelta(data['trip_start_date_delta'],
                                            unit='D')).dt.strftime("%Y-%m-%d"))
    # drop the column trip_start_date_delta because uneeded 
    data.drop(columns=['trip_start_date_delta'], inplace=True)                                       
    # Determine if the train is on time
    data['DeperatureOnTime'] = (data['AimedDepartureTime_seconds'] <= 
                                data['scheduled_departure_time_seconds'])
    data['ArrivalOnTime'] = (data['AimedArrivalTime_seconds'] <= 
                                data['scheduled_arrival_time_seconds'])
    # calculate the delay
    data['DeperatureDelay'] = (data['AimedDepartureTime_seconds'] - 
                                data['scheduled_departure_time_seconds'])
    data['ArrivalDelay'] = (data['AimedArrivalTime_seconds'] - 
                                data['scheduled_arrival_time_seconds'])   
    return (data)
    

def determine_delayed_trains(data):
    """    
    Creates a dataframe with the delayed trains and determines the maximum
        deperature delay.
    
    :param: data: pandas dataframe that contains the parsed real time data
    :type: data: pandas dataframe
    
    :return: delayed_trains: dataframe that contains the delayed trains
    :type: delayed_trains: pandas dataframe
    
    :return: maxDeperatureDelay: maximum deperature delay
    :type: maxDeperatureDelay: flaot
    """
    # select trains with delay greater than 2 minutes
    delayed_trains = data[data['DeperatureDelay'] >= 0].copy(deep=True)
    delayed_trains['DeperatureDelay'] = delayed_trains['DeperatureDelay']/60
    # sort by deperature time, ascending
    delayed_trains.sort_values('AimedDepartureTime_seconds', inplace=True)
    # group the data by trip_id and return the first one
    # this is the next stop that the train will be going to
    delayed_trains = delayed_trains.groupby('trip_id').first()
    delayed_trains.reset_index(inplace=True)
    delayed_trains = delayed_trains[['trip_id', 'short_stop_name', 
                                     'DeperatureDelay']]
    delayed_trains = delayed_trains.sort_values(by='DeperatureDelay', 
                                                ascending=False, 
                                                na_position='last')
    delayed_trains = delayed_trains.loc[
        delayed_trains['DeperatureDelay'] > warn_delay_threhold]
    maxDeperatureDelay = delayed_trains['DeperatureDelay'].max()
    return (maxDeperatureDelay, delayed_trains)
    
@ten.retry(wait=ten.wait_random_exponential(multiplier=1, max=10),
       reraise=True, stop=ten.stop_after_attempt(5),
       before_sleep=ten.before_sleep_log(logger, logging.DEBUG))	
def save_transit_data(data, type_switch, db_location):
    """
    Saves the transit data to sql database.
    
    :param: data: pandas databased that contains the data that should be saved
        in sql database
    :type: pandas dataframe
    
    :param: type_switch: string that is used to switch between siri and 
        gtfs-rt
    :type: type_switch: string
    
    :param: db_location: location of the sql file that the data is stored in
    :type: string
    """
    # select the appropiate table names
    if type_switch == 'siri':
        table_name = config.siri_table_name
        data_format_dict = ttd.siri_dict
    elif type_switch == 'gtfs-rt':
        table_name = config.gfts_rt_table_name
        data_format_dict = ttd.gtfs_dict
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
    Parases a utc time string and returns different time objects.

    :param: input_dt: input datetime object with timezone information
    :type: input_dt: datetime
    
    :param: to_time_zone: time zone object that is used to convert the utc time
        to the to_time_zone
    :type: to_time_zone: time zone object
    
    :return: utc:
    :type: utc: float
    
    :return: date: Year, month and date for the utc_time_str converted to the
        to_time_zone
    :type: date: string

    :return: time: hour, minute and second for the utc_time_str converted to 
        the to_time_zone
    :type: time: string    

    """    
    dt = input_dt.astimezone(to_time_zone)
    utc = input_dt.timestamp()
    date = dt.strftime('%Y-%m-%d')
    time = dt.strftime('%H:%M:%S')
    return (utc, date, time)


def seconds_from_midnight(time_series):
    """
    Converts the time series to seconds from midnight
    
    :param: time_series: a series that contains the time strings
    :type: time_series: series

    : return: pandas timedelta object
    """
    (hours, mins, secs) = parse_timeseries(time_series)
    return pd.Series(60*60*hours+60*mins+secs)
    

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
    time_series = time_series.astype('object')  
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
 

""" Dummy Functions Used to Test Functionality """

def dummy_function(trip_index, trip_id, start_station, end_station, start_loc, 
                   end_loc, sql_db_loc):
    """
    Dummy function that is used to test the scheduler. Designed to mimic
        google traffic.
    
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
    sf.insert_traffic_data(sql_db_loc, data_tuple)

    return None

@ten.retry(wait=ten.wait_random_exponential(multiplier=0.1, max=5),
       reraise=True, stop=ten.stop_after_attempt(5),
       before_sleep=ten.before_sleep_log(logger, logging.DEBUG))
def dummy_query_google_api():
    """
    This program is a dummy function used to test various aspects. The retry 
        wrapper will retry this function if an error occurs. The dummy function
        will randomly produce a key error. The probability was set so that the
        key error will propagate after the five attempts. The method is 
        exponential backoff with jitter and it will retry 5 times before 
        raising an exception.
    
    :return: directions_result: the results returned by querying googlemaps
    :type: dictionary
    
    """
    if random.uniform(0,1) <= 0.13:
        directions_result = [{'warnings': [], 'copyrights': 'Map data ©2018 Google', 'summary': 'US-101 N', 'legs': [{'steps': [{'distance': {'text': '318 ft', 'value': 97}, 'polyline': {'points': 'isbdFhdmiVVSFGHGRQJKLMNMRSFEFI'}, 'duration': {'text': '1 min', 'value': 14}, 'travel_mode': 'DRIVING', 'html_instructions': 'Head <b>southeast</b> on <b>Pacific Blvd</b>', 'end_location': {'lat': 37.5372658, 'lng': -122.296523}, 'start_location': {'lat': 37.53796639999999, 'lng': -122.2971724}}, {'distance': {'text': '0.8 mi', 'value': 1239}, 'polyline': {'points': '}nbdFf`miVRORQO]EICGACIOGIGEICKAI?C?IBGBEDC@CBABIDEDE@C@C@E?A?GAKEYi@A?AAOEo@kAgEsHmDqGw@uAm@aA?AA?ES?AACQ[Ya@SQ_@U{@k@SMc@Wy@_@OKg@U]UQMe@_@c@]a@c@QSEEMSMOIMi@y@S_@{@yAk@iAi@kAMUMW'}, 'duration': {'text': '3 mins', 'value': 166}, 'travel_mode': 'DRIVING', 'html_instructions': 'Turn <b>left</b> to merge onto <b>E Hillsdale Blvd</b>', 'end_location': {'lat': 37.54478460000001, 'lng': -122.2874522}, 'start_location': {'lat': 37.5372658, 'lng': -122.296523}}, {'distance': {'text': '6.7 mi', 'value': 10751}, 'polyline': {'points': '{}cdFpgkiVM_ACW?U@QDQDKBILUFEHCFCJAH@H?D@HBHDBDBDBBBD@DBF@F@H?F@D?DAFAHAHGLILGHoCbD{AdB]z@cB|AwBlBaCxBwApAqBdByBnBi@f@wBlB_GjF}GfGaCvBsAnAiAfAmBjBoBhBoAjAm@h@cCxBuAlAyAtAgDtCiA`AcBzA}@v@{@t@k@h@_@^o@l@q@r@o@n@q@r@UVY\\e@f@u@z@aBhBi@j@aFtFyA`BgBpB{AdByB`CkCxCaBhBeBlBwBbCsAxAoBxBq@r@YZqEdF_BdBoAvAe@h@wGpHmD|DkHdIyFlGKLkBrBu@|@yEhFsQhSyHrImBtBw@~@{@~@_A`Ac@f@i@n@g@j@a@d@]`@Y`@QVOTQXOXOXQ\\O\\Yp@Sn@Sj@Qn@Oj@Kj@Kb@Id@Ij@Gj@Ir@En@El@Ex@Ad@CdAAbAAfBAn@E|Gi@|o@SzXCvBA`CGfHE~DAjAAt@Cn@E~@MpBEp@G|@In@Gj@Ir@Il@[pBQz@Q`AWbAYjA[fAQh@Sh@_@`Ai@vAm@vAa@|@Yl@Yd@cAjBc@t@aJ`PyAhC{@xAaBvCW`@[l@oDjGeDzFaGhKgCnEqBlD{@xA}H`NaBtC'}, 'duration': {'text': '7 mins', 'value': 408}, 'travel_mode': 'DRIVING', 'html_instructions': 'Merge onto <b>US-101 N</b> via the ramp to <b>San Francisco</b>', 'end_location': {'lat': 37.6000361, 'lng': -122.3763451}, 'start_location': {'lat': 37.54478460000001, 'lng': -122.2874522}}, {'distance': {'text': '0.3 mi', 'value': 495}, 'polyline': {'points': 'gwndFds|iVK@I@EDEFqAlBw@jA]f@c@r@S^m@hA[d@UZKNIHKHKFMDMBK@I?K?GCKAMI}@m@UOGCGAGAGAK?C?G@G@E@MHWR'}, 'maneuver': 'ramp-right', 'travel_mode': 'DRIVING', 'html_instructions': 'Take exit <b>421</b> for <b>Millbrae Ave</b>', 'duration': {'text': '1 min', 'value': 38}, 'end_location': {'lat': 37.6034845, 'lng': -122.378707}, 'start_location': {'lat': 37.6000361, 'lng': -122.3763451}}, {'distance': {'text': '0.4 mi', 'value': 580}, 'polyline': {'points': 'wlodF|a}iVKJ`BjDh@bAVd@P`@Vf@Rd@l@nAjAnCz@jBdAxBvBpE'}, 'maneuver': 'turn-left', 'travel_mode': 'DRIVING', 'html_instructions': 'Turn <b>left</b> onto <b>E Millbrae Ave</b>', 'duration': {'text': '1 min', 'value': 85}, 'end_location': {'lat': 37.6005514, 'lng': -122.3840443}, 'start_location': {'lat': 37.6034845, 'lng': -122.378707}}, {'distance': {'text': '249 ft', 'value': 76}, 'polyline': {'points': 'mzndFfc~iVsB~A'}, 'maneuver': 'turn-right', 'travel_mode': 'DRIVING', 'html_instructions': 'Turn <b>right</b> onto <b>Rollins Rd</b>', 'duration': {'text': '1 min', 'value': 21}, 'end_location': {'lat': 37.6011264, 'lng': -122.3845203}, 'start_location': {'lat': 37.6005514, 'lng': -122.3840443}}, {'distance': {'text': '0.1 mi', 'value': 197}, 'polyline': {'points': 'a~ndFff~iVNV|A|C~@nBDFHP@BDHLV'}, 'maneuver': 'turn-left', 'travel_mode': 'DRIVING', 'html_instructions': 'Turn <b>left</b> at the 1st cross street onto <b>Camino Millennia</b>', 'duration': {'text': '1 min', 'value': 80}, 'end_location': {'lat': 37.600067, 'lng': -122.3863147}, 'start_location': {'lat': 37.6011264, 'lng': -122.3845203}}], 'end_location': {'lat': 37.600067, 'lng': -122.3863147}, 'traffic_speed_entry': [], 'end_address': 'Camino Millennia, Millbrae, CA 94030, USA', 'duration_in_traffic': {'text': '15 mins', 'value': 880}, 'duration': {'text': '14 mins', 'value': 812}, 'start_address': '3259 Pacific Blvd, San Mateo, CA 94403, USA', 'distance': {'text': '8.3 mi', 'value': 13435}, 'start_location': {'lat': 37.53796639999999, 'lng': -122.2971724}, 'via_waypoint': []}], 'bounds': {'southwest': {'lat': 37.5370749, 'lng': -122.3863147}, 'northeast': {'lat': 37.6035433, 'lng': -122.286424}}, 'waypoint_order': [], 'overview_polyline': {'points': 'isbdFhdmiVzBqBv@q@[s@QYQIUAMBUNWPOBSG[i@QGwF_KeFgJm@cAIYk@}@s@g@oAy@}Aw@w@a@o@c@iA}@s@w@k@w@}@yAgBcDw@aBMWM_ACm@@QJ]P_@PIREb@FTTJZB\\CPIVQVkFhG]z@cB|AyFfFmJnIwWxU{JlJwI|HoLhKeEvDcEdEkCxCq[`^aXdZme@xh@kn@nr@uDdE_AfAk@x@aAbBa@z@m@`Be@zA[vAg@`DObBKfBIvGyAhkBC`CInBSbDQlBQ~Ae@~Cc@|Bq@nCm@pBs@jBwAnD{@jBcNhVoG|KsR~\\sRx\\aBtCK@OFmDhFaCbEw@~@YLYDU?SEiBkAc@EYDe@\\KJ`BjD`AhBP`@dDlH`CdFvBpEsB~AlBtDdAvB^v@'}}]    
    else:
        directions_result = 0
    duration_in_traffic = (directions_result[0]['legs'][0]
        ['duration_in_traffic']['value'])
    return (duration_in_traffic, directions_result)



def periodic_dummy_function(tstArg0, tstArg1):
    """
    This program is a dummy function to test periodic functions.
   
    :param: tstArg0: string that is appended to beginning of the printed
        statement 
    :type: tstArg0: string
    
    :param: tstArg1: string that is appended to end of the printed statement 
    :type: tstArg1: string
    
    return: None
    """
    date_str = dt.datetime.now().isoformat()
    print(tstArg0 + date_str + tstArg1)
   
   
def ordered_unique_list(seq):
    """
    Reduces a list to unique values only while maintaining order.
    
    :param: seq: list that will be reduced to unique values only
    :type: seq: list
    
    return: list
    """
    seen = set()
    return [x for x in seq if x not in seen and not seen.add(x)]
