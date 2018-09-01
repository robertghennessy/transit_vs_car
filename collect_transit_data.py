"""


@author: Robert Hennessy (robertghennessy@gmail.com)
"""

import json
import datetime as dt
import dateutil.parser as dp
from dateutil import tz
import sql_functions as sf
from sql_functions import create_table
import config
import os
from google.transit import gtfs_realtime_pb2
import requests
from google.protobuf.json_format import MessageToDict

to_zone = tz.gettz('America/San_Francisco')
from_zone = tz.gettz('UTC')
agency = 'CT'
gtfs_rt_api = 'http://api.511.org/Transit/TripUpdates?api_key='
siri_api = 'http://api.511.org/Transit/StopMonitoring?api_key='

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
    
def query_siri():
    """
    Query the 511 api to collect stop monitoring information. Convert the json
        to a dict
    
    :param: None
    
    :return: dictionary with the stop monitoring information
    """  
    url = (siri_api + config.transit_511_api_key + '&agency=' + agency + 
        '&Format=JSON')
    json_url = requests.get(url)
    data = json.loads(json_url.content.decode('utf-8-sig'))
    return data




def parse_transit_data_siri(transit_data, db_location):
    """
    

    """    
    MonitoredStops = (transit_data['ServiceDelivery']['StopMonitoringDelivery']
                                ['MonitoredStopVisit'])
    conn = None
    try:
        conn = sf.create_connection(db_location, timeout=10)
        cursor = conn.cursor()
        for sInd in range(0,len(MonitoredStops)):
            RecordedAtTime = MonitoredStops[sInd]['RecordedAtTime']
            RecordedAtTime_dt = dp.parse(RecordedAtTime).astimezone(to_zone)
            RecordedAtTime_utc = dp.parse(RecordedAtTime).timestamp()
            RecordedAtTime_date = RecordedAtTime_dt.strftime('%Y-%m-%d')
            RecordedAtTime_time = RecordedAtTime_dt.strftime('%H:%M:%S')
            
            MonitoredVehicleJourney = MonitoredStops[sInd]['MonitoredVehicleJourney']
            train_id = (MonitoredVehicleJourney['FramedVehicleJourneyRef']
                            ['DatedVehicleJourneyRef'])
            MonitoredCall = MonitoredVehicleJourney['MonitoredCall']
            StationName = MonitoredCall['StopPointName']
            StationID = MonitoredCall['StopPointRef']
            VehicleAtStop = MonitoredCall['VehicleAtStop']
            # arrival time
            AimedArrivalTime = MonitoredCall['AimedArrivalTime']
            AimedArrivalTime_dt = dp.parse(AimedArrivalTime).astimezone(to_zone)
            AimedArrivalTime_utc = dp.parse(RecordedAtTime).timestamp()
            AimedArrivalTime_date = AimedArrivalTime_dt.strftime('%Y-%m-%d')
            AimedArrivalTime_time = AimedArrivalTime_dt.strftime('%H:%M:%S')
            #deperature time
            AimedDepartureTime = MonitoredCall['AimedDepartureTime']
            AimedDepartureTime_dt = dp.parse(AimedDepartureTime).astimezone(to_zone)
            AimedDepartureTime_utc = dp.parse(RecordedAtTime).timestamp()
            AimedDepartureTime_date = AimedDepartureTime_dt.strftime('%Y-%m-%d')
            AimedDepartureTime_time = AimedDepartureTime_dt.strftime('%H:%M:%S')
            # construct sql command to inject data into database
            sql_cmd = """ INSERT INTO transit_data_siri(RecordedAtTime_date, 
                            RecordedAtTime_time, RecordedAtTime_utc, StationName, 
                            StationID, train_id, VehicleAtStop, AimedArrivalTime_date, 
                            AimedArrivalTime_time, AimedArrivalTime_utc, 
                            AimedDepartureTime_date, AimedDepartureTime_time, 
                            AimedDepartureTime_utc) 
                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) """
            data = (RecordedAtTime_date, RecordedAtTime_time, RecordedAtTime_utc, 
                     StationName, StationID, train_id, VehicleAtStop, AimedArrivalTime_date, 
                     AimedArrivalTime_time, AimedArrivalTime_utc, 
                     AimedDepartureTime_date, AimedDepartureTime_time, 
                     AimedDepartureTime_utc) 
            cursor.execute(sql_cmd,data)      
        # commit the changes to the databse
        conn.commit()
    finally:
        if conn:
            conn.close()  


def parse_transit_data_gtfs_rt(transit_data, db_location):
    conn = None
    try:
        conn = sf.create_connection(db_location, timeout=10)
        cursor = conn.cursor()
             
        RecordedAtTime = transit_data['header']['timestamp']
        RecordedAtTime_dt = dt.datetime.utcfromtimestamp(float(RecordedAtTime))
        RecordedAtTime_dt = RecordedAtTime_dt.replace(tzinfo=from_zone)
        RecordedAtTime_dt= RecordedAtTime_dt.astimezone(to_zone)
        RecordedAtTime_utc = RecordedAtTime_dt.timestamp()
        RecordedAtTime_date = RecordedAtTime_dt.strftime('%Y-%m-%d')
        RecordedAtTime_time = RecordedAtTime_dt.strftime('%H:%M:%S')                       
        
        entities = transit_data['entity']     
        
        for eInd in range(0,len(entities)):
            entity = entities[eInd]
            train_id = entity['tripUpdate']['trip']['tripId']
            stopTimeUpdate = entity['tripUpdate']['stopTimeUpdate']
            for sInd in range(0,len(stopTimeUpdate)):
                StationID = stopTimeUpdate[sInd]['stopId']
                AimedDepartureTime = stopTimeUpdate[0]['departure']['time']
                AimedDepartureTime_dt = dt.datetime.utcfromtimestamp(float(AimedDepartureTime))
                AimedDepartureTime_dt = AimedDepartureTime_dt.replace(tzinfo=from_zone)
                AimedDepartureTime_dt= AimedDepartureTime_dt.astimezone(to_zone)                
                AimedDepartureTime_utc = AimedDepartureTime_dt.timestamp()
                AimedDepartureTime_date = AimedDepartureTime_dt.strftime('%Y-%m-%d')
                AimedDepartureTime_time = AimedDepartureTime_dt.strftime('%H:%M:%S')
                sql_cmd = """ INSERT INTO transit_data_gtfs_rt(RecordedAtTime_date, 
                            RecordedAtTime_time, RecordedAtTime_utc, 
                            StationID, train_id, AimedDepartureTime_date, 
                            AimedDepartureTime_time, 
                            AimedDepartureTime_utc) 
                            VALUES(?,?,?,?,?,?,?,?) """
                data = (RecordedAtTime_date, RecordedAtTime_time, RecordedAtTime_utc, 
                         StationID, train_id, AimedDepartureTime_date, AimedDepartureTime_time, 
                         AimedDepartureTime_utc) 
                cursor.execute(sql_cmd,data) 
                
        conn.commit()
    finally:
        if conn:
            conn.close()  

    

def create_transit_data_siri_table(db_location): 
    """
    Create the transit data table

    :param: db_location: location of the database file
    :type: db_location: string  

    :return: None
    """
    # create a table
    sql = """CREATE TABLE transit_data_siri
                      (RecordedAtTime_date text, RecordedAtTime_time text, 
                      RecordedAtTime_utc real, StationName text, 
                      StationID text, train_id text, VehicleAtStop text, 
                      AimedArrivalTime_date text, AimedArrivalTime_time text, 
                      AimedArrivalTime_utc real, AimedDepartureTime_date text, 
                      AimedDepartureTime_time text, AimedDepartureTime_utc real
                      ) 
                   """
    create_table(db_location,sql)
    return None


def create_transit_data_gtfs_rt_table(db_location): 
    """
    Create the transit data table

    :param: db_location: location of the database file
    :type: db_location: string  

    :return: None
    """
    # create a table
    sql = """CREATE TABLE transit_data_gtfs_rt
                      (RecordedAtTime_date text, RecordedAtTime_time text, 
                      RecordedAtTime_utc real, 
                      StationID text, train_id text, 
                      AimedDepartureTime_date text, 
                      AimedDepartureTime_time text, AimedDepartureTime_utc real
                      ) 
                   """
    create_table(db_location,sql)
    return None

def main():
    if os.path.isfile(config.test_data_db):
        os.remove(config.test_data_db)
    else:    # Show an error #
        print("Error: %s file not found" % config.test_data_db)
    create_transit_data_siri_table(config.test_data_db)
    create_transit_data_gtfs_rt_table(config.test_data_db)
    transit_data_siri = query_siri()
    parse_transit_data_siri(transit_data_siri,config.test_data_db)
    transit_data_gtfs_rt = query_gtfs_rt()
    parse_transit_data_gtfs_rt(transit_data_gtfs_rt, config.test_data_db)

if __name__ == '__main__':
    main()