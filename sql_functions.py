"""
Description: This program creates the sql databases and inserts data into them.

@author: Robert Hennessy (robertghennessy@gmail.com)
"""
import sqlite3

def create_connection(db_file, timeout=120):
    """
    Create a database connection to the SQLite database specified by db_file
    
    :param: db_file: database file location
    :type: db_file: string
    
    :return: Connection object or None
    """
    conn = sqlite3.connect(db_file, timeout=timeout)
    return conn


def create_table(db_location, table_statement):
    """
    Create a table in the database
    
    :param: db_location: location of the database file
    :type: db_location: string
     
    :param: table_statement: statement that used to create the table
    :type: table_statement: string   
    
    :return: None
    """   
    conn = None
    try:
        conn = create_connection(db_location)
        cursor = conn.cursor()
        cursor.execute(table_statement)
    finally:
        if conn:
            conn.close()


def insert_data(db_location,sql_cmd,data):
    """
    Insert data into a table
    
    :param: db_location: location of the database file
    :type: db_location: string    
    
    :param: sql_cmd: sql command to write it into the table
    :type: sql_cmd: string
    
    :param: data: data to be inserted into the table
    :type: data: tuple
    
    :return: None
    """   
    conn = None
    try:
        conn = create_connection(db_location)
        cursor = conn.cursor()
        cursor.execute(sql_cmd,data)
        conn.commit()
    finally:
        if conn:
            conn.close()  
  
def query_data(db_location,sql_cmd):
    """
    Insert data into a table
    
    :param: db_location: location of the database file
    :type: db_location: string    
    
    :param: sql_cmd: sql command to write it into the table
    :type: sql_cmd: string
    
    :return: rows that have returned from the queries
    """   
    conn = None
    try:
        conn = create_connection(db_location)
        cursor = conn.cursor()
        cursor.execute(sql_cmd)
        return cursor.fetchall()
    finally:
        if conn:
            conn.close() 

        
def create_traffic_data_table(db_location): 
    """
    Create the traffic data table

    :param: db_location: location of the database file
    :type: db_location: string  

    :return: None
    """
    # create a table
    sql = """CREATE TABLE traffic_data
                      (date text, time text, utc_time real, 
                       day_of_week integer, trip_index integer, 
                       trip_id integer, start_station text, end_station text, 
                       start_loc text, end_loc text, directions_result text, 
                       duration_in_traffic real) 
                   """
    create_table(db_location,sql)
    return None

def insert_traffic_data(db_location, data):
    """
    Insert the traffic data into the database
    
    :param: db_location: location of the database file
    :type: db_location: string  
    
    :param: data: data tuple to be inserted into the database
    :type: data: tuple    
    
    :return: None
    """
 
    sql = """ INSERT INTO traffic_data(date, time, utc_time, day_of_week, 
                                    trip_index, trip_id, start_station, 
                                    end_station, start_loc, end_loc, 
                                    directions_result, duration_in_traffic) 
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?) """
    insert_data(db_location, sql, data)
    return None
    

def create_proc_monitor_table(db_location): 
    """
    Create the process monitor table

    :param: db_location: location of the database file
    :type: db_location: string  

    :return: None
    """
    # create a table
    sql = """CREATE TABLE process_monitor
                      (date text, time text, utc_time real, 
                      day_of_week integer, push_notify integer, 
                      log_name string) 
                   """
    create_table(db_location,sql)
    return None
    
def insert_process_monitor(db_location, data):
    """
    Insert the process monitor data into the database
    
    :param: db_location: location of the database file
    :type: db_location: string  
    
    :param: data: data tuple to be inserted into the database
    :type: data: tuple    
    
    :return: None
    """
 
    sql = """ INSERT INTO process_monitor(date, time, utc_time, day_of_week, 
                                    push_notify, log_name) 
              VALUES(?,?,?,?,?,?) """
    insert_data(db_location, sql, data)
    return None
 

def create_push_monitor_table(db_location): 
    """
    Create the push monitor table

    :param: db_location: location of the database file
    :type: db_location: string  

    :return: None
    """
    # create a table
    sql = """CREATE TABLE push_monitor
                      (date text, time text, utc_time real, 
                      day_of_week integer, push_notify integer, 
                      function text) 
                   """
    create_table(db_location,sql)
    return None


def insert_push_monitor(db_location, data):
    """
    Insert the push monitor data into the database
    
    :param: db_location: location of the database file
    :type: db_location: string  
    
    :param: data: data tuple to be inserted into the database
    :type: data: tuple    
    
    :return: None
    """
 
    sql = """ INSERT INTO process_monitor(date, time, utc_time, day_of_week, 
                                    push_notify, function) 
              VALUES(?,?,?,?,?,?) """
    insert_data(db_location, sql, data)
    return None    
   

def create_results_table(db_location): 
    """
    Create the results table

    :param: db_location: location of the database file
    :type: db_location: string  

    :return: None
    """
    # create a table
    sql = """CREATE TABLE results
                      (trip_index int, train_id text, start_station text,
                      end_station text, duration_in_traffic_mean float,
                      duration_in_traffic_std float, trip_fract float,
                      take_train int, sched_trip_time float, count int,
                      filename text) 
                   """
    create_table(db_location,sql)
    return None

def insert_results(db_location, data):
    """
    Insert theresults into the database
    
    :param: db_location: location of the database file
    :type: db_location: string  
    
    :param: data: data tuple to be inserted into the database
    :type: data: tuple    
    
    :return: None
    """
 
    sql = """ INSERT INTO results  (trip_index, train_id, start_station,
                      end_station, duration_in_traffic_mean,
                      duration_in_traffic_std, trip_fract,
                      take_train, sched_trip_time, count, filename) 
              VALUES(?,?,?,?,?,?,?,?,?,?,?) """
    insert_data(db_location, sql, data)
    return None

    
def create_transit_data_siri_table(db_location): 
    """
    Create the transit data table

    :param: db_location: location of the database file
    :type: db_location: string  

    :return: None
    """
    # create a table
    sql = """CREATE TABLE transit_data_siri
                      (time_index int, 
                      trip_id text, 
                      stop_id integer, 
                      RecordedAtTime_date text, 
                      RecordedAtTime_time text, 
                      RecordedAtTime_utc real, 
                      StationName text, 
                      short_stop_name text,  
                      VehicleAtStop text, 
                      AimedArrivalTime_date text, 
                      AimedArrivalTime_time text, 
                      AimedArrivalTime_utc real, 
                      AimedArrivalTime_seconds real, 
                      scheduled_arrival_time_seconds real, 
                      ArrivalOnTime int, 
                      ArrivalDelay real, 
                      AimedDepartureTime_date text,
                      AimedDepartureTime_time text, 
                      AimedDepartureTime_utc real,
                      AimedDepartureTime_seconds real, 
                      scheduled_departure_time_seconds real,
                      DeperatureOnTime int, 
                      DeperatureDelay real
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
                      (time_index int, 
                      RecordedAtTime_date text, 
                      RecordedAtTime_time text, 
                      RecordedAtTime_utc real, 
                      stop_id int, 
                      short_stop_name text,
                      trip_id text, 
                      AimedDepartureTime_date text, 
                      AimedDepartureTime_time text, 
                      AimedDepartureTime_utc real,
                      AimedDepartureTime_seconds real,
                      scheduled_arrival_time_seconds real,
                      scheduled_departure_time_seconds real,
                      DeperatureOnTime int, 
                      DeperatureDelay real
                      ) 
                   """
    create_table(db_location,sql)
    return None