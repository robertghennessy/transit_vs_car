"""
Description: This file contains all sql_functions. This includes creating a 
    connection, creating tables, inserting data, updating entries and so on.

@author: Robert Hennessy (robertghennessy@gmail.com)
"""
import sqlite3
import datetime as dt

def create_connection(db_file, timeout=120, isolation_level=None):
    """
    Create a database connection to the SQLite database specified by db_file
    
    :param: db_file: database file location
    :type: db_file: string
    
    :return: Connection object or None
    """
    conn = sqlite3.connect(db_file, timeout=timeout, 
                           isolation_level=isolation_level)
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
 

def create_periodic_task_monitor_table(db_location): 
    """
    Create a periodic task monitor table

    :param: db_location: location of the database file
    :type: db_location: string  

    :return: None
    """
    # create a table
    sql = """CREATE TABLE periodic_task_monitor
                      (date text, time text, utc_time real, 
                      day_of_week integer, time_index int) 
                   """
    create_table(db_location,sql)
    return None
    
def insert_periodic_task_monitor(db_location, time_index):
    """
    Insert the process monitor data into the database
    
    :param: db_location: location of the database file
    :type: db_location: string  
    
    :param: time_index: the index for the periodic call
    :type: time_index: integer    
    
    :return: None
    """
    # create the time objects to save the results
    date_str = dt.datetime.now().date().isoformat()
    time_str = dt.datetime.now().time().isoformat()
    day_of_week = dt.datetime.now().isoweekday()
    utc_time_now = dt.datetime.utcnow().timestamp()
    task_monitor_data = (str(date_str), str(time_str), float(utc_time_now), 
                  int(day_of_week),int(time_index))    
    sql = """ INSERT INTO periodic_task_monitor(date, time, utc_time, 
                                    day_of_week, time_index)
              VALUES(?,?,?,?,?) """
    insert_data(db_location, sql, task_monitor_data)
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
                      push_name text) 
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
 
    sql = """ INSERT INTO push_monitor(date, time, utc_time, day_of_week, 
                                    push_notify, push_name) 
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
    Insert the results into the database
    
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
    
def create_transit_data_siri_table(name, db_location): 
    """
    Create the siri transit data table

    :param: name: name of the table
    :type: string

    :param: db_location: location of the database file
    :type: db_location: string  

    :return: None
    """
    # create a table
    sql = """CREATE TABLE %s
                      (TrainStartDate text,
                      trip_id text, 
                      stop_id integer,
                      time_index int, 
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
                   """ % name
    create_table(db_location,sql)
    return None


def create_transit_data_gtfs_rt_table(name, db_location): 
    """
    Create the transit data table
    
    :param: name: the name of the table
    :type: string

    :param: db_location: location of the database file
    :type: db_location: string  

    :return: None
    """
    # create a table
    sql = """CREATE TABLE %s
                      (TrainStartDate text,
                      time_index int, 
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
                   """ % name
                   
    create_table(db_location,sql)
    return None


def where_statement_common_entries(table_modified, table_base, 
                                   columns_to_compare):
    """
    Construct a sql where statement to compare rows in two different tables.
     
    :param: table_modified: the name of the table to be modified
    :type: string
    
    :param: table_base: the name of the base table 
    :type: string
    
    :param: columns_to_compare: a list of the columns to compare
    :type: list
    
    :return: where_statement: where statement to find common entries
    :type: where_statemet: string
    """
    where_statement = ""
    numb_of_compare = len(columns_to_compare)
    for cInd in range(numb_of_compare):
        new_where = '%s.%s = %s.%s' % (table_modified, 
                                       columns_to_compare[cInd], 
                                       table_base,
                                       columns_to_compare[cInd])
        where_statement = where_statement + new_where
        if cInd <= numb_of_compare - 2:
            where_statement = where_statement + ' AND '
    return where_statement
    

def delete_entries_in_common(table_modified, table_base, 
                          columns_to_compare):
    """
    Construct sql_query to delete common entries
    
    :param: table_modified: the name of the table to be modified
    :type: string
    
    :param: table_base: the name of the base table 
    :type: string
    
    :param: columns_to_compare: a list of the columns to compare
    :type: list
    
    :return: sql_query: query used to delete rows in common
    :type: sql_query: string
    """

    # Construct the where statement
    where_statement = where_statement_common_entries(table_modified, 
                                                     table_base,
                                                     columns_to_compare)
    # construct the sql_query
    sql_query = 'delete from %s where exists (select * from %s where %s)' % (
                    table_modified, table_base, where_statement)
    return sql_query


def copy_new_entries(table_modified, table_base, columns_to_compare):
    """
    Construct sql_query to copy new entries
    
    :param: table_modified: the name of the table to be modified
    :type: string
    
    :param: table_base: the name of the base table 
    :type: string
    
    :param: columns_to_compare: a list of the columns to compare
    :type: list
    
    :return: sql_query: query used to new entries in common
    :type: sql_query: string
    """

    # Construct the where statement
    where_statement = where_statement_common_entries(table_modified, 
                                                     table_base,
                                                     columns_to_compare)
    # construct the sql_query
    sql_query = ('Insert into %s select * from %s where not exists (select * '
                    'from %s where %s)') % (table_modified, table_base, 
                    table_modified, where_statement)
    return sql_query


def sql_delete_table(table_name):
    """
    Construct sql statement to delete a table
    
    :param: table_name: the name of the table to be deleted
    :type: table_name: string
    
    :return: sql_query: the sql query to delete a table
    :type: sql_query: string
    """
    sql_query = 'drop table %s' % table_name
    return sql_query


def sql_delete_table_if_exists(table_name):
    """
    Construct sql statement to delete a table if it exists
    
    :param: table_name: the name of the table to be deleted
    :type: table_name: string
    
    :return: sql_query: the sql query to delete a table
    :type: sql_query: string
    """
    sql_query = 'drop table if exists %s' % table_name
    return sql_query


def update_entries(db_location, table_modified, data, columns_to_compare):
    """
    Create a table in the database
    
    :param: db_location: location of the database file
    :type: db_location: string
     
    :param: table_modified: name of the string to update
    :type: table_modified: string   
    
    :param: data: pandas dataframe that contains the new data
    :type: data: pandas dataframe
    
    :param: columns_to_compare: a list of the columns to compare to determine
        if an entry needs to be updated
    :type: list
    
    
    :return: None
    """ 
    temp_table_name = table_modified + '_temp'
    conn = None
    try:
        # create an exclusive connection, no other process can read/write
        conn = create_connection(db_location, isolation_level='EXCLUSIVE')
        cursor = conn.cursor()
        # copy the data to a temporary table
        data.to_sql(temp_table_name, conn, index=False, if_exists='replace')
        # delete entries in the original table that will be replaced
        sql_cmd = delete_entries_in_common(table_modified, temp_table_name, 
                                           columns_to_compare)
        cursor.execute(sql_cmd)
        # copy over the rows from the temp table
        sql_cmd = copy_new_entries(table_modified, temp_table_name, 
                                           columns_to_compare)
        cursor.execute(sql_cmd)
        # delete temp table
        sql_cmd = sql_delete_table(temp_table_name)
        cursor.execute(sql_cmd)
        # commit changes        
        conn.commit()
    finally:
        if conn:
            conn.close()  

            
def create_table_def_string(input_dict):
    """
    This creates a string that will be used for sql table definition.
    
    :param: input_dict: a dictionary that contains the sql_name and the 
        sql_type
    :type: input_dict: dictionary
    
    :return: ret_str:  
    """
    key_list = list(input_dict.keys())
    ret_str = ''
    for kInd in range(len(key_list)):
        key = key_list[kInd]
        ret_str = ret_str + '%s %s' % (input_dict[key]['sql_name'],
                                       input_dict[key]['sql_type'])
        if kInd < len(key_list) - 1:
                ret_str = ret_str + ', '
    return ret_str


def create_table_from_dict(db_location, table_name, table_dict):
    """
    This creates a table from the table_dict
    
    :param: db_location: location of the database file
    :type: db_location: string  
    
    :param: table_name: the name of the table
    :type: string
    
    :param: table_dict: a dictionary that contains the table definition
        information
    :type: table_dict: dictionary
    
    :return: None
    """
    table_def_string = create_table_def_string(table_dict)
    sql = """CREATE TABLE %s (%s)""" % (table_name, table_def_string)
    create_table(db_location,sql)
    return None


def prepare_pandas_to_sql(df, table_dict):
    """
    Prepare the pandas dataframe to upload into dataframe. Orders the columns
    so that they are the same order of the sql database and fill missing
    columns with NaNs.
    
    :param: df: pandas dataframe that will be modified
    :type: df: pandas dataframe
    
    :param: table_dict: dictionary that containts the table definition
    :type: table_dict: dictionary
    
    return: out_df: output pandas dataframe
    :type: out_df: pandas dataframe
    """
    
    # creates the list to reindex the dataframe
    ordered_list = []
    for key in range(len(table_dict)):
        ordered_list.append(table_dict[key]['pandas_name'])
    # changes the order of the index, by default empty values = NaN
    out_df = df.reindex(columns=ordered_list)
    return out_df

