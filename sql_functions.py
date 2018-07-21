# -*- coding: utf-8 -*-
"""
This file contains the sql functions
- create a connection
- create trip_data table
- insert trip data into the table

@author: Robert Hennessy (rghennessy@gmail.com)
"""
import sqlite3

def create_trip_data_table(db_location): 
    """
    Create the trip data table
    : param db_location: location of the database file
    : return: None
    """
    conn = create_connection(db_location)
    cursor = conn.cursor()
    # create a table
    cursor.execute("""CREATE TABLE trip_data
                      (date text, time text, day_of_week integer, 
                       trip_index integer, trip_id integer, 
                       start_station text, end_station text, start_loc text,
                       end_loc text, directions_result text, 
                       duration_in_traffic real) 
                   """)
    conn.close()
    return None

def create_connection(db_file):
    """
    Create a database connection to the SQLite database specified by db_file
    :param db_file: database file
    :return: Connection object or None
    from: http://www.sqlitetutorial.net/sqlite-python/insert/
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception as e:
        print(e)
    return None

def insert_trip_data(conn, data):
    """
    Create a new task
    :param conn: connection to the database
    :param data: data tuple to be inserted into the database
    :return: returns the autoincrement value for the new row
    """
 
    sql = ''' INSERT INTO trip_data(date, time, day_of_week, trip_index, trip_id, 
                               start_station, 
                               end_station, start_loc, end_loc, 
                               directions_result, duration_in_traffic) 
              VALUES(?,?,?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, data)
    lastrowid = cur.lastrowid
    return lastrowid