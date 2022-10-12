"""
Description: This program is to do some of the error handling: 
1. Sends a push notification if the program restarts
2. Every night compares the expected number of results to the actual number of
results. Sends a push notification with the results from this check.

@author: Robert Hennessy (robertghennessy@gmail.com)
"""
import datetime as dt
import pandas as pd

import config
import sql_functions as sf
import push_notification as pn


def nightly_check(sql_db_loc, table_name, expected_num):
    """
    Compare the amount of data stored in the the previous day to desired 
    amount. Send push notification with the result
    
    :param sql_db_loc: location of the database file
    :type sql_db_loc: string
        
    :param expected_num: number of expected rows
    :type expected_num: int

    :return None
    """
    utc_time_prev = (dt.datetime.utcnow() + dt.timedelta(days=-1)) 
    utc_time_prev = utc_time_prev.timestamp()
    # determine if a push notification occurred during the desired time period
    # defined by delay_between_push
    rows_query = ('select count(*) from %s where utc_time > %f ' %
                  (table_name, utc_time_prev))
    num_rows = sf.query_data(sql_db_loc, rows_query)[0][0]
    
    if num_rows != expected_num:
        title_str = 'Error: Daily Count: %s' % table_name
        body_str = 'Desired Count = %d, Actual Count = %d' % (
            expected_num, num_rows)
    else:
        title_str = 'Pass: Daily Count: %s' % table_name
        body_str = 'Desired Count = %d, Actual Count = %d' % (
            expected_num, num_rows)
    pn.send_push_notification(title_str, body_str) 


def des_num_traffic_meas(csv_path, day_of_week):
    """
    Determine the number of measurements that should occur during a given day
    of the week
    
    :param csv_path: location of the csv file
    :type csv_path: string
        
    :param day_of_week: day of the week
    :type day_of_week: int

    :return None
    """
    df = pd.read_csv(csv_path, index_col=0)
    trips_today = df[df[config.weekday_names[day_of_week]] == 1]
    return trips_today.shape[0]


def des_periodic_meas(csv_path, day_of_week):
    """
    Determine the number of measurements that should occur during a given day
    of the week
    
    :param csv_path: location of the csv file
    :type csv_path: string
        
    :param day_of_week: day of the week
    :type day_of_week: int

    :return None
    """
    df = pd.read_csv(csv_path, index_col=0)
    trips_today = df[df['day_code'].str.contains(
                        config.day_of_week_codes[day_of_week])]
    return trips_today.shape[0]


def nightly_check_periodic(sql_db_loc, table_name, column_name, 
                           time_column_name, expected_num, task_name):
    """
    Compare the amount of data stored in the the previous day to desired 
    amount. Send push notification with the result
    
    :param sql_db_loc: location of the database file
    :type sql_db_loc: string
    
    :param table_name: name of table where the data is stored
    :type table_name: string
    
    :param column_name: name of the column to count to compare with the
        expected number
    :type column_name: string
    
    :param time_column_name: name of the column that contains the time for
        each row
    :type time_column_name: string
        
    :param expected_num: number of expected rows
    :type expected_num: int
    
    :param task_name: name of the task. This is sent in the push notification.
    :type task_name: string

    :return None
    """
    utc_time_prev = (dt.datetime.utcnow() + dt.timedelta(days=-1)) 
    utc_time_prev = utc_time_prev.timestamp()
    # determine if a push notification occurred during the desired time period
    # defined by delay_between_push
    rows_query = ('select count(distinct %s) from %s where %s > %f ' %
                  (column_name, table_name, time_column_name, utc_time_prev))
    num_rows = sf.query_data(sql_db_loc, rows_query)[0][0]
    if num_rows != expected_num:
        title_str = 'Error: Daily Count: %s' % task_name
        body_str = 'Desired Count = %d, Actual Count = %d' % (
            expected_num, num_rows)
    else:
        title_str = 'Pass: Daily Count: %s' % task_name
        body_str = 'Desired Count = %d, Actual Count = %d' % (
            expected_num, num_rows)
    pn.send_push_notification(title_str, body_str)    


def main():   
    day_of_week = (dt.datetime.today()+dt.timedelta(days=-1)).weekday()
    desired_number_of_traffic_measurements = des_num_traffic_meas(
                                        config.trips_csv, day_of_week)
    nightly_check(config.traffic_data_sql, 'traffic_data',
                  desired_number_of_traffic_measurements)
    desired_number_of_periodic_measurements = des_periodic_meas(
        config.periodic_jobs_csv, day_of_week)
    nightly_check_periodic(config.siri_data_sql, 'periodic_task_monitor',
                           'time_index', 'utc_time',
                           desired_number_of_periodic_measurements,
                           'siri Data')
    nightly_check_periodic(config.gtfs_rt_data_sql, 'periodic_task_monitor',
                           'time_index', 'utc_time',
                           desired_number_of_periodic_measurements,
                           'gtfs-rt Data')


if __name__ == '__main__':
    main()
