"""
Description: This program is to do some of the error handling: 
1. Sends a pushnotification if the program restarts
2. Every night compares the expected number of results to the actual number of
results. Sends a push notification with the results from this check.

@author: Robert Hennessy (robertghennessy@gmail.com)
"""
import datetime as dt
import pandas as pd

import config
import sql_functions as sf
import push_notification as pn

week_names_sched_trips = ['monday', 'tuesday', 'wednesday', 'thursday', 
                          'friday', 'saturday', 'sunday']
day_of_week_codes = ['mon','tue','wed','thu','fri','sat','sun']

def nightly_check(sql_db_loc, table_name, expected_num):
    """
    Compare the amount of data stored in the the previous day to desired 
    amount. Send push notification with the result
    
    :param: sql_db_loc: location of the database file
    :type: sql_db_loc: string  
        
    :param: expected_num: number of expected rows
    :type: expected_num: int

    : return: None
    """
    utc_time_prev = (dt.datetime.utcnow() + dt.timedelta(days=-1)) 
    utc_time_prev = utc_time_prev.timestamp()
    # determine if a push notification occurred during the desired time period
    # defined by delay_between_push
    rowsQuery = ('select count(*) from %s where utc_time > %f ' 
                    % (table_name, utc_time_prev))
    num_rows = sf.query_data(sql_db_loc,rowsQuery)[0][0]
    
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
    
    :param: csv_path: location of the csv file
    :type: csv_path: string  
        
    :param: day_of_week: day of the week
    :type: day_of_week: int

    : return: None
    """
    df = pd.read_csv(csv_path, index_col=0)
    trips_today = df[df[week_names_sched_trips[day_of_week]] == 1]
    return trips_today.shape[0]
    
def des_periodic_meas(csv_path, day_of_week):
    """
    Determine the number of measurements that should occur during a given day
    of the week
    
    :param: csv_path: location of the csv file
    :type: csv_path: string  
        
    :param: day_of_week: day of the week
    :type: day_of_week: int

    : return: None
    """
    df = pd.read_csv(csv_path, index_col=0)
    trips_today = df[df['day_code'].str.contains(
                        day_of_week_codes[day_of_week])]
    return trips_today.shape[0]
       
def nightly_check_periodic(sql_db_loc, table_name, column_name, 
                           time_column_name, expected_num):
    """
    Compare the amount of data stored in the the previous day to desired 
    amount. Send push notification with the result
    
    :param: sql_db_loc: location of the database file
    :type: sql_db_loc: string  
        
    :param: expected_num: number of expected rows
    :type: expected_num: int

    : return: None
    """
    utc_time_prev = (dt.datetime.utcnow() + dt.timedelta(days=-1)) 
    utc_time_prev = utc_time_prev.timestamp()
    # determine if a push notification occurred during the desired time period
    # defined by delay_between_push
    rowsQuery = ('select count(distinct %s) from %s where %s > %f ' 
                    % (column_name, table_name, time_column_name,
                       utc_time_prev))
    num_rows = sf.query_data(sql_db_loc,rowsQuery)[0][0]
    if num_rows != expected_num:
        title_str = 'Error: Daily Count: %s' % table_name
        body_str = 'Desired Count = %d, Actual Count = %d' % (
            expected_num, num_rows)
    else:
        title_str = 'Pass: Daily Count: %s' % table_name
        body_str = 'Desired Count = %d, Actual Count = %d' % (
            expected_num, num_rows)
    pn.send_push_notification(title_str, body_str)    
       
def main():   
    day_of_week = (dt.datetime.today()+dt.timedelta(days=-1)).weekday()
    desired_number_of_traffic_measurements = des_num_traffic_meas(
                                        config.trips_csv, day_of_week)
#    nightly_check(config.traffic_data_sql, 'traffic_data',
#                  desired_number_of_traffic_measurements)
    desired_number_of_periodic_measurements = des_periodic_meas(
        config.periodic_jobs_csv,day_of_week)
    nightly_check_periodic(config.transit_data_sql, 'transit_data_siri',
                  'time_index', 'RecordedAtTime_utc',
                  desired_number_of_periodic_measurements)
    nightly_check_periodic(config.transit_data_sql, 'transit_data_gtfs_rt',
                  'time_index', 'RecordedAtTime_utc',
                  desired_number_of_periodic_measurements)

if __name__ == '__main__':
    main()
