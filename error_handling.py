# -*- coding: utf-8 -*-
"""
@author: Robert Hennessy (rghennessy@gmail.com)

Description: This program is to do some of the error handling 

"""
import datetime as dt
import config
import sql_functions as sf
import pushover
import pandas as pd

week_names_sched_trips = ['monday', 'tuesday', 'wednesday', 'thursday', 
                          'friday', 'saturday', 'sunday']

def restart_push_notify(sql_db_loc, log_name):
    '''
    Record that a restart has occurred and do a push notification    
    
    :param: sql_db_loc: location of the database file
    :type: sql_db_loc: string  
        
    :param: log_name: name of the log file associated with the restart
    :type: log_name: string

    : return: None
    '''
    # delay between the push notifications in seconds
    delay_between_push = 60*60
    # create the time and date objects
    date_str = dt.datetime.now().date().isoformat()
    time_str = dt.datetime.now().time().isoformat()
    day_of_week = dt.datetime.now().isoweekday()
    utc_time_now = dt.datetime.utcnow().timestamp()
    utc_time_prev = (dt.datetime.now() + 
                     dt.timedelta(seconds=-delay_between_push)) 
    utc_time_prev = utc_time_prev.timestamp()
    # determine if a push notification occurred during the desired time period
    # defined by delay_between_push
    rowsQuery = ('select count(*) from process_monitor where utc_time > %f ' 
                    'and push_notify = 1' % utc_time_prev)            
    num_rows = sf.query_data(sql_db_loc,rowsQuery)[0][0]
    if num_rows == 0:
        push_notify = 1
        title_str = 'Car vs Caltrain Restarted'
        body_str = 'time = %s' % time_str
        send_push_notification(title_str, body_str)
    else:
        push_notify = 0
    # create the tuple that is inserted into the database. Ensure that all
    # parameters are the right data type
    data_tuple = (str(date_str), str(time_str), float(utc_time_now), 
                  int(day_of_week),int(push_notify), str(log_name))    
    sf.insert_process_monitor(sql_db_loc,data_tuple)
    

def send_push_notification(title_str, body_str):
    '''
    Send a push notification to the phone
    
    :param: title_str: title of the push notification
    :type: title_str: string  
        
    :param: body_str: body of the push notification
    :type: body_str: string

    : return: None
    '''
    # send a push notification to phone when the program has restarted
    client = pushover.Client(config.pushover_user_key, 
                             api_token=config.pushover_api_key)
    client.send_message(body_str, title=title_str)

def nightly_check(sql_db_loc,expected_num):
    '''
    Compare the amount of data stored in the the previous day to desired 
    amount. Send push notification with the result
    
    :param: sql_db_loc: location of the database file
    :type: sql_db_loc: string  
        
    :param: expected_num: number of expected rows
    :type: expected_num: int

    : return: None
    '''
    utc_time_prev = (dt.datetime.now() + dt.timedelta(days=-1)) 
    utc_time_prev = utc_time_prev.timestamp()
    # determine if a push notification occurred during the desired time period
    # defined by delay_between_push
    rowsQuery = ('select count(*) from process_monitor where utc_time > %f ' 
                    % utc_time_prev)   
    num_rows = sf.query_data(sql_db_loc,rowsQuery)[0][0]
    
    if num_rows != expected_num:
        title_str = 'Error: Daily Count'
        body_str = 'Desired Count = %d, Actual Count = %d' % (
            expected_num, num_rows)
    else:
        title_str = "Pass: Daily Count"
        body_str = 'Desired Count = %d, Actual Count = %d' % (
            expected_num, num_rows)
    send_push_notification(title_str, body_str) 


def des_num_meas(csv_path, day_of_week):
    '''
    Determine the number of measurements that should occur during a given day
    of the week
    
    :param: csv_path: location of the csv file
    :type: csv_path: string  
        
    :param: day_of_week: day of the week
    :type: day_of_week: int

    : return: None
    '''
    df = pd.read_csv(csv_path, index_col=0)
    trips_today = df[df[week_names_sched_trips[day_of_week]] == 1]
    return trips_today.shape[0]
       
def main():   
    day_of_week = (dt.datetime.today()+dt.timedelta(days=-1)).weekday()
    desired_number_of_measurements = des_num_meas(config.trips_csv_path_out,
                                                  day_of_week)
    nightly_check(config.output_database,desired_number_of_measurements)

if __name__ == '__main__':
    main()

    
    