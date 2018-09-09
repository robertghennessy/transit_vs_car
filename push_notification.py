"""
Description: This file contains functions to send push notifications

@author: Robert Hennessy (robertghennessy@gmail.com)
"""
import datetime as dt
import config
import sql_functions as sf
import pushover

delay_train_header=['Train', 'Station', 'Delay (min)']
delay_traincol_buf = 2

def send_push_notification(title_str, body_str):
    """
    Send a push notification to the phone
    
    :param: title_str: title of the push notification
    :type: title_str: string  
        
    :param: body_str: body of the push notification
    :type: body_str: string

    : return: None
    """
    # send a push notification to phone when the program has restarted
    client = pushover.Client(config.pushover_user_key, 
                             api_token=config.pushover_api_key)
    client.send_message(body_str, title=title_str)


def restart_push_notify(sql_db_loc, title_str, log_name):
    """
    Record that a restart has occurred and do a push notification    
    
    :param: sql_db_loc: location of the database file
    :type: sql_db_loc: string  
        
    :param: log_name: name of the log file associated with the restart
    :type: log_name: string

    : return: None
    """
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
        body_str = 'time = %s' % time_str
        send_push_notification(title_str, body_str)
    else:
        push_notify = 0
    # create the tuple that is inserted into the database. Ensure that all
    # parameters are the right data type
    data_tuple = (str(date_str), str(time_str), float(utc_time_now), 
                  int(day_of_week),int(push_notify), str(log_name))    
    sf.insert_process_monitor(sql_db_loc,data_tuple)
 

def delay_push_notify(sql_db_loc, delay_df):
    """
     Send a push notification because a train is delay
    
    :param: sql_db_loc: location of the database process time database
    :type: sql_db_loc: string  
        
    :param: delay_df: dataframe that contains information about the delayed 
        trains
    :type: delay_df: pandas dataframe

    : return: None
    """
    # delay between the push notifications in seconds
    delay_between_push = 5*60
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
    rowsQuery = ('select count(*) from delay_monitor where utc_time > %f ' 
                    'and push_notify = 1' % utc_time_prev)            
    num_rows = sf.query_data(sql_db_loc,rowsQuery)[0][0]
    if num_rows == 0:
        push_notify = 1
        title_str = 'Train Delays'
        body_str = construct_delay_text(delay_df, delay_train_header, 
                                        delay_traincol_buf)
        send_push_notification(title_str, body_str)
    else:
        push_notify = 0
    # create the tuple that is inserted into the database. Ensure that all
    # parameters are the right data type
    data_tuple = (str(date_str), str(time_str), float(utc_time_now), 
                  int(day_of_week),int(push_notify))    
    sf.insert_process_monitor(sql_db_loc,data_tuple)


def construct_delay_text(delay_df, header, col_buf):
    """
    Construct the train delay text
    
    :param: delay_df: dataframe that contains information about the delayed 
        trains
    :type: delay_df: pandas dataframe
    
    :param: header: a list that contains the header of the push notification
    :type: header: a list of strings
    
    :param: col_buf: the number of spaces between columns
    :type: col_buf: int

    :return: body_str: string for the body of the push notification
    :type: body_str: string
    
    """
    if len(delay_df.columns) != len(header):
        raise ValueError(('Length of the header does not match the number of'
                        ' cloumns in the delay_df'))
    # determine which columns have float and int types 
    index_number = delay_df.select_dtypes(include=['float', 'int']).columns
    # convert the numbers to have two decimel points
    delay_df[index_number] = delay_df[index_number].applymap(
                                        lambda x: '{0:.2f}'.format(x))
    (nrows, ncols) = delay_df.shape
    # creates a list of list with the values
    values_list = delay_df.values.tolist() 
    # inserts the header into the beginning
    values_list.insert(0,header)
    nrows = nrows + 1
    # determine the maximum length in all columns. add 2*col_buf
    col_len = [len(max([item[ind] for item in values_list], key=len)) + col_buf
        for ind in range(ncols)]
    # pad the strings so that each element in each column has the same length
    body_str = [[values_list[rInd][cInd].center(col_len[cInd]) 
            for cInd in range(ncols)] for rInd in range(nrows)]
    # collapse the rows into strings
    body_str = [''.join(item) for item in body_str]
    body_str = '\n'.join(body_str)
    return body_str



    
    