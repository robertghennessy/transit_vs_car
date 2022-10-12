"""
Description: This program was written to collect the data by calling the 
    functions in the sql task database.
    
@author: Robert Hennessy (robertghennessy@gmail.com)
"""

import datetime as dt
import logging.handlers

import config
import push_notification as pn
import scheduler_functions as sched

# set up the root logger
logger = logging.getLogger('')
logger.setLevel(logging.INFO)
dateTag = dt.datetime.now().strftime("%Y-%b-%d_%H-%M-%S")
log_filename = config.collect_data_log_file % dateTag
# add a rotating handler, rotate the file every 10 mb. Keep the last 5 logfiles
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler = logging.handlers.RotatingFileHandler(log_filename,
                                               maxBytes=10*1024*1024,
                                               backupCount=5)
handler.setFormatter(formatter)
logger.addHandler(handler)


def main():
    """
    Runs the tasks in the task database.

    :return None
    :rtype: None
    """
    # save the process data monitor and send a push notification 
    # when restarted
    pn.restart_push_notify(config.process_monitor_sql,
                           'Car vs Caltrain Restarted', log_filename)
    # run the tasks in the database
    sched.run_tasks(config.scheduler_sql) 


if __name__ == '__main__':
    main()
