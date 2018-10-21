"""
Description: Contains public configuration.

@author: Robert Hennessy (robertghennessy@gmail.com)
"""
from dateutil import tz
import os

import private_config


# gfts file location 
gtfs_zip_path = private_config.gtfs_zip_path
#directories
base_dir = private_config.base_dir
# directory names
file_dir_name = 'files'
plot_dir_name = 'plots'
logs_dir_name = 'logs'
# construt normal directories
file_dir = os.path.join(base_dir,file_dir_name)
plot_dir = os.path.join(base_dir,plot_dir_name)
logs_dir = os.path.join(base_dir,logs_dir_name)
# construt test directories
test_file_dir = os.path.join(base_dir,'test_' + file_dir_name)
test_plot_dir = os.path.join(base_dir,'test_' + plot_dir_name)
test_logs_dir = os.path.join(base_dir,'test_' + logs_dir_name)
# filenames
collect_data_log_filename = 'CollectData-%s.txt'
prepare_log_filename = 'Prepare-%s.txt'
transit_log_filename = 'TransitSchedulerLog-%s.txt'
# log file locations
collect_data_log_file = os.path.join(logs_dir, collect_data_log_filename)
prepare_log_file = os.path.join(logs_dir, prepare_log_filename)
transit_log_file = os.path.join(logs_dir, transit_log_filename)
# test log file locations
test_collect_data_log_file = os.path.join(test_logs_dir,
                                          collect_data_log_filename)
test_prepare_log_file = os.path.join(test_logs_dir, prepare_log_filename)
test_transit_log_file = os.path.join(test_logs_dir, transit_log_filename)
# filenames of the sql databases
traffic_data_sql_filename = 'taffic_data.sqlite'
siri_data_sql_filename = 'siri_data.sqlite'
gtfs_rt_data_sql_filename = 'gtfs_rt_data.sqlite'
scheduler_sql_filename = 'tasks.sqlite'
process_monitor_sql_filename = 'process_monitor.sqlite'
push_notification_sql_filename = 'push_notification.sqlite'
results_summary_sql_filename = 'results_summary.sqlite'
gfts_rt_table_name = 'transit_data_gtfs_rt'
siri_table_name = 'transit_data_siri'
# sql database locations
traffic_data_sql = os.path.join(file_dir, traffic_data_sql_filename)
siri_data_sql = os.path.join(file_dir, siri_data_sql_filename)
gtfs_rt_data_sql = os.path.join(file_dir, gtfs_rt_data_sql_filename)
scheduler_sql = os.path.join(file_dir, scheduler_sql_filename)
process_monitor_sql = os.path.join(file_dir, process_monitor_sql_filename)
push_notification_sql = os.path.join(file_dir, push_notification_sql_filename)
results_summary_sql = os.path.join(file_dir, results_summary_sql_filename)
# test sql database locations
test_traffic_data_sql = os.path.join(test_file_dir, traffic_data_sql_filename)
test_siri_data_sql = os.path.join(test_file_dir, siri_data_sql_filename)
test_gtfs_rt_data_sql = os.path.join(test_file_dir, gtfs_rt_data_sql_filename)
test_scheduler_sql = os.path.join(test_file_dir, scheduler_sql_filename)
test_process_monitor_sql = os.path.join(test_file_dir, 
                                        process_monitor_sql_filename)
test_results_summary_sql = os.path.join(test_file_dir, 
                                        results_summary_sql_filename)
# name of the csv files
trips_csv_filename = 'schedule_trips.csv'
periodic_jobs_csv_filename = 'periodic_jobs_schedule.csv'
schedule_monitor_csv_filename = 'schedule_monitor.csv'
# create csv locations
trips_csv = os.path.join(file_dir,trips_csv_filename)
periodic_jobs_csv = os.path.join(file_dir, periodic_jobs_csv_filename)
schedule_monitor_csv = os.path.join(file_dir,schedule_monitor_csv_filename)
# create tst csv locations
test_trips_csv = os.path.join(test_file_dir,trips_csv_filename)
test_periodic_jobs_csv = os.path.join(test_file_dir, periodic_jobs_csv_filename)

# pushover keys
pushover_api_key = private_config.pushover_api_key
pushover_user_key = private_config.pushover_user_key
# google transit api key
google_transit_api_key = private_config.google_transit_api_key
# 511 api key
transit_511_api_key= private_config.transit_511_api_key

### global constants
weekday_names = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 
                 'saturday', 'sunday']
day_of_week_codes = ['mon','tue','wed','thu','fri','sat','sun']
# used to convert from UTC to PDT/PST
to_zone = tz.gettz('America/San_Francisco')
from_zone = tz.gettz('UTC')
# disctionary whose values are appended to scheduler id
sched_id_dict = {'siri': 'sr_', 'gtfs-rt': 'grt_'}
