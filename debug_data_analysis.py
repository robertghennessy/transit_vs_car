# -*- coding: utf-8 -*-
"""
Created on Sun Oct 28 15:27:15 2018

@author: Robert
"""
import pandas as pd
import sql_functions as sf
import config
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime as dt
import numpy as np
import data_analysis as da

traffic_db_loc = config.traffic_data_sql
transit_db_loc = config.gtfs_rt_data_sql
schedule_trips = pd.read_csv(config.trips_csv, index_col=0)

last_date_traffic = da.min_max_date(traffic_db_loc, 'traffic_data', 
                                    'utc_time', 'max')
first_date_traffic = da.min_max_date(traffic_db_loc, 'traffic_data', 
                                     'utc_time', 'min')
last_date_transit = da.min_max_date(transit_db_loc, 'transit_data_gtfs_rt', 
                                    'AimedDepartureTime_utc', 'max')
first_date_transit = da.min_max_date(transit_db_loc, 'transit_data_gtfs_rt', 
                                     'AimedDepartureTime_utc', 'min')
# determine the minimum and maximum date
first_date = min(first_date_traffic, first_date_transit)
last_date = max(last_date_traffic, last_date_transit)
# round the first date to the bginning of the month for plotting
date_min = first_date.replace(day=1)
# round the last date to the end of the month for plotting
date_max = last_date.replace(day=1, month=last_date.month+1)
                        
trip_ind = 0                        

trip_id = schedule_trips.iloc[trip_ind]['trip_id']
stop_id = schedule_trips.iloc[trip_ind]['stop_id_stop']
train_sched_trip_duration = schedule_trips.iloc[trip_ind][
                                    'sched_trip_duration_secs']

transit_conn = sf.create_connection(config.gtfs_rt_data_sql) 
gtfs_rt_data_df = da.create_transit_results_df(config.gtfs_rt_data_sql, 
                                             'transit_data_gtfs_rt', trip_id,
                                             stop_id, 
                                             train_sched_trip_duration)
traffic_data_df = da.create_traffic_results_df(config.traffic_data_sql, 
                                             'traffic_data', trip_ind)
title_str = 'test'
leg_str = ['Car','Train']
# create pandas series with the missing dates = NaN
date_range = pd.date_range(date_min.date(), date_max.date())
traffic_data_df = traffic_data_df.reindex(date_range, fill_value=np.NAN)
# merge the 
trip_date = traffic_data_df.merge(gtfs_rt_data_df, left_index=True, 
                            right_index=True, how='outer')

da.plot_time_history(trip_date, title_str, leg_str, 
                     train_sched_trip_duration/60)