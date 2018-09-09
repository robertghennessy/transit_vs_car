"""
Description: Creates the plots (time history, emperical distribution and 
    histograms). Parses the results and saves the results in a sql file.

@author: Robert Hennessy (robertghennessy@gmail.com)
"""

import pandas as pd
import config
import os
import sql_functions as sf
import numpy as np
import matplotlib.pyplot as plt
import shutil
import matplotlib.dates as mdates
import datetime as dt
from dateutil import tz


# Take train if train fraction is greater than this number
take_train_fract = 0.5

  
def create_plots(csv_path_in, data_db_loc, results_db_loc, 
                 ecdf_dir,hist_dir,time_dir):
    """
    Plots the results and postprocesses the data to determine statistics for
    the train trips

    :param: csv_path_in: file location produced by prepare to collect data
    :type: csv_path_in: string
    
    :param: results_db_loc: file location to store the post processed data. 
        This includes the mean and standard deviation of the trip times.
    :type: results_db_loc: string   
    
    :param: ecdf_dir: directory to store the emperical distribution plots
    :type: ecdf_dir: string 
    
    :param: hist_dir: directory to store the histograms
    :type: hist_dir: string 
    
    :param: time_dir: directory to store the time history data
    :type: time_dir: string 
    """
    # read in the schedule trips
    schedule_trips = pd.read_csv(csv_path_in, index_col=0)
    ## Query the maximum time code in the data
    utc_max = sf.query_data(data_db_loc,"select max(utc_time) from traffic_data")
    utc_max = utc_max[0][0]
    # convert the time code to local time
    last_date = dt.datetime.utcfromtimestamp(float(utc_max))
    last_date = last_date.replace(tzinfo=config.from_zone)
    last_date = last_date.astimezone(config.to_zone)
    # query the minimum time code in the data
    utc_min = sf.query_data(data_db_loc,"select min(utc_time) from traffic_data")    
    utc_min = utc_min[0][0]
    # convert the time code to local time
    first_date = dt.datetime.utcfromtimestamp(float(utc_min))
    first_date = first_date.replace(tzinfo=config.from_zone)
    first_date = first_date.astimezone(config.to_zone) 
    # round the first date to the bginning of the month for plotting
    date_min = first_date.replace(day=1)
    # round the last date to the end of the month for plotting
    date_max = last_date.replace(day=1, month=last_date.month+1)
    # create a list of the trip_index stores in the results database
    trip_index_list = sf.query_data(data_db_loc,
                                "select distinct trip_index from traffic_data")  
    trip_index_list = [x[0] for x in trip_index_list]
    trip_index_list.sort()
    # connect to the data database
    conn = sf.create_connection(data_db_loc) 
    for trip_index in trip_index_list:
        print("plotting trip_index = " + str(trip_index))
        # read in the data for a given trip_index
        sql_query = "select * from traffic_data where trip_index = %g" % trip_index
        data_df = pd.read_sql_query(sql_query, conn)
        # convert the date column to datetime
        data_df['date'] = pd.to_datetime(data_df['date'], format="%Y-%m-%d")
        # convert the duration in traffic to minutes
        data_df['duration_in_traffic'] = data_df['duration_in_traffic']/60
        # scheduled trip time in minutes
        sched_trip_time = schedule_trips[schedule_trips['trip_index']==
            trip_index]['sched_trip_duration_secs'].values[0]/60
        duration_in_traffic = data_df['duration_in_traffic'].values
        start_station_str = data_df['start_station'].iloc[0]
        end_station_str = data_df['end_station'].iloc[0]
        train_number = data_df['trip_id'].iloc[0]
        title_str = ('Train ' + str(train_number) + ' - ' + start_station_str 
                    + ' to ' + end_station_str)
        filename = title_str + '.png'
        # plot the histogram of the data
        fig = plt.figure()
        n, bins, patches = plt.hist(duration_in_traffic,
                                    normed=1, facecolor='green', alpha=0.75)
        plt.title(title_str)
        plt.xlabel('Trip Duration [minutes]')
        plt.ylabel('Probability')
        fig.savefig(os.path.join(hist_dir, filename), bbox_inches='tight')
        plt.close(fig)
        # emperical cumulative density 
        sorted_duration_in_traffic = np.sort(duration_in_traffic)
        fig = plt.figure()
        plt.plot(sorted_duration_in_traffic, np.linspace(0, 1, 
                len(duration_in_traffic), endpoint=False))
        plt.xlabel('Trip Duration [minutes]')
        plt.ylabel('ECDF')
        plt.title(title_str)
        fig.savefig(os.path.join(ecdf_dir, filename), bbox_inches='tight')
        plt.close(fig)    
        # Trip duration versus date
        # create pandas series with the missing dates = NaN
        trip_date = data_df[['date', 'duration_in_traffic']]
        trip_date = trip_date.set_index('date')
        date_range = pd.date_range(first_date.date(), last_date.date())
        trip_date.index = pd.DatetimeIndex(trip_date.index)
        trip_date = trip_date.reindex(date_range, fill_value=np.NAN)
        # plot the time series data
        fig, ax = plt.subplots()
        trip_date.plot(style='o-', legend=False, ax=ax)
        plt.ylabel('Trip Duration [minutes]')
        plt.title(title_str)
        ax.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
        # rotate and align the tick labels so they look better
        fig.autofmt_xdate()
        # set the axes so that it starts and ends on the first of a month
        ax.set_xlim(date_min, date_max)
        fig.savefig(os.path.join(time_dir, filename), bbox_inches='tight')
        plt.close(fig)
        
        # determine the fraction of trips greater than the train time
        trip_fract = sum(sorted_duration_in_traffic>sched_trip_time)/len(
            sorted_duration_in_traffic)
        take_train = int(trip_fract>=take_train_fract)
        duration_in_traffic_mean = np.mean(duration_in_traffic)
        duration_in_traffic_std = np.std(duration_in_traffic)
        count = len(data_df.index)
        data = (trip_index, str(train_number), start_station_str,
                      end_station_str, duration_in_traffic_mean,
                      duration_in_traffic_std, trip_fract,
                      take_train, sched_trip_time, count, filename)
        sf.insert_results(results_db_loc, data)


def main():
    # Delete the files in the plot dir
    if os.path.exists(config.plot_dir):
        try:
            shutil.rmtree(config.plot_dir)
        except OSError as e:
            print ("Error: %s - %s." % (e.filename, e.strerror))
    # create the plots directory
    if not os.path.exists(config.plot_dir):
        os.makedirs(config.plot_dir)
    # create the plot subdirectories
    ecdf_dir = os.path.join(config.plot_dir, 'ecdf')
    if not os.path.exists(ecdf_dir):
        os.makedirs(ecdf_dir)
    hist_dir = os.path.join(config.plot_dir, 'hist')
    if not os.path.exists(hist_dir):
        os.makedirs(hist_dir)
    time_dir = os.path.join(config.plot_dir, 'time')
    if not os.path.exists(time_dir):
        os.makedirs(time_dir)
    # turn off the interactive mode for pyplot
    plt.ioff()
    # create the results database
    if os.path.exists(config.results_database):
        os.remove(config.results_database)
    sf.create_results_table(config.results_database)
    
    create_plots(config.trips_csv, config.results_summary_sql, 
                 config.results_database, ecdf_dir, hist_dir,time_dir)
    
if __name__ == '__main__':
    main()
