"""
Description: Creates the plots (time history, empirical distribution and
    histograms). Parses the results and saves the results in a sql file.

@author: Robert Hennessy (robertghennessy@gmail.com)
"""

import datetime as dt
import os
import shutil

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import config
import file_functions as ff
import sql_functions as sf

# Take train if train fraction is greater than this number
take_train_fraction = 0.5


def create_plots(csv_path_in, traffic_db_loc, results_db_loc,
                 ecdf_dir, hist_dir, time_dir):
    """
    Plots the results and post processes the data to determine statistics for
    the train trips

    :param csv_path_in: file location for schedule_trips.csv
    :type csv_path_in: string
    
    :param results_db_loc: file location to store the post processed data.
        This includes the mean and standard deviation of the trip times.
    :type results_db_loc: string
    
    :param ecdf_dir: directory to store the empirical distribution plots
    :type ecdf_dir: string
    
    :param hist_dir: directory to store the histograms
    :type hist_dir: string
    
    :param time_dir: directory to store the time history data
    :type time_dir: string
    """
    # read in the schedule trips
    schedule_trips = pd.read_csv(csv_path_in, index_col=0)
    # query the first and last date in the database  
    first_date = min_max_date(traffic_db_loc, 'traffic_data', 'utc_time',
                              'min')
    last_date = min_max_date(traffic_db_loc, 'traffic_data', 'utc_time', 'max')
    # round the first date to the beginning of the month for plotting
    date_min = first_date.replace(day=1)
    # round the last date to the end of the month for plotting
    date_max = last_date.replace(day=1, month=last_date.month + 1)
    # connect to the data database
    traffic_conn = sf.create_connection(traffic_db_loc)

    for trip_index in list(schedule_trips.index):
        print("plotting trip_index = " + str(trip_index))
        # read in the data for a given trip_index
        sql_query = """select * from traffic_data where 
                        trip_index = %g""" % trip_index
        traffic_data_df = pd.read_sql_query(sql_query, traffic_conn)
        # convert the date column to datetime
        traffic_data_df['date'] = pd.to_datetime(traffic_data_df['date'],
                                                 format="%Y-%m-%d")
        # convert the duration in traffic to minutes
        traffic_data_df['duration_in_traffic'] = traffic_data_df[
                                                     'duration_in_traffic'] / 60
        # scheduled trip time in minutes
        sched_trip_time = \
            schedule_trips[schedule_trips['trip_index'] ==
                           trip_index]['sched_trip_duration_secs'].values[
                0] / 60
        duration_in_traffic = traffic_data_df['duration_in_traffic'].values
        start_station_str = traffic_data_df['start_station'].iloc[0]
        end_station_str = traffic_data_df['end_station'].iloc[0]
        train_number = traffic_data_df['trip_id'].iloc[0]
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
        # empirical cumulative density
        sorted_duration_in_traffic = np.sort(duration_in_traffic)
        fig = plt.figure()
        plt.plot(sorted_duration_in_traffic,
                 np.linspace(0, 1, len(duration_in_traffic), endpoint=False))
        plt.xlabel('Trip Duration [minutes]')
        plt.ylabel('ECDF')
        plt.title(title_str)
        fig.savefig(os.path.join(ecdf_dir, filename), bbox_inches='tight')
        plt.close(fig)
        # Trip duration versus date
        # create pandas series with the missing dates = NaN
        trip_date = traffic_data_df[['date', 'duration_in_traffic']]
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
        trip_fraction = sum(
            sorted_duration_in_traffic > sched_trip_time) / len(
            sorted_duration_in_traffic)
        take_train = int(trip_fraction >= take_train_fraction)
        duration_in_traffic_mean = np.mean(duration_in_traffic)
        duration_in_traffic_std = np.std(duration_in_traffic)
        count = len(traffic_data_df.index)
        data = (trip_index, str(train_number), start_station_str,
                end_station_str, duration_in_traffic_mean,
                duration_in_traffic_std, trip_fraction,
                take_train, sched_trip_time, count, filename)
        sf.insert_results(results_db_loc, data)


def create_traffic_results_df(db_loc, table_name, trip_index):
    """                                  
    Pulls the data from the traffic database and performs some 
        processing.

    :param db_loc: location of the db locations
    :type db_loc: string
    
    :param table_name: name of the table to query
    :type table_name: string
    
    :param trip_index: trip index number
    :type trip_index: int
    
    :return: traffic_data_df,  data frame that contains the date and trip
        duration
    :rtype: pandas data frame
    """
    traffic_conn = sf.create_connection(db_loc)
    sql_query = """select * from %s where 
                    trip_index = %g""" % (table_name, trip_index)
    traffic_data_df = pd.read_sql_query(sql_query, traffic_conn)
    # convert the date column to datetime
    traffic_data_df['date'] = pd.to_datetime(traffic_data_df['date'],
                                             format="%Y-%m-%d")
    # convert the duration in traffic to minutes
    traffic_data_df['duration_in_traffic'] = traffic_data_df[
                                                 'duration_in_traffic'] / 60
    traffic_data_df = traffic_data_df[['date', 'duration_in_traffic']]
    traffic_data_df = traffic_data_df.set_index('date')
    traffic_data_df.index = pd.DatetimeIndex(traffic_data_df.index)
    return traffic_data_df


def create_transit_results_df(db_loc, table_name, trip_id,
                              stop_id, train_sched_trip_duration):
    """                                  
    Pulls the data from the gtfs-rt transit database and performs some 
        processing.

    :param db_loc: location of the db locations
    :type db_loc: string
    
    :param table_name: name of the table to query
    :type table_name: string
    
    :param trip_id: train number
    :type trip_id: string

    :param stop_id: stop id number
    :type stop_id: string

    :param train_sched_trip_duration: the scheduled length of the train trip
    :type train_sched_trip_duration: float
    
    :return: transit_data_df, data frame that contains the date and trip
        duration
    :rtype: pandas data frame
    """
    transit_conn = sf.create_connection(db_loc)
    sql_query = """select * from %s where 
                    trip_id = %g and stop_id = %g """ % (table_name, trip_id,
                                                         stop_id)
    transit_data_df = pd.read_sql_query(sql_query, transit_conn)
    # convert the date column to date times
    transit_data_df['train_start_date'] = pd.to_datetime(transit_data_df[
                                                             'train_start_date'],
                                                         format="%Y-%m-%d")
    # calculate the length of the train trip
    transit_data_df['train_duration'] = (transit_data_df['departure_delay'] +
                                         train_sched_trip_duration) / 60
    transit_data_df.rename(index=str,
                           columns={"train_start_date": "date"},
                           inplace=True)
    transit_data_df = transit_data_df[['date', 'train_duration']]
    transit_data_df = transit_data_df.set_index('date')
    transit_data_df.index = pd.DatetimeIndex(transit_data_df.index)
    return transit_data_df


def min_max_date(db_loc, table, column, func):
    """
    Query a date column and return the min/max date
    
    :param db_loc: location of the db locations
    :type db_loc: string

    :param table: name of the table that should be queried
    :type table: string
    
    :param column: name of the column that should be queried
    :type column: string
    
    :param func: function to apply to the column (min or max)
    :type func: string
    
    :return: date, return the date that corresponds to the applied function
    :rtype: datetime
    
    """
    sql_query = "select %s(%s) from %s" % (func, column, table)
    utc = sf.query_data(db_loc, sql_query)
    utc = utc[0][0]
    # convert the time code to local time
    date = dt.datetime.utcfromtimestamp(float(utc))
    date = date.replace(tzinfo=config.from_zone)
    date = date.astimezone(config.to_zone)
    return date


def plot_time_history(df, title_str, leg_str, sche_time=None):
    # plot the time series data
    fig, ax = plt.subplots()
    df.plot(style='o-', legend=False, ax=ax)
    plt.ylabel('Trip Duration [minutes]')
    plt.title(title_str)
    ax.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
    # rotate and align the tick labels so they look better
    fig.autofmt_xdate()
    # set the axes so that it starts and ends on the first of a month
    # ax.set_xlim(date_min, date_max)
    # set the y axes limit
    max_data = df.max().max()
    if max_data == int(max_data):
        max_data = max_data + 1
    else:
        max_data = np.ceil(max_data)
    min_data = df.min().min()
    if min_data == int(min_data):
        min_data = min_data - 1
    else:
        min_data = np.floor(min_data)
    ax.set_ylim(min_data, max_data)
    if sche_time is not None:
        ax.axhline(y=sche_time, linestyle='--', color='r', label='Schedule')
        # add legend
        leg_str.append('Schedule')
    plt.legend(leg_str, loc='lower left')


def main():
    # Delete the files in the plot dir
    if os.path.exists(config.plot_dir):
        try:
            shutil.rmtree(config.plot_dir)
        except OSError as e:
            print("Error: %s - %s." % (e.filename, e.strerror))
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
    ff.remove_files([config.results_summary_sql])
    sf.create_results_table(config.results_summary_sql)

    create_plots(config.trips_csv, config.traffic_data_sql,
                 config.results_summary_sql, ecdf_dir,
                 hist_dir, time_dir)


if __name__ == '__main__':
    main()
