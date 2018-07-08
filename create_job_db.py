# -*- coding: utf-8 -*-
"""


@author: Robert (rghennessy@gmail.com)
"""

from datetime import datetime, timedelta
import sys
import os
import config
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler

weekday_names_sched_trips = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
day_of_week_codes = ['mon','tue','wed','thu','fri','sat','sun']

schedule_trips = pd.read_csv(config.trips_csv_path_in, index_col=0)
schedule_trips = schedule_trips.sort_values(['departure_time_timedelta_start', 'arrival_time_timedelta_stop'])
schedule_trips_index = schedule_trips.index

## If file exists, delete it ##
if os.path.isfile(config.scheduler_sql_loc):
    os.remove(config.scheduler_sql_loc)
else:    ## Show an error ##
    print("Error: %s file not found" % config.scheduler_sql_loc)



# pass the location and index
def temp(index,start_loc,end_loc):
    print('index %d.' % index)

scheduler = BackgroundScheduler()
scheduler.add_jobstore('sqlalchemy', url='sqlite:///%s' % config.scheduler_sql_loc)

for sInd in range(len(schedule_trips)):
        trip = schedule_trips.loc[sInd]
        trip_id = schedule_trips_index[sInd]
        start_loc = {
            "lat" : trip['stop_lat_start'],
            "lng" : trip['stop_lon_start']
        }
        end_loc = {
            "lat" : trip['stop_lat_stop'],
            "lng" : trip['stop_lon_stop']
        }        
        
        sched_time = datetime.strptime(trip['departure_time_start'],"%H:%M:%S")
        day_code =''
        for day_ind in range(len(weekday_names_sched_trips)):
            if trip[weekday_names_sched_trips[day_ind]]:
                if day_code == '':
                    day_code = day_code + day_of_week_codes[day_ind]
                else :
                    day_code = day_code + ',' + day_of_week_codes[day_ind]
        # misfire_grace_time - seconds after the designated runtime that the job is still allowed to be run
        scheduler.add_job(temp,'cron',day_of_week=day_code, hour=sched_time.hour, minute=sched_time.minute, 
                          misfire_grace_time=120,id=str(trip_id),args=[trip_id,start_loc,end_loc])
#scheduler.print_jobs()
scheduler.start()
scheduler.shutdown()
