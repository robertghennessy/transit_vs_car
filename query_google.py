# -*- coding: utf-8 -*-
"""
Program to query google maps

@author: Robert (rghennessy@gmail.com)
"""

import googlemaps
from datetime import datetime
import pandas as pd
import config

gmaps = googlemaps.Client(key=config.google_transit_api_key)

schedule_trips = pd.read_csv(config.trips_csv_path_in)
schedule_trips = schedule_trips.sort_values(['departure_time_timedelta_start', 'arrival_time_timedelta_stop'])

first_trip = schedule_trips.loc[0]
start_loc = {
        "lat" : first_trip['stop_lat_start'],
        "lng" : first_trip['stop_lon_start']
}
end_loc = {
        "lat" : first_trip['stop_lat_stop'],
        "lng" : first_trip['stop_lon_stop']
}
# Hillsdale to Milbrae

# Request directions for driving between the two stations
now = datetime.now()
directions_result = gmaps.directions(start_loc,
                                     end_loc,
                                     mode="driving",
                                     departure_time=now)

# duration in traffic in seconds                          
duration_in_traffic = directions_result[0]['legs'][0]['duration_in_traffic']['value']

                                   
# i should store the whole thing just in case i need to process it in the future
