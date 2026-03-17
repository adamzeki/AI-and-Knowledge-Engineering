from data_reader import read_data, RailwayTuple
from data_structures import *
from datetime import date, time, datetime
from pathlib import Path
import pandas as pd
from pandas import DataFrame

dfs: RailwayTuple = None # Global variable to hold the dataframes read from the GTFS files, so they can be accessed by other functions without needing to pass them around as parameters
stops: dict[int, Node] = {} # Map for storing the graph, mapping stop_ids to their Node representations
best_time: dict[int, int] = {} # Map for storing the best time (in seconds) to reach each stop, initialized to infinity for all stops except the starting stop which is initialized to 0
curr_trip: dict[int, int] = {} # Map for storing the current trip_id used in the best solution to reach each stop, initialized to -1 for all stops to indicate that they have not been reached yet
best_layovers: dict[int, int] = {} # Map for storing the current lowest layover count to reach each stop, initialized to infinity for all stops except the starting stop which is initialized to 0

def initialize(dir_path: Path):
    def add_stop(stop_id: int, stop_name: str, stop_lat: float, stop_lon: float):
        stops[stop_id] = Node(
            stop_id=stop_id,
            stop_name=stop_name,
            stop_lat=stop_lat,
            stop_lon=stop_lon,
            edges={}
        )
        best_time[stop_id] = float('inf')
        curr_trip[stop_id] = -1
        best_layovers[stop_id] = float('inf')

    global dfs
    dfs = read_data(dir_path)

    # Separate parent and child stops to ensure parent stops are added before their children, so that we can properly link them in the graph
    parents = dfs.stops[dfs.stops['parent_station'].isna()]
    children = dfs.stops[dfs.stops['parent_station'].notna()]

    for row in parents.itertuples():
        add_stop(row.stop_id, row.stop_name, row.stop_lat, row.stop_lon)

    for row in children.itertuples(): # There shouldnt be any "orphaned" stops. If there are, we treat them as parent stops
        parent_id = row.parent_station
        if parent_id in stops:
            stops[parent_id].add_child(row.stop_id) # can we do this or do we have to acknowledge which child were going to? I think this is a good approach, cuts node count by around 60%
        else:
            add_stop(row.stop_id, row.stop_name, row.stop_lat, row.stop_lon)
    
    df_stop_times_sorted = dfs.stop_times.sort_values(by=['trip_id', 'stop_sequence'], ascending=[True, True]) # Sort stop_times by trip_id and stop_sequence to ensure we process stops in the correct order for each trip

    for trip_id, group in df_stop_times_sorted.groupby('trip_id'):
        stop_to_process_idx = -1
        stops_in_trip = group['stop_id'].tolist()

        for i in range(len(stops_in_trip) - 1):
            curr_stop = stops_in_trip[stop_to_process_idx] if stop_to_process_idx != -1 else stops_in_trip[i]
            next_stop = stops_in_trip[i+1]

            if next_stop.pickup_type == 0:
                stop_to_process_idx = -1 # Since next stop is usable, we dont need to save the index of stop to be processed

                trip_match = dfs.trips[dfs.trips.index == curr_stop.trip_id]
                current_service_id = trip_match.iloc[0]['service_id']

                cal_row = dfs.calendar[dfs.calendar.index == current_service_id].iloc[0]
                valid_from = cal_row['start_date']
                valid_to = cal_row['end_date']
                weekdays = {
                    0: bool(cal_row['monday']),
                    1: bool(cal_row['tuesday']),
                    2: bool(cal_row['wednesday']),
                    3: bool(cal_row['thursday']),
                    4: bool(cal_row['friday']),
                    5: bool(cal_row['saturday']),
                    6: bool(cal_row['sunday'])
                }

                special_dates = dfs.calendar_dates[dfs.calendar_dates.index == current_service_id]
                added_on = set(special_dates[special_dates['exception_type'] == 1]['date'])
                removed_on = set(special_dates[special_dates['exception_type'] == 2]['date'])

                stops[curr_stop.stop_id].add_trip(
                    next_stop.stop_id,
                    trip_id,
                    curr_stop.departure_time,
                    next_stop.arrival_time,
                    valid_from,
                    valid_to,
                    weekdays,
                    added_on,
                    removed_on
                )
            else:
                stop_to_process_idx = i if stop_to_process_idx == -1 else stop_to_process_idx # Next stop is unusable, so we need to save the one we havent processed yet
