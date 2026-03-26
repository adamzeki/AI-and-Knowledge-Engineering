import pandas as pd
from pandas import DataFrame, read_csv
from pathlib import Path
from utils import parse_gtfs_time
from typing import NamedTuple

class RailwayTuple(NamedTuple):
    calendar: DataFrame
    calendar_dates: DataFrame
    stops: DataFrame
    trips: DataFrame
    stop_times: DataFrame
    routes: DataFrame

def read_data(dir_path: Path) -> RailwayTuple:
    df_calendar = read_csv(dir_path / 'calendar.txt')
    df_calendar_dates = read_csv(dir_path / 'calendar_dates.txt')
    df_stops = read_csv(dir_path / 'stops.txt', dtype={'stop_id': 'Int32', 'parent_station': 'Int32', 'stop_lat': float, 'stop_lon': float})
    df_trips = read_csv(dir_path / 'trips.txt')
    df_stop_times = read_csv(dir_path / 'stop_times.txt')
    df_routes = read_csv(dir_path / 'routes.txt')

    # Dropping unnecessary columns
    df_stops.drop(columns=['stop_code', 'stop_desc', 'platform_code'], inplace=True, errors='ignore')
    df_routes.drop(columns=['agency_id', 'route_color', 'route_text_color'], inplace=True, errors='ignore')
    df_trips.drop(columns=['trip_headsign', 'direction_id', 'block_id'], inplace=True, errors='ignore')
    df_stop_times.drop(columns=['stop_headsign', 'shape_dist_traveled'], inplace=True, errors='ignore')

    # Processing dates
    for df in [df_calendar, df_calendar_dates]:
        date_cols = [c for c in ['start_date', 'end_date', 'date'] if c in df.columns]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col].astype(str), format='%Y%m%d').dt.date

    # Processing times
    for col in ['arrival_time', 'departure_time']:
        df_stop_times[col] = parse_gtfs_time(df_stop_times[col])

    # ID Optimization - replacing string IDs with new integer IDs for memory efficiency, faster lookups and comparisons
    # Define which tables/columns share the same ID "namespace"
    id_namespaces = {
        'trip_id': [(df_trips, 'trip_id'), (df_stop_times, 'trip_id')],
        'route_id': [(df_routes, 'route_id'), (df_trips, 'route_id')],
        'service_id': [(df_calendar, 'service_id'), (df_calendar_dates, 'service_id'), (df_trips, 'service_id')],
        'stop_id': [(df_stops, 'stop_id'), (df_stop_times, 'stop_id'), (df_stops, 'parent_station')]
    }

    for id_name, references in id_namespaces.items():
        # Find every unique string ID across all related tables
        all_vals = []
        for df, col in references:
            if col in df.columns:
                all_vals.append(df[col].dropna().astype(str))
        
        unique_ids = pd.unique(pd.concat(all_vals))
        
        # Create the Integer Mapping { "string_id": int_id }
        mapping = {val: i for i, val in enumerate(unique_ids)}
        
        # Apply the mapping to every table
        for df, col in references:
            if col in df.columns:
                # Int32 to allow for NaNs in columns like parent_station
                df[col] = df[col].astype(str).map(mapping).astype('Int32')

    df_trips.set_index('trip_id', inplace=True) # Set trip_id as index for faster lookups when building the graph
    df_calendar.set_index('service_id', inplace=True)
    df_calendar_dates.set_index('service_id', inplace=True)

    return RailwayTuple(
        calendar=df_calendar,
        calendar_dates=df_calendar_dates,
        stops=df_stops,
        trips=df_trips,
        stop_times=df_stop_times,
        routes=df_routes
    )

def main():
    base_path = Path(__file__).resolve().parent
    dir_path = base_path / 'google_transit'
    dfs = read_data(dir_path)
    
    for df in dfs:
        print(df.head())

if __name__ == "__main__":
    main()