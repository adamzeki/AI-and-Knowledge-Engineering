from datetime import time, datetime
import pandas as pd

def time_to_seconds(t: time) -> int:
    return t.hour * 3600 + t.minute * 60 + t.second

def seconds_to_time(seconds: int) -> time:
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return time(hour=hours, minute=minutes, second=secs)

def parse_gtfs_time(series: pd.Series) -> pd.Series:
    """Converts HH:MM:SS strings to integer seconds past midnight.
    Efficiently handles GTFS times > 24:00:00.
    """
    # Split by ':' and convert to integers
    parts = series.str.split(':', expand=True).astype(int)
    # Total seconds = H*3600 + M*60 + S
    return (parts[0] * 3600 + parts[1] * 60 + parts[2]).astype('int32')