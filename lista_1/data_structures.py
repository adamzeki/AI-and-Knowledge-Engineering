from dataclasses import dataclass
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime, time, timedelta, date

R = 6371.0  # Earth radius in kilometers

@dataclass
class Trip:
    '''
    Represents a trip from one existing stop to another
    '''
    departure: int
    arrival: int
    valid_from: date
    valid_to: date
    weekdays: dict[int, bool]
    added_on: set[date]
    removed_on: set[date]

    def cost(self, start_date: date, time_elapsed: int) -> float:
        '''
        Returns the cost (in seconds) of taking this trip, based on the arrival time to the previous stop and the departure and travel time of this trip
        '''
        days_elapsed, seconds_elapsed = divmod(time_elapsed, 86400)
        new_date = start_date + timedelta(days=days_elapsed)

        if new_date in self.removed_on:
            return float('inf')

        if new_date < self.valid_from or new_date > self.valid_to or self.weekdays[new_date.weekday()] == False:
            if new_date not in self.added_on:
                return float('inf')
        
        return self.arrival - seconds_elapsed
    
@dataclass
class Edge:
    '''
    Represents a railway connection between two existing stops
    '''
    trips: dict[int, Trip]

    def add_trip(self, trip_id: int, departure: int, arrival: int, valid_from: date, valid_to: date, weekdays: dict[int, bool], added_on: set[date], removed_on: set[date]): 
        self.trips[trip_id] = Trip(departure, arrival, valid_from, valid_to, weekdays, added_on, removed_on) # There should be no instance of two trips with the same id, but a different timedate. If there is, it's a problem with the data

    def is_loaded(self) -> bool:
        '''
        Returns whether this edge has any trips loaded into it
        '''
        return len(self.trips) > 0

    def cost(self, start_date: date, time_elapsed: int, layover_time: int = 0) -> tuple[int, int]: # sort and do bisect search for huge opt. may have to break it up into two functions, one for time other for layovers
        '''
        Returns the lowest cost (in seconds) of taking this edge, based on the arrival time to the previous stop and the departure and travel time of a trip along this edge
        '''
        min_cost = float('inf')
        final_trip_id = -1
        for trip_id, trip in self.trips.items():
            cost = trip.cost(start_date, time_elapsed + layover_time*60)
            if cost < min_cost:
                min_cost = cost
                final_trip_id = trip_id

        return min_cost, final_trip_id

@dataclass
class Node:
    '''
    Represents an existing parent stop in the railway network
    '''
    stop_id: int
    lat: float
    lon: float
    children: set[int]
    edges: dict[int, Edge]

    def add_child(self, child_id: int):
        self.children.add(child_id)

    def add_edge(self, next_id: int): # Probably unnecessary
        if next_id not in self.edges:
            self.edges[next_id] = Edge(next_id, [])
    
    def add_trip(self, next_id: int, trip_id: int, departure: int, arrival: int, valid_from: date, valid_to: date, weekdays: dict[int, bool], added_on: set[date], removed_on: set[date]):
        if next_id not in self.edges:
            self.edges[next_id] = Edge(next_id, [])
        
        self.edges[next_id].add_trip(trip_id, departure, arrival, valid_from, valid_to, weekdays, added_on, removed_on)

    def distance(self, other) -> float:
        '''
        Calculates the distance between this stop and another stop using the Haversine formula
        '''
        global R
        lat1 = radians(self.lat)
        lon1 = radians(self.lon)
        lat2 = radians(other.lat)
        lon2 = radians(other.lon)

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        return R * c