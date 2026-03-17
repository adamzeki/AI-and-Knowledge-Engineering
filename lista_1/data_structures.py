from attr import dataclass
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime, time, timedelta, date

R = 6371.0  # Earth radius in kilometers

@dataclass
class Trip:
    '''
    Represents a trip from one existing stop to another

    Attributes:
        trip_id: Unique identifier for the trip
        departure: Time of departure from the parent stop (in seconds since midnight)
        arrival: Time of arrival at the destination stop (in seconds since midnight)
        valid_from: Date from which this trip exists
        valid_to: Date until which this trip exists
        weekdays: Mapping of weekday (0-6) to a boolean indicating whether this trip operates on said weekday
        added_on: Set of special dates on which this trip is added to the schedule
        removed_on: Set of special dates on which this trip is removed from the schedule
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
        Args:
            prev_arrival: The arrival time at the previous stop
        Returns:
            The cost (in seconds) of taking this trip, or infinity if this trip is not available based on the arrival time at the previous stop
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

    Attributes:
        next_stop_id: Unique identifier for the stop that this edge leads to
        trips: Dictionary of trips that operate along this edge, with the key being the trip's id
    '''
    trips: dict[int, Trip]

    def add_trip(self, trip_id: int,departure: int, arrival: int, valid_from: date, valid_to: date, weekdays: dict[int, bool], added_on: set[date], removed_on: set[date]): 
        self.trips[trip_id] = Trip(departure, arrival, valid_from, valid_to, weekdays, added_on, removed_on) # There should be no instance of two trips with the same id, but a different timedate. If there is, it's a problem with the data

    def cost(self, start_date: date, time_elapsed: int) -> int: # sort and do bisect search for huge opt
        '''
        Returns the lowest cost (in seconds) of taking this edge, based on the arrival time to the previous stop and the departure and travel time of a trip along this edge
        Args:
            prev_arrival: The arrival time at the previous stop
        Returns:
            The lowest cost (in seconds) of taking this edge, or infinity if no trip is available
        '''
        min_cost = float('inf')
        for trip_id, trip in self.trips.items():
            cost = trip.cost(start_date, time_elapsed)
            if cost < min_cost:
                min_cost = cost

        return min_cost

@dataclass
class Node:
    '''
    Represents an existing parent stop in the railway network

    Attributes:
        stop_id: Unique identifier for the stop
        lat: Latitude of the stop
        lon: Longitude of the stop
        children: Set of stop's children stops
        edges: Dictionary representing railway connections from this stop to other stops, with the key being the other stop's id
    '''
    stop_id: int
    lat: float
    lon: float
    children: set[int]
    edges: dict[int, Edge]

    def add_child(self, child_id: int):
        self.children.add(child_id)

    def add_edge(self, next_id: int):
        if next_id not in self.edges:
            self.edges[next_id] = Edge(next_id, [])

    def distance(self, other: Node) -> float:
        '''
        Calculates the distance between this stop and another stop using the Haversine formula

        Args:
            other: Another Node object representing a different stop

        Returns:
            The distance in kilometers between the two stops
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