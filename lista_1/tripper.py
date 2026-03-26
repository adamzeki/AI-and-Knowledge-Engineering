from collections import deque
from data_reader import read_data, RailwayTuple
from data_structures import *
from datetime import date, time, datetime, timedelta
from pathlib import Path
import pandas as pd
from pandas import DataFrame
from utils import time_to_seconds, seconds_to_time
import time
import random as rnd
import heapq

MAX_TS_ITER = 100
NO_IMPROVE_LIMIT = 20
T_SIZE = 10
TS_CANDIDATE_SELECT_COUNT = 10

dfs: RailwayTuple                   # Global variable to hold the dataframes read from the GTFS files, so they can be accessed by other functions without needing to pass them around as parameters
stops: dict[int, Node] = {}         # Map for storing the graph, mapping stop_ids to their Node representations
best_time: dict[int, int] = {}      # Map for storing the best time (in seconds) to reach each stop, initialized to infinity for all stops except the starting stop which is initialized to 0
best_estimate: dict[int, int] = {}  # Basiacally f in A*
best_trip: dict[int, int] = {}      # Map for storing the current trip_id used in the best solution to reach each stop, initialized to -1 for all stops to indicate that they have not been reached yet // maybe unnecessary??
prev_stop: dict[int, int] = {}      # Map current stop_id to the id of the stop we came from (in the best solution at the moment)
best_layovers: dict[int, int] = {}  # Map for storing the current lowest layover count to reach each stop, initialized to infinity for all stops except the starting stop which is initialized to 0 // PROBABLY UNNECESSARY, INFO STORED IN HEAPQ
child_to_parent: dict[int, int] = {}

def initialize(dir_path: Path):
    def add_stop(stop_id: int, stop_name: str, stop_lat: float, stop_lon: float):
        stops[stop_id] = Node(
            stop_id=stop_id,
            stop_lat=stop_lat,
            stop_lon=stop_lon,
            edges={},
            children = set()
        )
        best_time[stop_id] = INT_INF
        best_trip[stop_id] = -1
        prev_stop[stop_id] = -1
        best_layovers[stop_id] = INT_INF
        child_to_parent[stop_id] = stop_id


    global dfs
    dfs = read_data(dir_path)

    # Separate parent and child stops to ensure parent stops are added before their children, so that we can properly link them in the graph
    parents = dfs.stops[dfs.stops['parent_station'].isna()]
    children = dfs.stops[dfs.stops['parent_station'].notna()]

    for row in parents.itertuples():
        add_stop(row.stop_id, row.stop_name, row.stop_lat, row.stop_lon)

    for row in children.itertuples(): # There shouldn't be any "orphaned" stops. If there are, we treat them as parent stops
        parent_id = row.parent_station

        if parent_id == 0:
            pass

        if parent_id in stops:
            stops[parent_id].add_child(row.stop_id) # can we do this or do we have to acknowledge which child were going to? I think this is a good approach, cuts node count by around 60%
            stops[row.stop_id] = stops[parent_id] # So we can easily find the proper node object for children // is it better than looking them up? I think so, but not sure

            child_to_parent[row.stop_id] = parent_id

        else:
            add_stop(row.stop_id, row.stop_name, row.stop_lat, row.stop_lon)

    df_stop_times_sorted = dfs.stop_times.sort_values(by=['trip_id', 'stop_sequence'], ascending=[True, True]) # Sort stop_times by trip_id and stop_sequence to ensure we process stops in the correct order for each trip

    for trip_id, group in df_stop_times_sorted.groupby('trip_id'):
        current_service_id = dfs.trips.loc[trip_id]['service_id']
        
        cal_row = dfs.calendar.loc[current_service_id]
        
        valid_from = cal_row['start_date']
        valid_to = cal_row['end_date']
        weekdays = {
            0: bool(cal_row['monday']), 1: bool(cal_row['tuesday']), 
            2: bool(cal_row['wednesday']), 3: bool(cal_row['thursday']), 
            4: bool(cal_row['friday']), 5: bool(cal_row['saturday']), 
            6: bool(cal_row['sunday'])
        }

        added_on = set()
        removed_on = set()
        
        if current_service_id in dfs.calendar_dates.index: # There might be no special dates for a trip
            special_dates = dfs.calendar_dates.loc[[current_service_id]] # We use [current_service_id] to ensure a dataframe is returned 
            added_on = set(special_dates[special_dates['exception_type'] == 1]['date'])
            removed_on = set(special_dates[special_dates['exception_type'] == 2]['date'])

        stops_in_trip = list(group.itertuples(index=False))
        origin_stop = None

        for i in range(len(stops_in_trip) - 1):
            if origin_stop is None:
                if stops_in_trip[i].pickup_type == 0:
                    origin_stop = stops_in_trip[i]
                else:
                    continue # Cant board here, skip to next

            next_stop = stops_in_trip[i+1]

            if next_stop.pickup_type == 0:
                u_id = child_to_parent[int(origin_stop.stop_id)]
                v_id = child_to_parent[int(next_stop.stop_id)]

                stops[u_id].add_trip(
                    next_id= v_id,
                    trip_id= int(trip_id),
                    departure=origin_stop.departure_time,
                    arrival=next_stop.arrival_time,
                    valid_from=valid_from,
                    valid_to=valid_to,
                    weekdays=weekdays,
                    added_on=added_on,
                    removed_on=removed_on
                )
                
                origin_stop = next_stop


def dijkstra_search(start_stop: int, end_stop: int, opt_criterium: str, start_time_secs: int, start_date: date) -> int:
    queue = [(start_time_secs, start_stop)]
    heapq.heapify(queue)
    best_time[start_stop] = start_time_secs

    while queue:
        time_secs, stop_id = heapq.heappop(queue)

        if stop_id == end_stop:
            break

        if best_time[stop_id] < time_secs:
            continue

        for neigh_id, edge in stops[stop_id].edges.items():
            edge_cost, trip_id = edge.cost(start_date, time_secs)
            new_time_secs = time_secs + edge_cost

            if best_time[neigh_id] > new_time_secs:
                best_time[neigh_id] = new_time_secs
                best_trip[neigh_id] = trip_id
                prev_stop[neigh_id] = stop_id
                heapq.heappush(queue, (new_time_secs, neigh_id))

    return best_time[end_stop] - start_time_secs


def astar_search(start_stop: int, end_stop: int, opt_criterium: str, start_time_secs: int, start_date: date) -> int:
    def f(stop_id: int) -> int:
        if opt_criterium == "t":
            return best_time[stop_id] + stops[stop_id].heuristic(stops[end_stop])
        else:
            return best_layovers[stop_id] + stops[stop_id].heuristic_layover(stops[end_stop])

    open = set()
    closed = set()
    open.add(start_stop)
    
    best_time[start_stop] = start_time_secs
    best_layovers[start_stop] = 0
    best_estimate[start_stop] = f(start_stop)

    while open:
        stop = -1
        stop_cost = INT_INF

        for s in open:
            if best_estimate[s] < stop_cost:
                stop = s
                stop_cost = best_estimate[s]

        if stop == end_stop:
            break
        
        open.remove(stop)
        closed.add(stop)

        for neigh_id, edge in stops[stop].edges.items():
            cost, trip_id = edge.cost(start_date, best_time[stop])
            layover = 0 if trip_id == best_trip[stop] else 1

            if not (neigh_id in open or neigh_id in closed):
                open.add(neigh_id)
                best_trip[neigh_id] = trip_id
                prev_stop[neigh_id] = stop
                best_time[neigh_id] = best_time[stop] + cost
                best_layovers[neigh_id] = best_layovers[stop] + layover
                best_estimate[neigh_id] = f(neigh_id)
            else:
                if best_time[neigh_id] > best_time[stop] + cost:
                    best_trip[neigh_id] = trip_id
                    prev_stop[neigh_id] = stop
                    best_time[neigh_id] = best_time[stop] + cost
                    best_layovers[neigh_id] = best_layovers[stop] + layover
                    best_estimate[neigh_id] = f(neigh_id)

                    if neigh_id in closed:
                        open.add(neigh_id)
                        closed.remove(neigh_id)

    return best_time[end_stop] - start_time_secs


def tabu_search_a(start_stop: int, end_stop: int, opt_criterium: str, start_time_secs: int, start_date: date) -> int:
    """
    Basic tabu search with no tabu list size limit
    """
    path, cost, tids = ts_get_initial(
        start_stop, end_stop, opt_criterium, start_time_secs, start_date)
    if not path:
        return INT_INF
 
    best_path, best_cost, best_tids = path[:], cost, tids[:]
    curr_path = path[:]
 
    tabu_set = set()
    no_improve = 0
 
    for _ in range(MAX_TS_ITER):
        neighbors = ts_get_neighbors(curr_path, tabu_set, start_time_secs, start_date)
        if not neighbors:
            break
 
        neighbors.sort(key=lambda x: x[0]) # Selecting neighbor with lowest cost
        n_cost, n_path, n_tids, n_move = neighbors[0]
 
        tabu_set.add(n_move)    # forbid this move permanently
        curr_path = n_path
 
        if n_cost < best_cost:
            best_cost, best_path, best_tids = n_cost, n_path[:], n_tids[:]
            no_improve = 0
        else:
            no_improve += 1
 
        if no_improve >= NO_IMPROVE_LIMIT:
            break
 
    fix_path(best_path, best_tids, start_time_secs, start_date)
    return best_cost


def tabu_search_b(start_stop: int, end_stop: int, opt_criterium: str, start_time_secs: int, start_date: date) -> int:
    """
    Tabu search with a dynamically sized tabu list, based on the amount of edges in current path. 
    At longer paths the search will remember more moves, so it doesn't re-explore long detours 
    """
    path, cost, tids = ts_get_initial(start_stop, end_stop, opt_criterium, start_time_secs, start_date)
    if not path:
        return INT_INF
 
    best_path, best_cost, best_tids = path[:], cost, tids[:]
    curr_path = path[:]
 
    tabu_deque = deque() # To remember which moves are the oldest
    tabu_set = set() # For O(1) lookups
    no_improve =0
 
    for _ in range(MAX_TS_ITER):
        L = len(curr_path) - 1
        max_t = max(1, (L + 1) // 2)
 
        while len(tabu_deque) > max_t: # Cutting out oldest moves if its necessary
            evicted = tabu_deque.popleft()
            tabu_set.discard(evicted)
 
        neighbors = ts_get_neighbors(curr_path, tabu_set, start_time_secs, start_date)

        if not neighbors:
            break
 
        neighbors.sort(key=lambda x: x[0])
        n_cost, n_path, n_tids, n_move = neighbors[0]
 
        tabu_deque.append(n_move)
        tabu_set.add(n_move)
        if len(tabu_deque) > max_t: # Cutting oldest moves again
            evicted =tabu_deque.popleft()
            tabu_set.discard(evicted)
 
        curr_path = n_path
 
        if n_cost < best_cost:
            best_cost, best_path, best_tids = n_cost, n_path[:], n_tids[:]
            no_improve = 0
        else:
            no_improve += 1
 
        if no_improve >= NO_IMPROVE_LIMIT:
            break
 
    fix_path(best_path, best_tids, start_time_secs, start_date)
    return best_cost


def tabu_search_c(start_stop: int, end_stop: int, opt_criterium: str, start_time_secs: int, start_date: date) -> int:
    """
    Tabu search with an aspiration criterium
    """
    path, cost, tids = ts_get_initial(start_stop, end_stop, opt_criterium, start_time_secs, start_date)
    if not path:
        return INT_INF
 
    best_path, best_cost, best_tids = path[:], cost, tids[:]
    curr_path = path[:]
 
    tabu_deque= deque()
    tabu_set = set()
    no_improve = 0
 
    for _ in range(MAX_TS_ITER):
        # Getting all neighbors, because all of them might meet aspiration criterium
        all_neighbors = ts_get_neighbors(curr_path, tabu_set=set(), start_time_secs=start_time_secs, start_date=start_date, skip_tabu=False)
        if not all_neighbors:
            break
 
        all_neighbors.sort(key=lambda x: x[0])
 
        chosen = None
        for n_cost, n_path, n_tids, n_move in all_neighbors:
            is_tabu = n_move in tabu_set
            if is_tabu:
                # If better than best soltuin, we break tabu. Else, we check next neighbor
                if n_cost < best_cost:
                    chosen = (n_cost, n_path, n_tids, n_move)
                    break
                continue

            # If none tabu moves get chosen, then first non-tabu is the best
            chosen = (n_cost, n_path, n_tids, n_move)
            break
 
        if chosen is None:
            break   # All moves are tabu and none pass aspiration
 
        n_cost, n_path, n_tids, n_move = chosen
 
        if len(tabu_deque) >= T_SIZE:
            evicted = tabu_deque.popleft()
            tabu_set.discard(evicted)
        tabu_deque.append(n_move)
        tabu_set.add(n_move)
 
        curr_path = n_path
 
        if n_cost < best_cost:
            best_cost, best_path, best_tids = n_cost, n_path[:], n_tids[:]
            no_improve = 0
        else:
            no_improve += 1
 
        if no_improve >= NO_IMPROVE_LIMIT:
            break
 
    fix_path(best_path, best_tids, start_time_secs, start_date)
    return best_cost


def tabu_search_d(start_stop: int, end_stop: int, opt_criterium: str, start_time_secs: int, start_date: date) -> int:
    """
    Tabu search where instead of calculating cost for all neighbors, we select a random sample blind, then calculate the cost for that sample
    """
    path, cost, tids = ts_get_initial(start_stop, end_stop, opt_criterium, start_time_secs, start_date)
    if not path:
        return INT_INF
 
    best_path, best_cost, best_tids = path[:], cost, tids[:]
    curr_path = path[:]
 
    tabu_deque = deque()
    tabu_set = set()
    no_improve = 0
 
    for _ in range(MAX_TS_ITER):
        path_set = set(curr_path)
 
        candidates= []

        # We calculate the neighbors withjout looking at cost to save computation
        for i in range(len(curr_path) - 1):
            u, v = curr_path[i], curr_path[i + 1]

            for w in stops[u].edges:
                if w not in path_set and v in stops[w].edges:
                    move = ('ins', u, v, w)

                    if move not in tabu_set:
                        candidates.append(('ins', i, w, move))
 
        for i in range(1, len(curr_path) - 1):
            u, w, v = curr_path[i - 1], curr_path[i], curr_path[i + 1]

            if v in stops[u].edges:
                move = ('rem', u, w, v)

                if move not in tabu_set:
                    candidates.append(('rem', i, None, move))
 
        if not candidates:
            break
 
        # Now we select a random sample of neighbors and only evaluate those
        sample = rnd.sample(candidates, min(TS_CANDIDATE_SELECT_COUNT, len(candidates)))
 
        evaluated = []
        for kind, idx, w, move in sample:
            if kind == 'ins':
                new_path = curr_path[: idx + 1] + [w] + curr_path[idx + 1:]
            else:
                new_path = curr_path[:idx] + curr_path[idx + 1:]
 
            n_cost, n_tids = ts_compute_cost(new_path, start_time_secs, start_date)

            if n_cost < INT_INF:
                evaluated.append((n_cost, new_path, n_tids, move))
 
        if not evaluated:
            no_improve += 1

            if no_improve >= NO_IMPROVE_LIMIT:
                break
            continue
 

        evaluated.sort(key=lambda x: x[0])
        n_cost, n_path, n_tids, n_move = evaluated[0]
 
        if len(tabu_deque) >= T_SIZE:
            evicted = tabu_deque.popleft()
            tabu_set.discard(evicted)
        tabu_deque.append(n_move)
        tabu_set.add(n_move)
 
        curr_path = n_path
 
        if n_cost < best_cost:
            best_cost, best_path, best_tids = n_cost, n_path[:], n_tids[:]
            no_improve = 0
        else:
            no_improve += 1
 
        if no_improve >= NO_IMPROVE_LIMIT:
            break
 
    fix_path(best_path, best_tids, start_time_secs, start_date)
    return best_cost


def run_alg(alg_type: str, start_stop: int, end_stop: int, opt_criterium: str, start_dt: datetime):
    start_date = start_dt.date()
    start_time_secs = time_to_seconds(start_dt.time())
    true_start = stops[start_stop].stop_id
    true_end = stops[end_stop].stop_id

    start = time.time()

    journey_time = -1

    # Chagne to switch case!!!!!
    if alg_type == "d":
        print("Running Dijkstra")
        journey_time = dijkstra_search(true_start, true_end, opt_criterium, start_time_secs, start_date)
    elif alg_type == "a":
        print("Running A*")
        journey_time = astar_search(true_start, true_end, opt_criterium, start_time_secs, start_date)
    elif alg_type == "ts_a":
        print("Running Tabu Search A")
        journey_time = tabu_search_a(true_start, true_end, opt_criterium, start_time_secs, start_date)
    elif alg_type == "ts_b":
        print("Running Tabu Search B")
        journey_time = tabu_search_b(true_start, true_end, opt_criterium, start_time_secs, start_date)
    elif alg_type == "ts_c":
        print("Running Tabu Search C")
        journey_time = tabu_search_c(true_start, true_end, opt_criterium, start_time_secs, start_date)
    elif alg_type == "ts_d":
        print("Running Tabu Search D")
        journey_time = tabu_search_d(true_start, true_end, opt_criterium, start_time_secs, start_date)
    else:
        print("Nonexsitent alg selected")
        return

    end = time.time()
    end_dt = start_dt + timedelta(seconds=journey_time)

    #reconstruct_path(true_end)
    print(f"Start time: {start_dt}")
    print(f"End time: {end_dt}")
    print(f"Journey time: {seconds_to_time(journey_time)}")
    print(f"Layovers: {best_layovers[true_end]}")
    print(f"Time to find shortest path: {end - start} ms")

    reset_dicts()


def ts_extract_path(end_stop: int) -> list[int]:
    path = []
    sid = end_stop
    while sid != -1:
        path.append(sid)
        sid = prev_stop[sid]
    path.reverse()
    return path if len(path) >= 2 else []


def ts_compute_cost(path: list[int], start_time_secs: int, start_date: date) -> tuple[int, list[int]]:
    if len(path) < 2:
        return 0, [-1] * len(path)
 
    t = start_time_secs
    trip_ids: list[int] = [-1]
    curr_trip = -1
 
    for i in range(len(path) - 1):
        u, v = path[i], path[i + 1]
        if v not in stops[u].edges:
            return INT_INF, []
        cost, tid = stops[u].edges[v].cost(start_date, t, curr_trip_id=curr_trip)
        if cost >= INT_INF:
            return INT_INF, []
        t += cost
        curr_trip = tid
        trip_ids.append(tid)
 
    return t - start_time_secs, trip_ids


def ts_get_initial(start_stop: int, end_stop: int, opt_criterium: str, start_time_secs: int, start_date: date) -> tuple[list[int], int, list[int]]:
    """
    We begin tabu search with an existing solution to refine it, not build one from scratch
    """
    reset_dicts()
    dijkstra_search(start_stop, end_stop, opt_criterium, start_time_secs, start_date)
    path = ts_extract_path(end_stop)
    if not path or path[0] != start_stop:
        return [], INT_INF, []
    cost, tids = ts_compute_cost(path, start_time_secs, start_date)

    return path, cost, tids


def ts_get_neighbors(path: list[int], tabu_set: set, start_time_secs: int, start_date: date, skip_tabu: bool = True) -> list[tuple]:
    """
    Generate path's neighborhood by trying two moves: inserting a stop between two existing stops (turn u -> v into u -> w -> v) and bypassing a stop thats between two other stops (turn u -> w -> v into u -> v)
    """
    neighbors: list[tuple] = []
    path_set = set(path)
 
    # Inserts
    for i in range(len(path)-1):
        u, v = path[i], path[i+1]
        for w in stops[u].edges:
            if w in path_set or v not in stops[w].edges: # dont want cycles, and w must connect to v
                continue

            move = ('ins', u, v, w)

            if skip_tabu and move in tabu_set:
                continue

            new_path = path[:i+1] + [w] + path[i+1:]
            cost, tids = ts_compute_cost(new_path, start_time_secs, start_date)

            if cost < INT_INF:
                neighbors.append((cost, new_path, tids, move))
 
    # Removes
    for i in range(1, len(path)-1):
        u, w, v = path[i-1], path[i], path[i+1]

        if v not in stops[u].edges:
            continue

        move = ('rem', u, w, v)

        if skip_tabu and move in tabu_set:
            continue

        new_path = path[:i] + path[i+1:]
        cost, tids = ts_compute_cost(new_path, start_time_secs, start_date)

        if cost < INT_INF:
            neighbors.append((cost, new_path, tids, move))
 
    return neighbors


def fix_path(path: list[int], trip_ids: list[int], start_time_secs: int, start_date: date,) -> None:
    t = start_time_secs
    prev_trip = -1
    layovers  = 0
 
    prev_stop[path[0]] = -1
    best_time[path[0]] = t
    best_trip[path[0]] = -1
    best_layovers[path[0]] = 0
 
    for i in range(1, len(path)):
        u, v = path[i-1], path[i]
        tid = trip_ids[i]
        cost, _ = stops[u].edges[v].cost(start_date, t, curr_trip_id=trip_ids[i-1])
        t += cost
 
        if tid != prev_trip: 
            layovers += 1
 
        prev_stop[v] = u
        best_trip[v] = tid
        best_time[v] = t
        best_layovers[v] = layovers
        prev_trip = tid


def reconstruct_path(end_stop: int):
    stop_id = end_stop
    journey = []
    used_trips = []
    
    while stop_id > -1:
        journey.append(stop_id)
        used_trips.append(best_trip[stop_id])
        stop_id = prev_stop[stop_id]
    
    journey.reverse()
    used_trips.reverse()

    for stop, trip in zip(journey, used_trips):
        print(f"Used {trip} to get to {stop}")


def reset_dicts() -> None:
    for sid in stops:
        best_time[sid] = INT_INF
        best_trip[sid] = -1
        prev_stop[sid] = -1
        best_layovers[sid] = INT_INF
        best_estimate[sid] = INT_INF

# Czy musimy zaWsze konwertować odpowiednio child_id przed dodaniem tripa???

def main():
    base_path = Path(__file__).resolve().parent
    dir_path = base_path / 'google_transit'
    initialize(dir_path)
    run_alg('d', 0, 10, "t", datetime(2026, 3, 1, 10, 30))
    run_alg('a', 0, 10, "t", datetime(2026, 3, 1, 10, 30))
    run_alg('a', 0, 10, "l", datetime(2026, 3, 1, 10, 30))
    run_alg('ts_a', 0, 10, "t", datetime(2026, 3, 1, 10, 30))
    run_alg('ts_b', 0, 10, "t", datetime(2026, 3, 1, 10, 30))
    run_alg('ts_c', 0, 10, "t", datetime(2026, 3, 1, 10, 30))
    run_alg('ts_d', 0, 10, "t", datetime(2026, 3, 1, 10, 30))

if __name__ == '__main__':
    main()