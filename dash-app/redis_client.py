# redis_client.py - OPTIMIZED FOR HIGH THROUGHPUT
import redis
import json
import msgpack
from datetime import datetime, timedelta
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Optimized Redis connection with ultra-high-performance connection pool
pool = redis.ConnectionPool(
    host='redis', 
    port=6379, 
    db=0, 
    max_connections=150,  # Increased for ultra-high throughput
    decode_responses=True,
    socket_timeout=3,     # Further reduced for faster timeouts
    socket_connect_timeout=3,
    retry_on_timeout=True,
    health_check_interval=30
)
r = redis.Redis(connection_pool=pool)

# Simulation state - Optimized for real-time streaming
simulation_start_time = None
simulation_speed = 2  # Seconds between each simulation step
current_step = 0

# Enhanced caching system
taxi_data_cache = {}
taxi_route_cache = {}
taxi_ids_cache = None
taxi_ids_cache_time = 0
cache_expiry = 60  # Reduced to 1 minute for more frequent updates

def get_all_taxi_ids():
    """Get all taxi IDs using Redis ZRANGEBYSCORE for active taxis (sorted set)"""
    global taxi_ids_cache, taxi_ids_cache_time
    
    current_time = time.time()
    if taxi_ids_cache is None or (current_time - taxi_ids_cache_time) > cache_expiry:
        try:
            # Use Redis sorted set for active taxis - get only recent ones (last 5 minutes)
            time_threshold = current_time - 300  # 5 minutes ago
            taxi_ids_cache = r.zrangebyscore('taxi:active', time_threshold, current_time)
            taxi_ids_cache_time = current_time
            logger.info(f"Refreshed taxi cache: {len(taxi_ids_cache)} active taxis (last 5 minutes)")
        except Exception as e:
            logger.error(f"Error fetching taxi IDs: {e}")
            if taxi_ids_cache is None:
                taxi_ids_cache = []
    
    return taxi_ids_cache

def get_latest_location(taxi_id):
    """Get latest location with enhanced error handling and trajectory support"""
    key = f"location:{taxi_id}"
    try:
        # Use pipeline for multiple operations
        pipe = r.pipeline()
        pipe.hgetall(key)
        pipe.hget("metrics:speed", taxi_id)
        # Also get recent trajectory points
        pipe.lrange(f"trajectory:{taxi_id}", 0, 4)  # Get last 5 positions
        results = pipe.execute()
        
        data = results[0]
        speed_data = results[1]
        trajectory_data = results[2]
        
        if data and "lat" in data and "lon" in data:
            # Parse speed safely
            speed = 0.0
            if speed_data:
                try:
                    speed = float(speed_data)
                except (ValueError, TypeError):
                    speed = 0.0
            
            # Parse trajectory for smooth movement
            trajectory = []
            if trajectory_data:
                for traj_point in trajectory_data:
                    try:
                        import json
                        trajectory.append(json.loads(traj_point))
                    except:
                        continue
            
            # FIXED: Redis stores lat/lon correctly, don't swap them
            location_data = {
                "latitude": float(data["lat"]),   # lat is latitude
                "longitude": float(data["lon"]),  # lon is longitude  
                "timestamp": data.get("time", ""),
                "speed": speed,
                "trajectory": trajectory  # Add trajectory for smooth movement
            }
            
            return location_data
    except Exception as e:
        logger.error(f"Error fetching location for taxi {taxi_id}: {e}")
    
    return None

def get_taxi_speed(taxi_id):
    """Get current speed from Redis metrics"""
    speed_data = r.hget("metrics:speed", taxi_id)
    if speed_data:
        try:
            return float(speed_data)
        except (ValueError, TypeError):
            return 0.0
    return 0.0

def get_taxi_average_speed(taxi_id):
    """Get average speed from Redis metrics"""
    avg_speed_data = r.hget("metrics:avgSpeed", taxi_id)
    if avg_speed_data:
        try:
            return float(avg_speed_data)
        except (ValueError, TypeError):
            return 0.0
    return 0.0

def get_taxi_distance(taxi_id):
    """Get total distance from Redis metrics"""
    try:
        distance_data = r.hget("metrics:distance", taxi_id)
        if distance_data:
            return float(distance_data)
    except (ValueError, TypeError, Exception) as e:
        logger.error(f"Error fetching distance for taxi {taxi_id}: {e}")
    return 0.0

def get_all_taxi_distances():
    """Get total distances for all CURRENTLY ACTIVE taxis only"""
    try:
        # Get currently active taxi IDs (those with current location data)
        current_taxi_ids = get_all_taxi_ids()
        if not current_taxi_ids:
            return 0.0
        
        # Get all distance data from Redis
        all_distances = r.hgetall("metrics:distance")
        total_distance = 0.0
        active_distance_entries = 0
        excluded_taxis = []
        
        for taxi_id, distance_str in all_distances.items():
            # Only include distance for currently active taxis
            if taxi_id in current_taxi_ids:
                try:
                    distance = float(distance_str)
                    total_distance += distance
                    active_distance_entries += 1
                except (ValueError, TypeError):
                    continue
            else:
                excluded_taxis.append(taxi_id)
        
        if excluded_taxis:
            logger.info(f"Excluded distance from inactive taxis: {excluded_taxis}")
        
        logger.info(f"Total distance calculation: {active_distance_entries} active taxis, "
                   f"{len(all_distances)} total distance entries, "
                   f"total distance: {total_distance:.2f} km")
        
        return total_distance
    except Exception as e:
        logger.error(f"Error fetching all taxi distances: {e}")
        return 0.0

def get_route(taxi_id):
    key = f"route:{taxi_id}"
    route_points = r.lrange(key, 0, -1)  # Get all route points
    if route_points:
        try:
            parsed_points = [json.loads(point) for point in route_points]
            # Sort by timestamp to ensure chronological order
            parsed_points.sort(key=lambda x: x.get('timestamp', ''))
            return parsed_points
        except json.JSONDecodeError as e:
            print(f"Error parsing route data for {taxi_id}: {e}")
            return []
    return []

def get_all_sorted_timestamps():
    """Get all unique timestamps from the data, sorted chronologically"""
    global all_sorted_timestamps
    
    if all_sorted_timestamps:
        return all_sorted_timestamps
    
    all_timestamps = set()
    all_taxi_ids = get_all_taxi_ids()
    
    for taxi_id in all_taxi_ids:
        route_data = get_route(taxi_id)
        for position in route_data:
            if 'timestamp' in position:
                all_timestamps.add(position['timestamp'])
    
    all_sorted_timestamps = sorted(list(all_timestamps))
    print(f"Found {len(all_sorted_timestamps)} unique timestamps")
    if all_sorted_timestamps:
        print(f"First timestamp: {all_sorted_timestamps[0]}")
        print(f"Last timestamp: {all_sorted_timestamps[-1]}")
    
    return all_sorted_timestamps

def get_current_data_timestamp():
    """Get the current timestamp based on stepping through data points every few seconds"""
    global simulation_start_time, current_timestamp_index
    
    timestamps = get_all_sorted_timestamps()
    if not timestamps:
        return datetime.now()
    
    if simulation_start_time is None:
        simulation_start_time = time.time()
        current_timestamp_index = 0
    
    # Calculate which timestamp we should be at based on elapsed time
    elapsed_real_time = time.time() - simulation_start_time
    target_index = int(elapsed_real_time / simulation_speed) % len(timestamps)
    
    current_timestamp_index = target_index
    current_timestamp_str = timestamps[current_timestamp_index]
    
    try:
        return datetime.strptime(current_timestamp_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return datetime.now()

def get_current_simulation_time():
    """Get current simulation time based on stepping through actual data timestamps"""
    return get_current_data_timestamp()

def get_taxi_position_at_timestamp(taxi_id, target_timestamp_str):
    """Get taxi position for a specific timestamp string - only if taxi is active at this time"""
    route_data = get_route(taxi_id)
    if not route_data:
        return None
    
    # Look for exact timestamp match first
    for position in route_data:
        if position.get('timestamp') == target_timestamp_str:
            return position
    
    # If no exact match, find the closest timestamp within a reasonable time window
    closest_position = None
    min_time_diff = float('inf')
    max_time_window = 3600  # Only show taxi if it has data within 1 hour of target time
    
    try:
        target_time = datetime.strptime(target_timestamp_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None
    
    for position in route_data:
        if 'timestamp' not in position:
            continue
        try:
            pos_time = datetime.strptime(position['timestamp'], '%Y-%m-%d %H:%M:%S')
            time_diff = abs((pos_time - target_time).total_seconds())
            
            # Only consider positions within the time window
            if time_diff <= max_time_window and time_diff < min_time_diff:
                min_time_diff = time_diff
                closest_position = position
        except (ValueError, TypeError):
            continue
    
    return closest_position

def get_active_taxis_at_timestamp(target_timestamp_str, max_taxis=1000):
    """Get all active taxis at a specific timestamp - optimized for large numbers with real speed data"""
    all_taxi_ids = get_all_taxi_ids()
    active_taxis = {}
    
    # Limit the number of taxis to avoid performance issues
    limited_taxi_ids = all_taxi_ids[:max_taxis]
    
    for taxi_id in limited_taxi_ids:
        position = get_taxi_position_at_timestamp(taxi_id, target_timestamp_str)
        if position:  # Only include taxis that are active at this time
            # Get real speed from Redis metrics (calculated by Flink)
            speed = get_taxi_speed(taxi_id)
            active_taxis[taxi_id] = {
                'lat': position.get('latitude', 0),
                'lng': position.get('longitude', 0),
                'speed': speed,  # Real speed from Flink processing
                'timestamp': position.get('timestamp', '')
            }
    
    return active_taxis

def get_taxi_clusters(active_taxis, cluster_distance=0.01):
    """Group nearby taxis into clusters to reduce rendering load"""
    clusters = []
    processed = set()
    
    for taxi_id, data in active_taxis.items():
        if taxi_id in processed:
            continue
            
        cluster = {
            'center_lat': data['lat'],
            'center_lng': data['lng'],
            'taxi_count': 1,
            'avg_speed': data['speed'],
            'taxi_ids': [taxi_id]
        }
        processed.add(taxi_id)
        
        # Find nearby taxis
        for other_taxi_id, other_data in active_taxis.items():
            if other_taxi_id in processed:
                continue
                
            # Simple distance calculation
            lat_diff = abs(data['lat'] - other_data['lat'])
            lng_diff = abs(data['lng'] - other_data['lng'])
            
            if lat_diff < cluster_distance and lng_diff < cluster_distance:
                cluster['taxi_count'] += 1
                cluster['avg_speed'] = (cluster['avg_speed'] + other_data['speed']) / 2
                cluster['taxi_ids'].append(other_taxi_id)
                processed.add(other_taxi_id)
        
        clusters.append(cluster)
    
    return clusters

def calculate_speed(lat1, lon1, lat2, lon2, time1, time2):
    """
    Calculate speed between two GPS points using Haversine formula
    Returns speed in km/h
    """
    from math import radians, cos, sin, asin, sqrt
    
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers
    distance = c * r
    
    # Calculate time difference in hours
    try:
        t1 = datetime.strptime(time1, '%Y-%m-%d %H:%M:%S')
        t2 = datetime.strptime(time2, '%Y-%m-%d %H:%M:%S')
        time_diff = abs((t2 - t1).total_seconds()) / 3600  # Convert to hours
        
        if time_diff > 0:
            speed = distance / time_diff  # km/h
            return min(speed, 200)  # Cap at reasonable speed (200 km/h)
        else:
            return 0
    except (ValueError, ZeroDivisionError):
        return 0

def get_taxi_relative_position(taxi_id, simulation_step):
    """
    Get taxi position based on relative simulation step.
    Each taxi follows its own timeline from start to finish.
    This ensures all taxis are always moving regardless of their absolute timestamps.
    Uses real speed data from Flink processing.
    """
    route_data = get_route(taxi_id)
    if not route_data:
        return None
    
    # Calculate which data point to show based on simulation step
    total_points = len(route_data)
    if total_points == 0:
        return None
    
    # Use modulo to cycle through the route continuously
    current_index = simulation_step % total_points
    
    position = route_data[current_index]
    
    # Get real speed from Redis metrics (calculated by Flink using Haversine formula)
    speed = get_taxi_speed(taxi_id)
    
    return {
        'latitude': position.get('latitude', 0),
        'longitude': position.get('longitude', 0),
        'speed': speed,  # Real speed from Flink processing
        'timestamp': position.get('timestamp', ''),
        'route_progress': (current_index / total_points) * 100  # Progress through route
    }

def get_all_active_taxis_relative(max_taxis=1000):
    """
    Get all active taxis with their current positions using relative timeline.
    Optimized for high throughput processing.
    """
    global current_step
    
    all_taxi_ids = get_all_taxi_ids()
    # Limit number of taxis for performance, but show more for demonstration
    if len(all_taxi_ids) > max_taxis:
        all_taxi_ids = all_taxi_ids[:max_taxis]
    
    active_taxis = {}
    
    for taxi_id in all_taxi_ids:
        position = get_taxi_relative_position(taxi_id, current_step)
        if position:
            active_taxis[taxi_id] = {
                'lat': position['latitude'],
                'lng': position['longitude'], 
                'speed': position['speed'],
                'timestamp': position['timestamp'],
                'progress': position['route_progress']
            }
    
    return active_taxis

def advance_simulation_step():
    """Advance the simulation by one step (called every few seconds)"""
    global current_step, simulation_start_time
    
    if simulation_start_time is None:
        simulation_start_time = time.time()
        current_step = 0
    else:
        current_step += 1
    
    return current_step

def get_current_simulation_step():
    """Get the current simulation step"""
    global simulation_start_time, current_step
    
    if simulation_start_time is None:
        simulation_start_time = time.time()
        current_step = 0
        return current_step
    
    # Auto-advance step based on elapsed time
    elapsed_time = time.time() - simulation_start_time
    target_step = int(elapsed_time / simulation_speed)
    
    if target_step > current_step:
        current_step = target_step
    
    return current_step

def cleanup_stale_distance_data():
    """Clean up distance data for taxis that are no longer active"""
    try:
        # Get currently active taxi IDs
        current_taxi_ids = set(get_all_taxi_ids())
        if not current_taxi_ids:
            return 0
        
        # Get all distance entries
        all_distance_entries = r.hgetall("metrics:distance")
        stale_entries = []
        
        for taxi_id in all_distance_entries.keys():
            if taxi_id not in current_taxi_ids:
                stale_entries.append(taxi_id)
        
        # Remove stale entries
        if stale_entries:
            r.hdel("metrics:distance", *stale_entries)
            logger.info(f"Cleaned up {len(stale_entries)} stale distance entries: {stale_entries}")
        
        return len(stale_entries)
    except Exception as e:
        logger.error(f"Error cleaning up stale distance data: {e}")
        return 0

def get_recently_active_taxi_ids(time_threshold_minutes=5):
    """Get taxi IDs that have been updated recently (indicating current simulation activity)"""
    try:
        from datetime import datetime, timedelta
        
        current_time = datetime.now()
        threshold_time = current_time - timedelta(minutes=time_threshold_minutes)
        
        # Get all taxi location keys
        keys = []
        cursor = 0
        while True:
            cursor, partial_keys = r.scan(cursor=cursor, match="location:*", count=10000)
            keys.extend(partial_keys)
            if cursor == 0:
                break
        
        recent_taxi_ids = []
        for key in keys:
            if key.startswith("location:"):
                taxi_id = key.split(":")[1]
                # Get the timestamp for this taxi
                taxi_data = r.hgetall(key)
                if taxi_data and 'time' in taxi_data:
                    try:
                        # Parse the timestamp
                        taxi_time_str = taxi_data['time']
                        # Handle different timestamp formats
                        try:
                            taxi_time = datetime.strptime(taxi_time_str, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            try:
                                taxi_time = datetime.strptime(taxi_time_str, '%H:%M:%S')
                                # For time-only format, assume today's date
                                taxi_time = current_time.replace(hour=taxi_time.hour, 
                                                               minute=taxi_time.minute, 
                                                               second=taxi_time.second)
                            except ValueError:
                                # Skip if we can't parse the timestamp
                                continue
                        
                        # Check if it's recent (within threshold)
                        if taxi_time >= threshold_time:
                            recent_taxi_ids.append(taxi_id)
                    except Exception:
                        # Skip if we can't parse the timestamp
                        continue
        
        logger.info(f"Found {len(recent_taxi_ids)} recently active taxis (last {time_threshold_minutes} minutes)")
        return recent_taxi_ids
    except Exception as e:
        logger.error(f"Error fetching recently active taxi IDs: {e}")
        return []

def get_batch_locations(taxi_ids, batch_size=1000):
    """ULTRA-HIGH-PERFORMANCE: Get locations for multiple taxis using optimized Redis pipeline"""
    results = {}
    
    # Adaptive batch sizing for maximum performance
    if len(taxi_ids) > 5000:
        batch_size = 2000  # Very large batches for huge datasets
    elif len(taxi_ids) > 2000:
        batch_size = 1000  # Large batches for huge datasets
    elif len(taxi_ids) > 500:
        batch_size = 500   # Medium batches
    else:
        batch_size = min(batch_size, len(taxi_ids))
    
    # Process in optimized batches
    for i in range(0, len(taxi_ids), batch_size):
        batch_taxi_ids = taxi_ids[i:i + batch_size]
        
        # Use pipeline for ultra-fast batch operations
        pipe = r.pipeline()
        for taxi_id in batch_taxi_ids:
            pipe.hmget(f"location:{taxi_id}", ["lat", "lon", "time"])
            pipe.hget("metrics:speed", taxi_id)
        
        try:
            pipe_results = pipe.execute()
            
            # Process results in groups of 2 (location, speed) for maximum speed
            for j in range(0, len(pipe_results), 2):
                taxi_idx = j // 2
                if taxi_idx < len(batch_taxi_ids):
                    taxi_id = batch_taxi_ids[taxi_idx]
                    location_data = pipe_results[j]
                    speed_data = pipe_results[j + 1]
                    
                    if location_data and location_data[0] and location_data[1]:
                        try:
                            results[taxi_id] = {
                                'latitude': float(location_data[0]),
                                'longitude': float(location_data[1]),
                                'timestamp': location_data[2] or '',
                                'speed': float(speed_data) if speed_data else 0.0
                            }
                        except (ValueError, TypeError):
                            continue
        except Exception as e:
            logger.error(f"Error in ultra-fast batch location fetch: {e}")
            continue
    
    logger.info(f"Ultra-fast batch fetch: {len(results)}/{len(taxi_ids)} locations in {len(taxi_ids)//batch_size + 1} batches")
    return results

def check_area_violations_bulk(taxi_locations):
    """Check area violations for multiple taxis using Redis GEO operations"""
    try:
        # Use Redis GEORADIUS for efficient area checking
        in_area_taxis = r.georadius(
            'taxi_coords', 
            MONITORED_AREA['center_lon'],
            MONITORED_AREA['center_lat'],
            MONITORED_AREA['max_radius_km'],
            'km',
            withdist=True
        )
        
        # Convert to set for fast lookup
        in_area_set = {taxi_id for taxi_id, _ in in_area_taxis}
        
        violations = []
        for taxi_id, location in taxi_locations.items():
            if taxi_id not in in_area_set:
                # Calculate distance from center for the violation report
                import math
                lat1, lon1 = location['latitude'], location['longitude']
                lat2, lon2 = MONITORED_AREA['center_lat'], MONITORED_AREA['center_lon']
                
                # Haversine formula to calculate distance
                lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
                dlat = lat2 - lat1
                dlon = lon2 - lon1
                a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
                c = 2 * math.asin(math.sqrt(a))
                distance_km = 6371.0 * c  # Earth radius in km
                
                violations.append({
                    'taxi_id': taxi_id,
                    'type': 'Area Violation',
                    'lat': location['latitude'],
                    'lng': location['longitude'],
                    'timestamp': location['timestamp'],
                    'distance_from_center': distance_km
                })
        
        return violations
    except Exception as e:
        logger.error(f"Error checking area violations: {e}")
        return []

# Add constants for monitored area (matching Flink configuration)
MONITORED_AREA = {
    'center_lat': 39.9163,
    'center_lon': 116.3972,
    'warning_radius_km': 10.0,
    'max_radius_km': 15.0
}