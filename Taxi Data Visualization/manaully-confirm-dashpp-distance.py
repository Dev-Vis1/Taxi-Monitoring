import math
from collections import defaultdict

def haversine(lat1, lon1, lat2, lon2):
    EARTH_RADIUS_KM = 6371.0
    lat1_rad, lon1_rad = math.radians(lat1), math.radians(lon1)
    lat2_rad, lon2_rad = math.radians(lat2), math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return EARTH_RADIUS_KM * c

def parse_line(line):
    parts = line.strip().split(',')
    if len(parts) != 4:
        return None
    taxi_id = parts[0]
    lat = float(parts[2])
    lon = float(parts[3])
    return taxi_id, lat, lon

def compute_total_distance(filename):
    taxi_points = defaultdict(list)
    with open(filename, 'r') as f:
        for line in f:
            parsed = parse_line(line)
            if parsed:
                taxi_id, lat, lon = parsed
                taxi_points[taxi_id].append((lat, lon))
    taxi_distances = {}
    for taxi_id, points in taxi_points.items():
        total = 0.0
        for i in range(1, len(points)):
            lat1, lon1 = points[i-1]
            lat2, lon2 = points[i]
            total += haversine(lat1, lon1, lat2, lon2)
        taxi_distances[taxi_id] = total
    return taxi_distances

if __name__ == "__main__":
    filename = "2.txt"  # Change path if needed
    distances = compute_total_distance(filename)
    for taxi_id, total_km in distances.items():
        print(f"Taxi