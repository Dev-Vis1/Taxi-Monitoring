# redis_client.py
import redis
import json

# Connect to Redis running in Docker
r = redis.Redis(host='redis', port=6379, decode_responses=True)

def get_all_taxi_ids():
    return [key.split(":")[1] for key in r.scan_iter("location:*")]

def get_latest_location(taxi_id):
    key = f"location:{taxi_id}"
    data = r.hgetall(key)
    if data and "lat" in data and "lon" in data:
        return {
            "latitude": float(data["lat"]),
            "longitude": float(data["lon"]),
            "timestamp": data.get("time", "")
        }
    return None  # Return None if data is invalid or missing

def get_route(taxi_id):
    key = f"route:{taxi_id}"
    route_points = r.lrange(key, 0, -1)
    if route_points:
        return [json.loads(point) for point in route_points]
    return []  # Return an empty list if no route data exists
