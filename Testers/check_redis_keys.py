#!/usr/bin/env python3
"""
Script to check Redis keys and values for the taxi tracking system.
"""

import redis
import json
import sys
from datetime import datetime

def check_redis_keys():
    """Check what keys are stored in Redis"""
    try:
        # Connect to Redis
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        
        # Test connection
        r.ping()
        print("✅ Connected to Redis successfully")
        
        # Get all keys
        all_keys = r.keys('*')
        print(f"\n📊 Total keys in Redis: {len(all_keys)}")
        
        if len(all_keys) == 0:
            print("❌ No keys found in Redis!")
            return
        
        # Categorize keys
        key_categories = {
            'location': [],
            'trajectory': [],
            'metrics': [],
            'taxi:active': [],
            'other': []
        }
        
        for key in all_keys:
            if key.startswith('location:'):
                key_categories['location'].append(key)
            elif key.startswith('trajectory:'):
                key_categories['trajectory'].append(key)
            elif key.startswith('metrics:'):
                key_categories['metrics'].append(key)
            elif key == 'taxi:active':
                key_categories['taxi:active'].append(key)
            else:
                key_categories['other'].append(key)
        
        # Display results
        print("\n📋 Key Categories:")
        for category, keys in key_categories.items():
            if keys:
                print(f"  {category}: {len(keys)} keys")
        
        # Show active taxis
        active_taxis = r.smembers('taxi:active')
        print(f"\n🚖 Active taxis: {len(active_taxis)}")
        if active_taxis:
            # Show first 10 active taxis
            sample_taxis = list(active_taxis)[:10]
            print(f"  Sample taxi IDs: {', '.join(sample_taxis)}")
            if len(active_taxis) > 10:
                print(f"  ... and {len(active_taxis) - 10} more")
        
        # Show sample location data
        if key_categories['location']:
            print(f"\n📍 Sample location data:")
            sample_location_key = key_categories['location'][0]
            location_data = r.hgetall(sample_location_key)
            print(f"  {sample_location_key}: {location_data}")
        
        # Show metrics
        if key_categories['metrics']:
            print(f"\n📈 Metrics keys:")
            for metric_key in key_categories['metrics']:
                metric_count = r.hlen(metric_key)
                print(f"  {metric_key}: {metric_count} entries")
        
        # Memory usage
        memory_info = r.info('memory')
        used_memory = memory_info.get('used_memory_human', 'N/A')
        print(f"\n💾 Redis memory usage: {used_memory}")
        
        return True
        
    except redis.ConnectionError:
        print("❌ Cannot connect to Redis. Is it running?")
        print("   Try: docker exec -it redis redis-cli ping")
        return False
    except Exception as e:
        print(f"❌ Error checking Redis: {e}")
        return False

def calculate_total_distance():
    """Calculate total distance covered by all active taxis"""
    try:
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        
        # Get all distance metrics
        distance_data = r.hgetall('metrics:distance')
        
        if not distance_data:
            print("❌ No distance data found in Redis!")
            return None
        
        # Convert to float values and calculate statistics
        distances = []
        total_distance = 0
        
        for taxi_id, distance_str in distance_data.items():
            try:
                distance = float(distance_str)
                distances.append(distance)
                total_distance += distance
            except ValueError:
                print(f"⚠️  Invalid distance value for taxi {taxi_id}: {distance_str}")
                continue
        
        if not distances:
            print("❌ No valid distance values found!")
            return None
        
        # Calculate statistics
        avg_distance = total_distance / len(distances)
        max_distance = max(distances)
        min_distance = min(distances)
        
        # Display results
        print(f"\n🛣️  Total Distance Statistics")
        print(f"{'='*50}")
        print(f"🚖 Taxis with Distance Data: {len(distances)}")
        print(f"")
        print(f"🔢 TOTAL DISTANCE: {total_distance:,.2f} km")
        print(f"📊 Average Distance per Taxi: {avg_distance:.2f} km")
        print(f"🏆 Maximum Distance: {max_distance:.2f} km")
        print(f"🚀 Minimum Distance: {min_distance:.2f} km")
        
        # Distance distribution
        short_trips = len([d for d in distances if d < 50])
        medium_trips = len([d for d in distances if 50 <= d < 200])
        long_trips = len([d for d in distances if d >= 200])
        
        print(f"\n📏 Distance Distribution:")
        print(f"  🚗 Short trips (<50 km): {short_trips} taxis")
        print(f"  🛣️  Medium trips (50-200 km): {medium_trips} taxis")
        print(f"  🚛 Long trips (≥200 km): {long_trips} taxis")
        
        # Find taxis with longest distances
        print(f"\n🏆 Top 5 Longest Distance Taxis:")
        distance_items = [(taxi_id, float(distance)) for taxi_id, distance in distance_data.items()]
        distance_items.sort(key=lambda x: x[1], reverse=True)
        
        for i, (taxi_id, distance) in enumerate(distance_items[:5]):
            print(f"  {i+1}. Taxi {taxi_id}: {distance:.2f} km")
        
        # Show details of the taxi with maximum distance
        if distance_items:
            max_taxi_id, max_distance = distance_items[0]
            print(f"\n🥇 Champion Taxi Details (Taxi {max_taxi_id}):")
            print(f"  🛣️  Total Distance: {max_distance:,.2f} km")
            
            # Get additional details
            location_data = r.hgetall(f"location:{max_taxi_id}")
            current_speed = r.hget('metrics:speed', max_taxi_id)
            avg_speed = r.hget('metrics:avgSpeed', max_taxi_id)
            
            if location_data:
                print(f"  📍 Current Location: {location_data.get('lat', 'N/A')}, {location_data.get('lon', 'N/A')}")
                print(f"  🕒 Last Update: {location_data.get('time', 'N/A')}")
            
            if current_speed:
                print(f"  🚗 Current Speed: {float(current_speed):.2f} km/h")
            if avg_speed:
                print(f"  📊 Average Speed: {float(avg_speed):.2f} km/h")
            
            # Fun facts
            print(f"  🌍 Distance Facts:")
            print(f"    • Equivalent to {max_distance / 40075:.1f} times around Earth")
            print(f"    • Could drive from Beijing to New York {max_distance / 11000:.1f} times")
            print(f"    • That's {max_distance / 1773:.1f} times the average taxi distance")
        
        return {
            'total_distance': total_distance,
            'avg_distance': avg_distance,
            'taxi_count': len(distances),
            'max_distance': max_distance,
            'min_distance': min_distance
        }
        
    except Exception as e:
        print(f"❌ Error calculating distance stats: {e}")
        return None

def detect_and_clean_anomalies():
    """Detect and optionally clean anomalous data points"""
    try:
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        
        print(f"\n🔍 Anomaly Detection & Data Validation")
        print(f"{'='*60}")
        
        # Define thresholds for anomaly detection
        MAX_REALISTIC_DISTANCE = 5000  # km - max realistic distance for a taxi
        MAX_REALISTIC_SPEED = 120      # km/h - max realistic speed in city
        MIN_REALISTIC_SPEED = 0        # km/h - minimum speed
        MAX_REALISTIC_AVG_SPEED = 80   # km/h - max realistic average speed
        
        anomalies_found = []
        
        # Check distance anomalies
        print(f"\n📏 Distance Anomaly Detection (threshold: {MAX_REALISTIC_DISTANCE} km)")
        distance_data = r.hgetall('metrics:distance')
        distance_anomalies = []
        
        for taxi_id, distance_str in distance_data.items():
            try:
                distance = float(distance_str)
                if distance > MAX_REALISTIC_DISTANCE:
                    distance_anomalies.append((taxi_id, distance))
                    anomalies_found.append(f"Distance: Taxi {taxi_id} = {distance:.2f} km")
            except ValueError:
                anomalies_found.append(f"Invalid distance: Taxi {taxi_id} = {distance_str}")
        
        if distance_anomalies:
            print(f"  ❌ Found {len(distance_anomalies)} distance anomalies:")
            for taxi_id, distance in sorted(distance_anomalies, key=lambda x: x[1], reverse=True):
                print(f"    • Taxi {taxi_id}: {distance:,.2f} km")
        else:
            print(f"  ✅ No distance anomalies found")
        
        # Check speed anomalies
        print(f"\n🚗 Speed Anomaly Detection (threshold: {MAX_REALISTIC_SPEED} km/h)")
        speed_data = r.hgetall('metrics:speed')
        speed_anomalies = []
        
        for taxi_id, speed_str in speed_data.items():
            try:
                speed = float(speed_str)
                if speed > MAX_REALISTIC_SPEED or speed < MIN_REALISTIC_SPEED:
                    speed_anomalies.append((taxi_id, speed))
                    anomalies_found.append(f"Speed: Taxi {taxi_id} = {speed:.2f} km/h")
            except ValueError:
                anomalies_found.append(f"Invalid speed: Taxi {taxi_id} = {speed_str}")
        
        if speed_anomalies:
            print(f"  ❌ Found {len(speed_anomalies)} speed anomalies:")
            for taxi_id, speed in sorted(speed_anomalies, key=lambda x: x[1], reverse=True):
                print(f"    • Taxi {taxi_id}: {speed:.2f} km/h")
        else:
            print(f"  ✅ No speed anomalies found")
        
        # Check average speed anomalies
        print(f"\n📊 Average Speed Anomaly Detection (threshold: {MAX_REALISTIC_AVG_SPEED} km/h)")
        avg_speed_data = r.hgetall('metrics:avgSpeed')
        avg_speed_anomalies = []
        
        for taxi_id, avg_speed_str in avg_speed_data.items():
            try:
                avg_speed = float(avg_speed_str)
                if avg_speed > MAX_REALISTIC_AVG_SPEED:
                    avg_speed_anomalies.append((taxi_id, avg_speed))
                    anomalies_found.append(f"Avg Speed: Taxi {taxi_id} = {avg_speed:.2f} km/h")
            except ValueError:
                anomalies_found.append(f"Invalid avg speed: Taxi {taxi_id} = {avg_speed_str}")
        
        if avg_speed_anomalies:
            print(f"  ❌ Found {len(avg_speed_anomalies)} average speed anomalies:")
            for taxi_id, avg_speed in sorted(avg_speed_anomalies, key=lambda x: x[1], reverse=True):
                print(f"    • Taxi {taxi_id}: {avg_speed:.2f} km/h")
        else:
            print(f"  ✅ No average speed anomalies found")
        
        # Location anomalies (outside reasonable Beijing area)
        print(f"\n📍 Location Anomaly Detection (Beijing area: 39.4-40.4 lat, 115.8-117.4 lon)")
        location_anomalies = []
        
        # Get a sample of location keys to check
        location_keys = r.keys('location:*')[:50]  # Check first 50 for performance
        
        for location_key in location_keys:
            location_data = r.hgetall(location_key)
            if location_data:
                try:
                    lat = float(location_data.get('lat', 0))
                    lon = float(location_data.get('lon', 0))
                    taxi_id = location_key.split(':')[1]
                    
                    # Check if outside reasonable Beijing area
                    if not (39.4 <= lat <= 40.4 and 115.8 <= lon <= 117.4):
                        location_anomalies.append((taxi_id, lat, lon))
                        anomalies_found.append(f"Location: Taxi {taxi_id} = ({lat}, {lon})")
                except ValueError:
                    taxi_id = location_key.split(':')[1]
                    anomalies_found.append(f"Invalid location: Taxi {taxi_id}")
        
        if location_anomalies:
            print(f"  ❌ Found {len(location_anomalies)} location anomalies (sample):")
            for taxi_id, lat, lon in location_anomalies[:5]:
                print(f"    • Taxi {taxi_id}: ({lat:.6f}, {lon:.6f})")
        else:
            print(f"  ✅ No location anomalies found in sample")
        
        # Summary
        print(f"\n📋 Anomaly Summary:")
        print(f"  Distance anomalies: {len(distance_anomalies)}")
        print(f"  Speed anomalies: {len(speed_anomalies)}")
        print(f"  Avg speed anomalies: {len(avg_speed_anomalies)}")
        print(f"  Location anomalies: {len(location_anomalies)} (sample)")
        print(f"  Total anomalies: {len(anomalies_found)}")
        
        return {
            'distance_anomalies': distance_anomalies,
            'speed_anomalies': speed_anomalies,
            'avg_speed_anomalies': avg_speed_anomalies,
            'location_anomalies': location_anomalies,
            'total_anomalies': len(anomalies_found)
        }
        
    except Exception as e:
        print(f"❌ Error detecting anomalies: {e}")
        return None

def show_taxi_details(taxi_id):
    """Show detailed information for a specific taxi"""
    try:
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        
        print(f"\n🚖 Details for Taxi {taxi_id}:")
        
        # Location data
        location_key = f"location:{taxi_id}"
        location_data = r.hgetall(location_key)
        if location_data:
            print(f"  📍 Current location: {location_data}")
        else:
            print(f"  📍 No location data found")
        
        # Trajectory data
        trajectory_key = f"trajectory:{taxi_id}"
        trajectory_data = r.lrange(trajectory_key, 0, -1)
        if trajectory_data:
            print(f"  🛣️  Trajectory points: {len(trajectory_data)}")
            for i, point in enumerate(trajectory_data):
                print(f"    {i+1}. {point}")
        else:
            print(f"  🛣️  No trajectory data found")
        
        # Metrics
        speed = r.hget('metrics:speed', taxi_id)
        avg_speed = r.hget('metrics:avgSpeed', taxi_id)
        distance = r.hget('metrics:distance', taxi_id)
        
        print(f"  📈 Metrics:")
        print(f"    Speed: {speed if speed else 'N/A'} km/h")
        print(f"    Avg Speed: {avg_speed if avg_speed else 'N/A'} km/h")
        print(f"    Distance: {distance if distance else 'N/A'} km")
        
    except Exception as e:
        print(f"❌ Error getting taxi details: {e}")

if __name__ == "__main__":
    print("🔍 Checking Redis keys for taxi tracking system...")
    
    if check_redis_keys():
        print("\n✅ Redis check completed successfully!")
        
        # Calculate total distance
        print("\n" + "="*60)
        calculate_total_distance()
        
        # Detect anomalies
        print("\n" + "="*60)
        detect_and_clean_anomalies()
        
        # If a taxi ID is provided as argument, show details
        if len(sys.argv) > 1:
            taxi_id = sys.argv[1]
            show_taxi_details(taxi_id)
    else:
        print("\n❌ Redis check failed!")
        sys.exit(1)
