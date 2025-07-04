#!/usr/bin/env python3
"""
Calculate total speed and statistics for active taxis from Redis.
"""

import redis
import statistics
from datetime import datetime

def calculate_taxi_speed_stats():
    """Calculate speed statistics for all active taxis"""
    try:
        # Connect to Redis
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        
        # Test connection
        r.ping()
        print("✅ Connected to Redis successfully")
        
        # Get all speed metrics
        speed_data = r.hgetall('metrics:speed')
        
        if not speed_data:
            print("❌ No speed data found in Redis!")
            return
        
        # Convert to float values
        speeds = []
        for taxi_id, speed_str in speed_data.items():
            try:
                speed = float(speed_str)
                speeds.append(speed)
            except ValueError:
                print(f"⚠️  Invalid speed value for taxi {taxi_id}: {speed_str}")
                continue
        
        if not speeds:
            print("❌ No valid speed values found!")
            return
        
        # Calculate statistics
        total_speed = sum(speeds)
        avg_speed = statistics.mean(speeds)
        median_speed = statistics.median(speeds)
        max_speed = max(speeds)
        min_speed = min(speeds)
        
        # Count active taxis
        active_taxis = r.smembers('taxi:active')
        active_count = len(active_taxis)
        
        # Display results
        print(f"\n📊 Taxi Speed Statistics")
        print(f"{'='*50}")
        print(f"🚖 Total Active Taxis: {active_count}")
        print(f"📈 Taxis with Speed Data: {len(speeds)}")
        print(f"")
        print(f"🔢 TOTAL SPEED: {total_speed:.2f} km/h")
        print(f"📊 Average Speed: {avg_speed:.2f} km/h")
        print(f"📍 Median Speed: {median_speed:.2f} km/h")
        print(f"🚀 Maximum Speed: {max_speed:.2f} km/h")
        print(f"🐌 Minimum Speed: {min_speed:.2f} km/h")
        
        # Speed distribution
        stationary_taxis = len([s for s in speeds if s == 0.0])
        slow_taxis = len([s for s in speeds if 0 < s <= 10])
        normal_taxis = len([s for s in speeds if 10 < s <= 50])
        fast_taxis = len([s for s in speeds if s > 50])
        
        print(f"\n🚦 Speed Distribution:")
        print(f"  🛑 Stationary (0 km/h): {stationary_taxis} taxis")
        print(f"  🐌 Slow (0-10 km/h): {slow_taxis} taxis")
        print(f"  🚗 Normal (10-50 km/h): {normal_taxis} taxis")
        print(f"  🏎️  Fast (>50 km/h): {fast_taxis} taxis")
        
        # Find fastest taxis
        print(f"\n🏆 Top 5 Fastest Taxis:")
        speed_items = [(taxi_id, float(speed)) for taxi_id, speed in speed_data.items()]
        speed_items.sort(key=lambda x: x[1], reverse=True)
        
        for i, (taxi_id, speed) in enumerate(speed_items[:5]):
            print(f"  {i+1}. Taxi {taxi_id}: {speed:.2f} km/h")
        
        return {
            'total_speed': total_speed,
            'avg_speed': avg_speed,
            'active_count': active_count,
            'speed_count': len(speeds)
        }
        
    except redis.ConnectionError:
        print("❌ Cannot connect to Redis. Is it running?")
        return None
    except Exception as e:
        print(f"❌ Error calculating speed stats: {e}")
        return None

if __name__ == "__main__":
    print("🔍 Calculating taxi speed statistics...")
    stats = calculate_taxi_speed_stats()
    
    if stats:
        print(f"\n✅ Calculation completed successfully!")
        print(f"📋 Summary: {stats['speed_count']} taxis with total speed of {stats['total_speed']:.2f} km/h")
    else:
        print("\n❌ Speed calculation failed!")
