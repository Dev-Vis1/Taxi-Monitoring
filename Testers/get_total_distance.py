#!/usr/bin/env python3
"""
Quick script to get total distance covered by all active taxis.
"""

import redis

def get_total_distance():
    """Get total distance covered by all active taxis"""
    try:
        # Connect to Redis
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        
        # Test connection
        r.ping()
        print("✅ Connected to Redis successfully")
        
        # Get all distance metrics
        distance_data = r.hgetall('metrics:distance')
        
        if not distance_data:
            print("❌ No distance data found in Redis!")
            return
        
        # Calculate total distance
        total_distance = 0
        valid_count = 0
        
        for taxi_id, distance_str in distance_data.items():
            try:
                distance = float(distance_str)
                total_distance += distance
                valid_count += 1
            except ValueError:
                print(f"⚠️  Invalid distance value for taxi {taxi_id}: {distance_str}")
                continue
        
        # Get active taxi count
        active_taxis = r.smembers('taxi:active')
        active_count = len(active_taxis)
        
        # Display results
        print(f"\n🛣️  TOTAL DISTANCE SUMMARY")
        print(f"{'='*50}")
        print(f"🚖 Active Taxis: {active_count}")
        print(f"📊 Taxis with Distance Data: {valid_count}")
        print(f"")
        print(f"🔢 TOTAL DISTANCE: {total_distance:,.2f} km")
        print(f"📊 Average Distance per Taxi: {total_distance/valid_count:.2f} km")
        
        # Convert to other units for perspective
        print(f"\n📏 Distance in Other Units:")
        print(f"  🌍 Total in miles: {total_distance * 0.621371:,.2f} miles")
        print(f"  🌍 Total in meters: {total_distance * 1000:,.0f} meters")
        print(f"  🌍 Equivalent to ~{total_distance / 40075:.1f} times around Earth's equator")
        
        return total_distance
        
    except redis.ConnectionError:
        print("❌ Cannot connect to Redis. Is it running?")
        return None
    except Exception as e:
        print(f"❌ Error getting distance data: {e}")
        return None

if __name__ == "__main__":
    print("🔍 Getting total distance covered by active taxis...")
    
    total = get_total_distance()
    
    if total is not None:
        print(f"\n✅ Total distance retrieved: {total:,.2f} km")
    else:
        print("\n❌ Failed to get total distance!")
