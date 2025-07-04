#!/usr/bin/env python3
"""
Data Validation and Anomaly Detection System for Taxi Tracking
"""

import redis
import json
import sys
import statistics
from datetime import datetime, timedelta

class TaxiDataValidator:
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.r = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
        
        # Define validation thresholds
        self.VALIDATION_RULES = {
            'distance': {
                'max_total_distance': 5000,      # km - max realistic total distance
                'max_segment_distance': 50,      # km - max distance between two points
                'min_segment_distance': 0.001,   # km - minimum meaningful distance
            },
            'speed': {
                'max_speed': 120,                # km/h - max realistic speed
                'min_speed': 0,                  # km/h - minimum speed
                'max_avg_speed': 80,             # km/h - max realistic average speed
                'max_acceleration': 50,          # km/h/s - max realistic acceleration
            },
            'location': {
                'beijing_bounds': {
                    'min_lat': 39.4, 'max_lat': 40.4,
                    'min_lon': 115.8, 'max_lon': 117.4
                },
                'forbidden_city': {
                    'lat': 39.9163, 'lon': 116.3972,
                    'max_radius': 15  # km - your tracking radius
                }
            },
            'time': {
                'max_time_gap': 3600,            # seconds - max gap between updates
                'min_time_gap': 1,               # seconds - minimum gap between updates
            }
        }
    
    def validate_location(self, lat, lon):
        """Validate if location is within reasonable bounds"""
        bounds = self.VALIDATION_RULES['location']['beijing_bounds']
        
        # Check if within Beijing area
        if not (bounds['min_lat'] <= lat <= bounds['max_lat'] and 
                bounds['min_lon'] <= lon <= bounds['max_lon']):
            return False, f"Location outside Beijing bounds: ({lat:.6f}, {lon:.6f})"
        
        return True, "Valid location"
    
    def validate_speed(self, speed, avg_speed=None):
        """Validate speed values"""
        rules = self.VALIDATION_RULES['speed']
        
        if speed < rules['min_speed']:
            return False, f"Speed too low: {speed} km/h"
        
        if speed > rules['max_speed']:
            return False, f"Speed too high: {speed} km/h"
        
        if avg_speed and avg_speed > rules['max_avg_speed']:
            return False, f"Average speed too high: {avg_speed} km/h"
        
        return True, "Valid speed"
    
    def validate_distance(self, total_distance, segment_distance=None):
        """Validate distance values"""
        rules = self.VALIDATION_RULES['distance']
        
        if total_distance > rules['max_total_distance']:
            return False, f"Total distance too high: {total_distance} km"
        
        if segment_distance:
            if segment_distance > rules['max_segment_distance']:
                return False, f"Segment distance too high: {segment_distance} km"
            
            if segment_distance < rules['min_segment_distance']:
                return False, f"Segment distance too low: {segment_distance} km"
        
        return True, "Valid distance"
    
    def detect_all_anomalies(self):
        """Detect all anomalies in the current Redis data"""
        anomalies = {
            'distance': [],
            'speed': [],
            'location': [],
            'invalid_data': []
        }
        
        print("🔍 Scanning Redis data for anomalies...")
        
        # Distance anomalies
        distance_data = self.r.hgetall('metrics:distance')
        for taxi_id, distance_str in distance_data.items():
            try:
                distance = float(distance_str)
                is_valid, message = self.validate_distance(distance)
                if not is_valid:
                    anomalies['distance'].append({
                        'taxi_id': taxi_id,
                        'value': distance,
                        'reason': message
                    })
            except ValueError:
                anomalies['invalid_data'].append({
                    'taxi_id': taxi_id,
                    'field': 'distance',
                    'value': distance_str,
                    'reason': 'Invalid number format'
                })
        
        # Speed anomalies
        speed_data = self.r.hgetall('metrics:speed')
        avg_speed_data = self.r.hgetall('metrics:avgSpeed')
        
        for taxi_id, speed_str in speed_data.items():
            try:
                speed = float(speed_str)
                avg_speed = float(avg_speed_data.get(taxi_id, 0))
                
                is_valid, message = self.validate_speed(speed, avg_speed)
                if not is_valid:
                    anomalies['speed'].append({
                        'taxi_id': taxi_id,
                        'speed': speed,
                        'avg_speed': avg_speed,
                        'reason': message
                    })
            except ValueError:
                anomalies['invalid_data'].append({
                    'taxi_id': taxi_id,
                    'field': 'speed',
                    'value': speed_str,
                    'reason': 'Invalid number format'
                })
        
        # Location anomalies (sample for performance)
        location_keys = self.r.keys('location:*')[:100]  # Check first 100
        for location_key in location_keys:
            location_data = self.r.hgetall(location_key)
            if location_data:
                try:
                    lat = float(location_data.get('lat', 0))
                    lon = float(location_data.get('lon', 0))
                    taxi_id = location_key.split(':')[1]
                    
                    is_valid, message = self.validate_location(lat, lon)
                    if not is_valid:
                        anomalies['location'].append({
                            'taxi_id': taxi_id,
                            'lat': lat,
                            'lon': lon,
                            'reason': message
                        })
                except ValueError:
                    taxi_id = location_key.split(':')[1]
                    anomalies['invalid_data'].append({
                        'taxi_id': taxi_id,
                        'field': 'location',
                        'reason': 'Invalid coordinates'
                    })
        
        return anomalies
    
    def clean_anomalies(self, anomalies, dry_run=True):
        """Clean identified anomalies from Redis"""
        if dry_run:
            print("🧹 DRY RUN - Anomalies that would be cleaned:")
        else:
            print("🧹 CLEANING anomalies from Redis...")
        
        cleaned_count = 0
        
        # Clean distance anomalies
        for anomaly in anomalies['distance']:
            taxi_id = anomaly['taxi_id']
            print(f"  - Distance: Taxi {taxi_id} ({anomaly['value']} km)")
            if not dry_run:
                self.r.hdel('metrics:distance', taxi_id)
                cleaned_count += 1
        
        # Clean speed anomalies
        for anomaly in anomalies['speed']:
            taxi_id = anomaly['taxi_id']
            print(f"  - Speed: Taxi {taxi_id} ({anomaly['speed']} km/h)")
            if not dry_run:
                self.r.hdel('metrics:speed', taxi_id)
                self.r.hdel('metrics:avgSpeed', taxi_id)
                cleaned_count += 1
        
        # Clean location anomalies
        for anomaly in anomalies['location']:
            taxi_id = anomaly['taxi_id']
            print(f"  - Location: Taxi {taxi_id} ({anomaly['lat']:.6f}, {anomaly['lon']:.6f})")
            if not dry_run:
                self.r.delete(f'location:{taxi_id}')
                self.r.delete(f'trajectory:{taxi_id}')
                self.r.srem('taxi:active', taxi_id)
                cleaned_count += 1
        
        # Clean invalid data
        for anomaly in anomalies['invalid_data']:
            taxi_id = anomaly['taxi_id']
            field = anomaly['field']
            print(f"  - Invalid {field}: Taxi {taxi_id}")
            if not dry_run:
                if field == 'distance':
                    self.r.hdel('metrics:distance', taxi_id)
                elif field == 'speed':
                    self.r.hdel('metrics:speed', taxi_id)
                    self.r.hdel('metrics:avgSpeed', taxi_id)
                elif field == 'location':
                    self.r.delete(f'location:{taxi_id}')
                    self.r.delete(f'trajectory:{taxi_id}')
                    self.r.srem('taxi:active', taxi_id)
                cleaned_count += 1
        
        if not dry_run:
            print(f"✅ Cleaned {cleaned_count} anomalous entries")
        else:
            total_to_clean = len(anomalies['distance']) + len(anomalies['speed']) + len(anomalies['location']) + len(anomalies['invalid_data'])
            print(f"📋 Would clean {total_to_clean} entries")
        
        return cleaned_count
    
    def generate_validation_report(self):
        """Generate a comprehensive validation report"""
        anomalies = self.detect_all_anomalies()
        
        print("\n📊 DATA VALIDATION REPORT")
        print("="*60)
        
        total_anomalies = sum(len(anomalies[key]) for key in anomalies.keys())
        
        print(f"📋 Summary:")
        print(f"  Distance anomalies: {len(anomalies['distance'])}")
        print(f"  Speed anomalies: {len(anomalies['speed'])}")
        print(f"  Location anomalies: {len(anomalies['location'])}")
        print(f"  Invalid data entries: {len(anomalies['invalid_data'])}")
        print(f"  TOTAL ANOMALIES: {total_anomalies}")
        
        # Show top anomalies
        if anomalies['distance']:
            print(f"\n🛣️  Top Distance Anomalies:")
            for i, anomaly in enumerate(sorted(anomalies['distance'], key=lambda x: x['value'], reverse=True)[:3]):
                print(f"    {i+1}. Taxi {anomaly['taxi_id']}: {anomaly['value']:,.2f} km")
        
        if anomalies['speed']:
            print(f"\n🚗 Top Speed Anomalies:")
            for i, anomaly in enumerate(sorted(anomalies['speed'], key=lambda x: x['speed'], reverse=True)[:3]):
                print(f"    {i+1}. Taxi {anomaly['taxi_id']}: {anomaly['speed']:.2f} km/h")
        
        if total_anomalies > 0:
            print(f"\n⚠️  RECOMMENDATIONS:")
            print(f"  1. Review and clean anomalous data")
            print(f"  2. Implement validation in your Flink job")
            print(f"  3. Add data quality monitoring")
            print(f"  4. Consider data source quality")
            
            print(f"\n🧹 CLEANING OPTIONS:")
            print(f"  python taxi_data_validator.py --clean     # Remove anomalies")
            print(f"  python taxi_data_validator.py --dry-run   # Preview cleaning")
        else:
            print(f"\n✅ No anomalies detected - data quality is good!")
        
        return anomalies

def main():
    validator = TaxiDataValidator()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--clean':
            anomalies = validator.detect_all_anomalies()
            validator.clean_anomalies(anomalies, dry_run=False)
        elif sys.argv[1] == '--dry-run':
            anomalies = validator.detect_all_anomalies()
            validator.clean_anomalies(anomalies, dry_run=True)
        else:
            print("Usage: python taxi_data_validator.py [--clean | --dry-run]")
    else:
        validator.generate_validation_report()

if __name__ == "__main__":
    main()
