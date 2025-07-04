#!/usr/bin/env python3
"""
Real-time Performance Monitor for Ultra-High-Throughput Taxi System
Monitors Kafka, Flink, and Redis to ensure Flink keeps up with producer
"""

import time
import requests
import json
import redis
import subprocess
import os
from datetime import datetime
from collections import defaultdict
try:
    from kafka import KafkaConsumer, TopicPartition
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    print("⚠️  kafka-python not available. Install with: pip install kafka-python")
    KAFKA_AVAILABLE = False

class PerformanceMonitor:
    def __init__(self):
        self.kafka_broker = 'localhost:9092'
        self.flink_url = 'http://localhost:8081'
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.start_time = time.time()
        self.last_metrics = {}
        
    def get_kafka_metrics(self):
        """Monitor Kafka topic lag and throughput"""
        if not KAFKA_AVAILABLE:
            return None
            
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=[self.kafka_broker],
                enable_auto_commit=False,
                consumer_timeout_ms=1000
            )
            
            topic = 'taxi-locations'
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                return None
                
            partition_info = []
            total_lag = 0
            total_messages = 0
            
            for partition_id in partitions:
                tp = TopicPartition(topic, partition_id)
                consumer.assign([tp])
                
                # Get latest offset (end of log)
                latest_offset = consumer.end_offsets([tp])[tp]
                
                # Get committed offset (last processed by consumer group)
                committed = consumer.committed(tp) or 0
                
                lag = latest_offset - committed
                total_lag += lag
                total_messages += latest_offset
                
                partition_info.append({
                    'partition': partition_id,
                    'latest_offset': latest_offset,
                    'committed_offset': committed,
                    'lag': lag
                })
            
            consumer.close()
            
            return {
                'total_messages': total_messages,
                'total_lag': total_lag,
                'partitions': len(partitions),
                'partition_details': partition_info,
                'avg_lag_per_partition': total_lag / len(partitions) if partitions else 0
            }
            
        except Exception as e:
            print(f"❌ Kafka metrics error: {e}")
            return None
    
    def get_flink_metrics(self):
        """Monitor Flink job performance and backpressure"""
        try:
            # Get job overview
            jobs_response = requests.get(f"{self.flink_url}/jobs", timeout=5)
            if jobs_response.status_code != 200:
                return None
                
            jobs = jobs_response.json()['jobs']
            if not jobs:
                return {'status': 'NO_JOBS_RUNNING'}
            
            # Get the running job (should be our taxi job)
            running_job = None
            for job in jobs:
                if job['status'] == 'RUNNING':
                    running_job = job
                    break
            
            if not running_job:
                return {'status': 'NO_RUNNING_JOBS'}
            
            job_id = running_job['jid']
            
            # Get detailed job metrics
            detail_response = requests.get(f"{self.flink_url}/jobs/{job_id}", timeout=5)
            
            if detail_response.status_code != 200:
                return None
            
            job_details = detail_response.json()
            
            return {
                'job_id': job_id,
                'job_name': job_details['name'],
                'status': job_details['state'],
                'start_time': job_details['start-time'],
                'duration': job_details['duration']
            }
            
        except Exception as e:
            print(f"❌ Flink metrics error: {e}")
            return None
    
    def get_redis_metrics(self):
        """Monitor Redis performance and data volume"""
        try:
            info = self.redis_client.info()
            
            # Get key counts
            active_taxis = len(self.redis_client.smembers('taxi:active'))
            speed_records = len(self.redis_client.hgetall('metrics:speed'))
            avg_speed_records = len(self.redis_client.hgetall('metrics:avgSpeed'))
            distance_records = len(self.redis_client.hgetall('metrics:distance'))
            
            # Get memory usage
            memory_used = info.get('used_memory', 0)
            memory_peak = info.get('used_memory_peak', 0)
            
            # Calculate throughput
            total_commands = info.get('total_commands_processed', 0)
            
            return {
                'memory_used_mb': memory_used / (1024 * 1024),
                'memory_peak_mb': memory_peak / (1024 * 1024),
                'total_commands': total_commands,
                'active_taxis': active_taxis,
                'speed_records': speed_records,
                'avg_speed_records': avg_speed_records,
                'distance_records': distance_records,
                'connected_clients': info.get('connected_clients', 0),
                'instantaneous_ops_per_sec': info.get('instantaneous_ops_per_sec', 0)
            }
            
        except Exception as e:
            print(f"❌ Redis metrics error: {e}")
            return None
    
    def calculate_throughput(self, current_metrics, metric_key):
        """Calculate throughput between measurements"""
        current_time = time.time()
        
        if metric_key in self.last_metrics:
            last_value, last_time = self.last_metrics[metric_key]
            if current_time > last_time:
                throughput = (current_metrics - last_value) / (current_time - last_time)
                self.last_metrics[metric_key] = (current_metrics, current_time)
                return throughput
        
        self.last_metrics[metric_key] = (current_metrics, current_time)
        return 0
    
    def print_status_report(self, kafka_metrics, flink_metrics, redis_metrics):
        """Print comprehensive status report"""
        os.system('cls' if os.name == 'nt' else 'clear')
        
        elapsed = time.time() - self.start_time
        elapsed_str = f"{int(elapsed//60):02d}:{int(elapsed%60):02d}"
        
        print("=" * 100)
        print(f"🚀 ULTRA-HIGH-PERFORMANCE TAXI MONITORING - Runtime: {elapsed_str}")
        print("=" * 100)
        
        # Kafka Status
        print("\n📊 KAFKA STATUS:")
        if kafka_metrics:
            print(f"   📈 Total Messages:     {kafka_metrics['total_messages']:,}")
            print(f"   ⏳ Total Lag:          {kafka_metrics['total_lag']:,}")
            print(f"   🔄 Partitions:         {kafka_metrics['partitions']}")
            print(f"   📊 Avg Lag/Partition:  {kafka_metrics['avg_lag_per_partition']:.1f}")
            
            if kafka_metrics['total_lag'] > 100000:
                print("   ⚠️  HIGH LAG DETECTED - Flink may be falling behind!")
            elif kafka_metrics['total_lag'] > 50000:
                print("   🔶 MODERATE LAG - Monitor closely")
            else:
                print("   ✅ LAG HEALTHY")
        else:
            print("   ❌ Kafka metrics unavailable")
        
        # Flink Status  
        print("\n🔧 FLINK STATUS:")
        if flink_metrics and 'status' in flink_metrics:
            if flink_metrics['status'] == 'NO_JOBS_RUNNING':
                print("   ❌ NO JOBS RUNNING")
            elif flink_metrics['status'] == 'NO_RUNNING_JOBS':
                print("   ⚠️  JOBS EXIST BUT NOT RUNNING")
            else:
                print(f"   📋 Job:               {flink_metrics['job_name']}")
                print(f"   🟢 Status:            {flink_metrics['status']}")
        else:
            print("   ❌ Flink metrics unavailable")
        
        # Redis Status
        print("\n💾 REDIS STATUS:")
        if redis_metrics:
            print(f"   🏎️  Active Taxis:       {redis_metrics['active_taxis']:,}")
            print(f"   📊 Speed Records:      {redis_metrics['speed_records']:,}")
            print(f"   📈 Avg Speed Records:  {redis_metrics['avg_speed_records']:,}")
            print(f"   📏 Distance Records:   {redis_metrics['distance_records']:,}")
            print(f"   💽 Memory Used:        {redis_metrics['memory_used_mb']:.1f} MB")
            print(f"   ⚡ Ops/Sec:            {redis_metrics['instantaneous_ops_per_sec']:,}")
            
            # Calculate Redis throughput
            redis_throughput = self.calculate_throughput(redis_metrics['total_commands'], 'redis_commands')
            if redis_throughput > 0:
                print(f"   🚀 Avg Throughput:     {redis_throughput:,.0f} commands/sec")
        else:
            print("   ❌ Redis metrics unavailable")
        
        # Overall System Health
        print("\n🎯 SYSTEM HEALTH:")
        if kafka_metrics and flink_metrics and redis_metrics:
            if (kafka_metrics['total_lag'] < 50000 and 
                flink_metrics.get('status') == 'RUNNING' and
                redis_metrics['active_taxis'] > 0):
                print("   ✅ SYSTEM RUNNING OPTIMALLY")
                print("   🔥 Ultra-high-performance mode ACTIVE")
            elif kafka_metrics['total_lag'] > 100000:
                print("   ⚠️  PERFORMANCE DEGRADATION DETECTED")
                print("   💡 Consider scaling Flink or reducing producer rate")
            else:
                print("   🔶 SYSTEM OPERATIONAL")
        else:
            print("   ❌ UNABLE TO DETERMINE SYSTEM HEALTH")
        
        print("\n" + "=" * 100)
        print("Press Ctrl+C to stop monitoring...")
    
    def monitor(self, interval=5):
        """Main monitoring loop"""
        print("🚀 Starting ultra-high-performance monitoring...")
        print(f"📊 Monitoring interval: {interval} seconds")
        
        try:
            while True:
                kafka_metrics = self.get_kafka_metrics()
                flink_metrics = self.get_flink_metrics()
                redis_metrics = self.get_redis_metrics()
                
                self.print_status_report(kafka_metrics, flink_metrics, redis_metrics)
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n\n🛑 Monitoring stopped by user")
        except Exception as e:
            print(f"\n\n❌ Monitoring error: {e}")

if __name__ == "__main__":
    monitor = PerformanceMonitor()
    monitor.monitor()
