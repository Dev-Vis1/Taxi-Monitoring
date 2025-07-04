package com.a6.taxi.operators;

import com.a6.taxi.dto.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class RedisSink<T> extends RichSinkFunction<T> {

    private transient JedisPool jedisPool;
    private transient BlockingQueue<T> writeQueue;
    private transient ExecutorService executor;
    private static final int QUEUE_CAPACITY = 20000;
    private static final int BATCH_SIZE = 200;
    private static final long FLUSH_INTERVAL_MS = 100;
    private static final int MAX_RETRIES = 3;
    private volatile boolean running = true;

    @Override
    public void open(Configuration parameters) {
        // Optimized Redis connection pool
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(50);
        config.setMaxIdle(20);
        config.setMinIdle(10);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setTestWhileIdle(true);
        config.setMaxWait(Duration.ofSeconds(5));
        
        jedisPool = new JedisPool(config, "redis", 6379, 5000);
        writeQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        
        // Start async writer
        executor = Executors.newSingleThreadExecutor();
        executor.submit(this::asyncWriter);
    }

    @Override
    public void close() {
        running = false;
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    @Override
    public void invoke(T input, Context context) {
        if (!writeQueue.offer(input)) {
            // Handle queue full situation with direct write
            writeDirectWithRetry(input);
        }
    }
    
    private void writeDirectWithRetry(T input) {
        for (int i = 0; i < MAX_RETRIES; i++) {
            try (Jedis jedis = jedisPool.getResource()) {
                processRecord(jedis, input);
                return;
            } catch (JedisConnectionException e) {
                System.err.println("Direct write failed, retry " + (i+1) + "/" + MAX_RETRIES);
                try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
        System.err.println("Failed direct write after " + MAX_RETRIES + " attempts");
    }

    private void asyncWriter() {
        List<T> batch = new ArrayList<>(BATCH_SIZE);
        while (running || !writeQueue.isEmpty()) {
            try {
                // Block with timeout to allow periodic flushing
                T record = writeQueue.poll(FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
                if (record != null) {
                    batch.add(record);
                    // Drain as many as possible without blocking
                    writeQueue.drainTo(batch, BATCH_SIZE - batch.size());
                }
                
                if (!batch.isEmpty() && (batch.size() >= BATCH_SIZE || record == null)) {
                    flushBatchWithRetry(batch);
                    batch.clear();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("Async writer error: " + e.getMessage());
            }
        }
        
        // Flush any remaining records
        if (!batch.isEmpty()) {
            flushBatchWithRetry(batch);
        }
    }

    private void flushBatchWithRetry(List<T> batch) {
        for (int i = 0; i < MAX_RETRIES; i++) {
            try (Jedis jedis = jedisPool.getResource()) {
                Pipeline pipeline = jedis.pipelined();
                
                for (T input : batch) {
                    processRecord(pipeline, input);
                }
                
                pipeline.sync();
                return; // Success, exit retry loop
            } catch (JedisConnectionException e) {
                System.err.println("Batch flush failed, retry " + (i+1) + "/" + MAX_RETRIES);
                try { Thread.sleep(200); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
        System.err.println("Failed batch flush after " + MAX_RETRIES + " attempts");
    }

    private void processRecord(Pipeline pipelined, T input) {
        if (input instanceof TaxiSpeed) {
            TaxiSpeed speed = (TaxiSpeed) input;
            pipelined.hset("metrics:speed", speed.getTaxiId(), String.valueOf(speed.getSpeed()));

        } else if (input instanceof TaxiAverageSpeed) {
            TaxiAverageSpeed avgSpeed = (TaxiAverageSpeed) input;
            pipelined.hset("metrics:avgSpeed", avgSpeed.getTaxiId(), String.valueOf(avgSpeed.getAverageSpeed()));

        } else if (input instanceof TaxiDistance) {
            TaxiDistance distance = (TaxiDistance) input;
            pipelined.hset("metrics:distance", distance.getTaxiId(), String.valueOf(distance.getDistance()));

        } else if (input instanceof TaxiLocation) {
            TaxiLocation location = (TaxiLocation) input;
            String locationKey = "location:" + location.getTaxiId();
            String trajectoryKey = "trajectory:" + location.getTaxiId();
            
            // Store current location
            pipelined.hset(locationKey, "lat", String.valueOf(location.getLatitude()));
            pipelined.hset(locationKey, "lon", String.valueOf(location.getLongitude()));
            pipelined.hset(locationKey, "time", location.getTimestamp());
            
            // Store trajectory point - only 20% of trajectories to reduce memory
            if (System.currentTimeMillis() % 5 == 0) {
                pipelined.lpush(trajectoryKey, formatTrajectoryPoint(location));
                pipelined.ltrim(trajectoryKey, 0, 4);  // Only last 5 positions
            }
            
            // Update active set
            pipelined.sadd("taxi:active", location.getTaxiId());
        }
    }
    
    private void processRecord(Jedis jedis, T input) {
        if (input instanceof TaxiSpeed) {
            TaxiSpeed speed = (TaxiSpeed) input;
            jedis.hset("metrics:speed", speed.getTaxiId(), String.valueOf(speed.getSpeed()));

        } else if (input instanceof TaxiAverageSpeed) {
            TaxiAverageSpeed avgSpeed = (TaxiAverageSpeed) input;
            jedis.hset("metrics:avgSpeed", avgSpeed.getTaxiId(), String.valueOf(avgSpeed.getAverageSpeed()));

        } else if (input instanceof TaxiDistance) {
            TaxiDistance distance = (TaxiDistance) input;
            jedis.hset("metrics:distance", distance.getTaxiId(), String.valueOf(distance.getDistance()));

        } else if (input instanceof TaxiLocation) {
            TaxiLocation location = (TaxiLocation) input;
            String locationKey = "location:" + location.getTaxiId();
            
            jedis.hset(locationKey, "lat", String.valueOf(location.getLatitude()));
            jedis.hset(locationKey, "lon", String.valueOf(location.getLongitude()));
            jedis.hset(locationKey, "time", location.getTimestamp());
            
            // Only store trajectory in batch mode to reduce load
            jedis.sadd("taxi:active", location.getTaxiId());
        }
    }
    
    private String formatTrajectoryPoint(TaxiLocation location) {
        return String.format(
            "{\"lat\":%.6f,\"lon\":%.6f,\"time\":\"%s\"}",
            location.getLatitude(),
            location.getLongitude(), 
            location.getTimestamp()
        );
    }
}