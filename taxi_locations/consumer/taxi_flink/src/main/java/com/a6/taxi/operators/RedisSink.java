package com.a6.taxi.operators;

import com.a6.taxi.dto.TaxiAverageSpeed;
import com.a6.taxi.dto.TaxiSpeed;
import com.a6.taxi.dto.TaxiLocation;
import com.a6.taxi.dto.TaxiDistance;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.List;

public class RedisSink<T> extends RichSinkFunction<T> {

    private transient JedisPool jedisPool;
    private transient List<T> buffer;
    private static final int BATCH_SIZE = 100; // Batch operations for better performance
    private long lastFlushTime = 0;
    private static final long FLUSH_INTERVAL = 1000; // Flush every 1 second

    @Override
    public void open(Configuration parameters) {
        // Optimized Redis connection pool
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(20);
        config.setMaxIdle(10);
        config.setMinIdle(5);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setTestWhileIdle(true);

        jedisPool = new JedisPool(config, "redis", 6379, 10000);
        buffer = new ArrayList<>();
        lastFlushTime = System.currentTimeMillis();
    }

    @Override
    public void close() {
        if (!buffer.isEmpty()) {
            flushBuffer();
        }
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    @Override
    public void invoke(T input, Context context) {
        buffer.add(input);

        // Flush buffer if it's full or if enough time has passed
        if (buffer.size() >= BATCH_SIZE ||
                (System.currentTimeMillis() - lastFlushTime) > FLUSH_INTERVAL) {
            flushBuffer();
        }
    }

    private void flushBuffer() {
        if (buffer.isEmpty())
            return;

        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline pipeline = jedis.pipelined();

            for (T input : buffer) {
                if (input instanceof TaxiSpeed) {
                    TaxiSpeed speed = (TaxiSpeed) input;
                    pipeline.hset("metrics:speed", speed.getTaxiId(), String.valueOf(speed.getSpeed()));

                } else if (input instanceof TaxiAverageSpeed) {
                    TaxiAverageSpeed avgSpeed = (TaxiAverageSpeed) input;
                    pipeline.hset("metrics:avgSpeed", avgSpeed.getTaxiId(), String.valueOf(avgSpeed.getAverageSpeed()));

                } else if (input instanceof TaxiDistance) {
                    TaxiDistance distance = (TaxiDistance) input;
                    pipeline.hset("metrics:distance", distance.getTaxiId(), String.valueOf(distance.getDistance()));

                } else if (input instanceof TaxiLocation) {
                    TaxiLocation location = (TaxiLocation) input;
                    String locationKey = "location:" + location.getTaxiId();
                    pipeline.hset(locationKey, "lat", String.valueOf(location.getLatitude()));
                    pipeline.hset(locationKey, "lon", String.valueOf(location.getLongitude()));
                    pipeline.hset(locationKey, "time", location.getTimestamp());

                } else if (input instanceof String) {
                    pipeline.lpush("taxi:logs", (String) input);
                }
            }

            pipeline.sync(); // Execute all commands
            buffer.clear();
            lastFlushTime = System.currentTimeMillis();

        } catch (Exception e) {
            System.err.println("Error flushing Redis buffer: " + e.getMessage());
        }
    }
}