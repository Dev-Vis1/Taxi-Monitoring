package com.a6.taxi.operators;

import com.a6.taxi.dto.TaxiAverageSpeed;
import com.a6.taxi.dto.TaxiSpeed;
import com.a6.taxi.dto.TaxiLocation;
import com.a6.taxi.dto.TaxiDistance;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

public class RedisSink<T> extends RichSinkFunction<T> {

    private transient Jedis redisClient;

    @Override
    public void open(Configuration parameters) {
        redisClient = new Jedis("redis", 6379);
    }

    @Override
    public void close() {
        if (redisClient != null) {
            redisClient.close();
        }
    }

    @Override
    public void invoke(T input, Context context) {
        if (input instanceof TaxiSpeed) {
            TaxiSpeed speed = (TaxiSpeed) input;
            redisClient.hset("metrics:speed", speed.getTaxiId(), String.valueOf(speed.getSpeed()));

        } else if (input instanceof TaxiAverageSpeed) {
            TaxiAverageSpeed avgSpeed = (TaxiAverageSpeed) input;
            redisClient.hset("metrics:avgSpeed", avgSpeed.getTaxiId(), String.valueOf(avgSpeed.getAverageSpeed()));

        } else if (input instanceof TaxiDistance) {
            TaxiDistance distance = (TaxiDistance) input;
            redisClient.hset("metrics:distance", distance.getTaxiId(), String.valueOf(distance.getDistance()));

        } else if (input instanceof TaxiLocation) {
            TaxiLocation location = (TaxiLocation) input;
            String locationKey = "location:" + location.getTaxiId();
            redisClient.hset(locationKey, "lat", String.valueOf(location.getLatitude()));
            redisClient.hset(locationKey, "lon", String.valueOf(location.getLongitude()));
            redisClient.hset(locationKey, "time", location.getTimestamp());

        } else if (input instanceof String) {
            redisClient.lpush("taxi:logs", (String) input);
        }
    }
}
