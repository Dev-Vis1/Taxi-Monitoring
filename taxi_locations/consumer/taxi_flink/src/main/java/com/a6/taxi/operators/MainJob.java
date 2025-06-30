package com.a6.taxi.operators;

import com.a6.taxi.deserialization.TaxiLocationDeserializer;
import com.a6.taxi.dto.*;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MainJob {

    private static final Logger log = LoggerFactory.getLogger(MainJob.class);

    private static final double CENTER_LAT = 39.9163;
    private static final double CENTER_LON = 116.3972;
    private static final double WARNING_RADIUS = 10.0;
    private static final double MAX_RADIUS = 15.0;
    private static final double MAX_SPEED_KMH = 50.0;

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // OPTIMIZATION: Configure environment for high throughput
        env.setParallelism(20);  // Increased parallelism
        env.enableCheckpointing(30000);  // Checkpoint every 30 seconds
        env.getConfig().setAutoWatermarkInterval(1000);  // More frequent watermarks
        
        // Configure for better performance with large datasets
        env.getConfig().enableObjectReuse();  // Reuse objects for better performance

        var source = KafkaSource.<TaxiLocation>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("taxi-locations")
                .setGroupId("flink-taxi")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TaxiLocationDeserializer())
                .build();

        var locationStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        var speedStream = locationStream
                .keyBy(TaxiLocation::getTaxiId)
                .process(new SpeedCalculator());

        var speedAlerts = speedStream
                .keyBy(TaxiSpeed::getTaxiId)
                .process(new SpeedWarningGenerator());

        speedAlerts.addSink(logSink("Speed Alert"));

        var zoneExitAlerts = locationStream
                .keyBy(TaxiLocation::getTaxiId)
                .process(new ZoneExitNotifier());

        zoneExitAlerts.addSink(logSink("Zone Alert"));

        speedStream.addSink(new SinkFunction<>() {
            @Override
            public void invoke(TaxiSpeed value, Context context) {
                System.out.printf("Taxi %s speed: %.2f km/h%n", value.getTaxiId(), value.getSpeed());
            }
        });

        var avgSpeedStream = speedStream
                .keyBy(TaxiSpeed::getTaxiId)
                .process(new AverageSpeedCalculator());

        avgSpeedStream.addSink(new SinkFunction<>() {
            @Override
            public void invoke(TaxiAverageSpeed value, Context context) {
                System.out.printf("Taxi %s average speed: %.2f km/h%n", value.getTaxiId(), value.getAverageSpeed());
            }
        });

        avgSpeedStream.print("Average Speeds");

        var distanceStream = locationStream
                .keyBy(TaxiLocation::getTaxiId)
                .process(new DistanceTracker());

        distanceStream.addSink(new SinkFunction<>() {
            @Override
            public void invoke(TaxiDistance value, Context context) {
                System.out.printf("Taxi %s total distance: %.2f km%n", value.getTaxiId(), value.getDistance());
            }
        });

        distanceStream.print("Distance Updates");

        locationStream.print();

        locationStream.addSink(new RedisSink<>());
        speedStream.addSink(new RedisSink<>());
        avgSpeedStream.addSink(new RedisSink<>());
        distanceStream.addSink(new RedisSink<>());

        env.execute("Flink Taxi Monitoring Job");
    }

    private static SinkFunction<String> logSink(String label) {
        return new SinkFunction<>() {
            @Override
            public void invoke(String message, Context context) {
                log.info("{}: {}", label, message);
            }
        };
    }

    public static class SpeedWarningGenerator extends KeyedProcessFunction<String, TaxiSpeed, String> {
        @Override
        public void processElement(TaxiSpeed speed, Context context, Collector<String> out) {
            if (speed.getSpeed() > MAX_SPEED_KMH) {
                out.collect("⚠️ Speed alert for Taxi " + speed.getTaxiId() + ": " + speed.getSpeed() + " km/h");
            }
        }
    }

    public static class ZoneExitNotifier extends KeyedProcessFunction<String, TaxiLocation, String> {
        @Override
        public void processElement(TaxiLocation location, Context context, Collector<String> out) {
            double distance = Haversine.computeDistance(CENTER_LAT, CENTER_LON, location.getLatitude(),
                    location.getLongitude());
            if (distance > WARNING_RADIUS && distance <= MAX_RADIUS) {
                out.collect(
                        "⚠️ Taxi " + location.getTaxiId() + " is exiting the monitored zone. Distance: " + distance
                                + " km");
            }
        }
    }

    public static class SpeedCalculator extends KeyedProcessFunction<String, TaxiLocation, TaxiSpeed> {
        private transient ValueState<TaxiLocation> previousLocation;
        private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<TaxiLocation> desc = new ValueStateDescriptor<>("lastLocation",
                    TypeInformation.of(new TypeHint<>() {
                    }));
            previousLocation = getRuntimeContext().getState(desc);
        }

        @Override
        public void processElement(TaxiLocation current, Context context, Collector<TaxiSpeed> out) throws Exception {
            TaxiLocation previous = previousLocation.value();

            if (previous != null) {
                long t1 = parseTime(previous.getTimestamp());
                long t2 = parseTime(current.getTimestamp());

                // Handle both forward and backward timestamps for academic project
                // Calculate time difference as absolute value to handle out-of-order data
                long timeDiff = Math.abs(t2 - t1);
                
                if (timeDiff > 0) {
                    double dist = Haversine.computeDistance(
                            previous.getLatitude(), previous.getLongitude(),
                            current.getLatitude(), current.getLongitude());
                    double hours = timeDiff / 3600000.0;
                    if (hours > 0) {
                        double speed = dist / hours;
                        // Cap speed at reasonable maximum (200 km/h) to avoid unrealistic values
                        speed = Math.min(speed, 200.0);
                        out.collect(new TaxiSpeed(current.getTaxiId(), speed));
                    } else {
                        log.warn("Zero time interval for Taxi {}", current.getTaxiId());
                    }
                } else {
                    log.warn("Identical timestamps for Taxi {}: current={}, previous={}", current.getTaxiId(), t2, t1);
                }
            }

            previousLocation.update(current);
        }

        private long parseTime(String timestamp) throws ParseException {
            return formatter.parse(timestamp).getTime();
        }
    }

    public static class AverageSpeedCalculator extends KeyedProcessFunction<String, TaxiSpeed, TaxiAverageSpeed> {
        private transient ValueState<Tuple2<Integer, Double>> speedStats;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Tuple2<Integer, Double>> descriptor = new ValueStateDescriptor<>("speedStats",
                    TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {
                    }));
            speedStats = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(TaxiSpeed speed, Context context, Collector<TaxiAverageSpeed> out) throws Exception {
            var current = speedStats.value();
            if (current == null)
                current = Tuple2.of(0, 0.0);

            int count = current.f0 + 1;
            double total = current.f1 + speed.getSpeed();

            double avg = count > 0 ? total / count : 0.0;

            if (!Double.isNaN(avg)) {
                out.collect(new TaxiAverageSpeed(speed.getTaxiId(), avg));
            } else {
                log.warn("NaN detected in average speed for Taxi {}", speed.getTaxiId());
            }

            speedStats.update(Tuple2.of(count, total));
        }
    }

    public static class DistanceTracker extends KeyedProcessFunction<String, TaxiLocation, TaxiDistance> {
        private transient ValueState<Double> accumulatedDistance;
        private transient ValueState<TaxiLocation> lastKnownLocation;

        @Override
        public void open(Configuration config) {
            accumulatedDistance = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("accumulatedDistance", Double.class));
            lastKnownLocation = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("lastKnownLocation", TaxiLocation.class));
        }

        @Override
        public void processElement(TaxiLocation current, Context context, Collector<TaxiDistance> out)
                throws Exception {
            var previous = lastKnownLocation.value();
            double totalDistance = accumulatedDistance.value() != null ? accumulatedDistance.value() : 0.0;

            if (previous != null) {
                double segment = Haversine.computeDistance(
                        previous.getLatitude(), previous.getLongitude(),
                        current.getLatitude(), current.getLongitude());
                totalDistance += segment;
            }

            out.collect(new TaxiDistance(current.getTaxiId(), totalDistance));

            accumulatedDistance.update(totalDistance);
            lastKnownLocation.update(current);
        }
    }
}
