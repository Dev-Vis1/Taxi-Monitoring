package com.a6.taxi.operators;

import com.a6.taxi.deserialization.TaxiLocationDeserializer;
import com.a6.taxi.dto.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;

public class MainJob {

    private static final Logger log = LoggerFactory.getLogger(MainJob.class);

    // Define side outputs for different alert types
    private static final OutputTag<String> SPEED_ALERTS = new OutputTag<String>("speed-alerts") {};
    private static final OutputTag<String> ZONE_ALERTS = new OutputTag<String>("zone-alerts") {};

    // Constants
    private static final double CENTER_LAT = 39.9163;
    private static final double CENTER_LON = 116.3972;
    private static final double WARNING_RADIUS = 10.0;
    private static final double MAX_RADIUS = 15.0;
    private static final double MAX_SPEED_KMH = 50.0;
    private static final double MAX_REASONABLE_SPEED = 200.0; // km/h
    private static final long STATE_TTL_HOURS = 6;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance and state recovery
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(300000);
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoints"));
        
        // HIGH-THROUGHPUT configuration - OPTIMIZED for massive data volumes
        env.setParallelism(8);  // Increased to fully utilize available slots
        env.getConfig().setAutoWatermarkInterval(5000);  // Even less frequent watermarks for max performance
        env.getConfig().enableObjectReuse();
        env.getConfig().setLatencyTrackingInterval(-1);  // Disable latency tracking
        env.getConfig().setMaxParallelism(128);  // Allow for future scaling

        // Kafka source configuration
        KafkaSource<TaxiLocation> source = KafkaSource.<TaxiLocation>builder()
            .setBootstrapServers("kafka:29092")
            .setTopics("taxi-locations")
            .setGroupId("flink-taxi")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new TaxiLocationDeserializer())
            .setProperty("partition.discovery.interval.ms", "30000")  // Dynamic partition discovery
            .build();

        // Watermark strategy with extreme performance optimization
        WatermarkStrategy<TaxiLocation> watermarkStrategy = WatermarkStrategy
            .<TaxiLocation>forBoundedOutOfOrderness(Duration.ofMinutes(2))  // Much higher tolerance for batch processing
            .withTimestampAssigner((element, recordTimestamp) -> {
                try {
                    return FastDateFormat.parse(element.getTimestamp());
                } catch (ParseException e) {
                    log.debug("Failed to parse timestamp for taxi {}: {}", element.getTaxiId(), element.getTimestamp());
                    return System.currentTimeMillis();
                }
            })
            .withIdleness(Duration.ofMinutes(15));  // Higher idleness for batch processing

        // Main stream
        DataStream<TaxiLocation> locationStream = env.fromSource(
            source, 
            watermarkStrategy,
            "Kafka Source"
        );

        // SPEED CALCULATION =====================================================
        SingleOutputStreamOperator<TaxiSpeed> speedStream = locationStream
            .keyBy(TaxiLocation::getTaxiId)
            .process(new SpeedCalculator())
            .name("SpeedCalculator")
            .uid("speed-calculator");

        // SPEED ALERTS ==========================================================
        DataStream<String> speedAlerts = speedStream
            .getSideOutput(SPEED_ALERTS);
        
        speedAlerts.addSink(new LogSink("SPEED ALERT"));
        
        // ZONE EXIT ALERTS ======================================================
        SingleOutputStreamOperator<TaxiLocation> zoneStream = locationStream
            .keyBy(TaxiLocation::getTaxiId)
            .process(new ZoneExitNotifier())
            .name("ZoneExitNotifier")
            .uid("zone-notifier");
        
        DataStream<String> zoneAlerts = zoneStream
            .getSideOutput(ZONE_ALERTS);
        
        zoneAlerts.addSink(new LogSink("ZONE ALERT"));
        
        // AVERAGE SPEED =========================================================
        SingleOutputStreamOperator<TaxiAverageSpeed> avgSpeedStream = speedStream
            .keyBy(TaxiSpeed::getTaxiId)
            .process(new AverageSpeedCalculator())
            .name("AverageSpeedCalculator")
            .uid("avg-speed-calculator");
        
        // DISTANCE TRACKING =====================================================
        SingleOutputStreamOperator<TaxiDistance> distanceStream = locationStream
            .keyBy(TaxiLocation::getTaxiId)
            .process(new DistanceTracker())
            .name("DistanceTracker")
            .uid("distance-tracker");
        
        // REDIS SINK CONNECTIONS ================================================
        locationStream.addSink(new RedisSink<>()).name("LocationRedisSink");
        speedStream.addSink(new RedisSink<>()).name("SpeedRedisSink");
        avgSpeedStream.addSink(new RedisSink<>()).name("AvgSpeedRedisSink");
        distanceStream.addSink(new RedisSink<>()).name("DistanceRedisSink");

        env.execute("Optimized Taxi Monitoring");
    }

    // Optimized timestamp parser
    private static class FastDateFormat {
        private static final ThreadLocal<SimpleDateFormat> FORMATTER = 
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        
        public static long parse(String timestamp) throws ParseException {
            return FORMATTER.get().parse(timestamp).getTime();
        }
    }

    // Zone Exit Notifier ========================================================
    public static class ZoneExitNotifier 
        extends KeyedProcessFunction<String, TaxiLocation, TaxiLocation> {
        
        @Override
        public void processElement(
            TaxiLocation location, 
            Context context, 
            Collector<TaxiLocation> out
        ) {
            out.collect(location);
            
            double distance = Haversine.computeDistance(
                CENTER_LAT, CENTER_LON, 
                location.getLatitude(), location.getLongitude()
            );
            
            if (distance > WARNING_RADIUS && distance <= MAX_RADIUS) {
                context.output(ZONE_ALERTS,
                    "⚠️ Taxi " + location.getTaxiId() + 
                    " exiting zone. Distance: " + String.format("%.2f", distance) + " km"
                );
            }
        }
    }

    // Speed Calculator ==========================================================
    public static class SpeedCalculator 
        extends KeyedProcessFunction<String, TaxiLocation, TaxiSpeed> {
        
        private transient ValueState<TaxiLocation> previousLocation;
        private final StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.hours(STATE_TTL_HOURS))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanupInRocksdbCompactFilter(1000)  // Enable RocksDB compaction filter
            .build();

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<TaxiLocation> desc = new ValueStateDescriptor<>(
                "lastLocation", 
                TypeInformation.of(new TypeHint<TaxiLocation>() {})
            );
            desc.enableTimeToLive(ttlConfig);
            previousLocation = getRuntimeContext().getState(desc);
        }

        @Override
        public void processElement(
            TaxiLocation current, 
            Context context, 
            Collector<TaxiSpeed> out
        ) throws Exception {
            TaxiLocation previous = previousLocation.value();
            previousLocation.update(current);

            if (previous == null) return;

            long t1 = FastDateFormat.parse(previous.getTimestamp());
            long t2 = FastDateFormat.parse(current.getTimestamp());
            long timeDiff = Math.abs(t2 - t1);

            if (timeDiff == 0) {
                log.debug("Zero time difference for taxi {}", current.getTaxiId());
                return;
            }

            double dist = Haversine.computeDistance(
                previous.getLatitude(), previous.getLongitude(),
                current.getLatitude(), current.getLongitude()
            );
            
            double hours = timeDiff / 3600000.0;
            double speed = dist / hours;

            // Validate and cap speed
            if (speed > MAX_REASONABLE_SPEED) {
                log.debug("Capped speed for taxi {}: {:.2f} km/h", current.getTaxiId(), speed);
                speed = MAX_REASONABLE_SPEED;
            }
            
            TaxiSpeed speedData = new TaxiSpeed(current.getTaxiId(), speed);
            out.collect(speedData);
            
            // Emit alert if needed
            if (speed > MAX_SPEED_KMH) {
                context.output(SPEED_ALERTS,
                    "⚠️ Speed alert for Taxi " + current.getTaxiId() + 
                    ": " + String.format("%.2f", speed) + " km/h"
                );
            }
        }
    }

    // Average Speed Calculator ==================================================
    public static class AverageSpeedCalculator 
        extends KeyedProcessFunction<String, TaxiSpeed, TaxiAverageSpeed> {
        
        private transient ValueState<Tuple2<Integer, Double>> speedStats;
        private final StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.hours(STATE_TTL_HOURS))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanupInRocksdbCompactFilter(1000)
            .build();

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Tuple2<Integer, Double>> descriptor = 
                new ValueStateDescriptor<>(
                    "speedStats",
                    TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {})
                );
            descriptor.enableTimeToLive(ttlConfig);
            speedStats = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
            TaxiSpeed speed, 
            Context context, 
            Collector<TaxiAverageSpeed> out
        ) throws Exception {
            Tuple2<Integer, Double> current = speedStats.value();
            if (current == null) {
                current = Tuple2.of(0, 0.0);
            }

            int count = current.f0 + 1;
            double total = current.f1 + speed.getSpeed();
            double avg = total / count;

            if (!Double.isNaN(avg)) {
                out.collect(new TaxiAverageSpeed(speed.getTaxiId(), avg));
                speedStats.update(Tuple2.of(count, total));
            } else {
                log.warn("NaN in average speed for taxi {}", speed.getTaxiId());
                speedStats.clear();
            }
        }
    }

    // Distance Tracker ==========================================================
    public static class DistanceTracker 
        extends KeyedProcessFunction<String, TaxiLocation, TaxiDistance> {
        
        private transient ValueState<Double> accumulatedDistance;
        private transient ValueState<TaxiLocation> lastKnownLocation;
        private final StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.hours(STATE_TTL_HOURS))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanupInRocksdbCompactFilter(1000)
            .build();

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Double> distanceDesc = new ValueStateDescriptor<>(
                "accumulatedDistance", 
                Double.class
            );
            distanceDesc.enableTimeToLive(ttlConfig);
            accumulatedDistance = getRuntimeContext().getState(distanceDesc);
            
            ValueStateDescriptor<TaxiLocation> locationDesc = new ValueStateDescriptor<>(
                "lastKnownLocation", 
                TaxiLocation.class
            );
            locationDesc.enableTimeToLive(ttlConfig);
            lastKnownLocation = getRuntimeContext().getState(locationDesc);
        }

        @Override
        public void processElement(
            TaxiLocation current, 
            Context context, 
            Collector<TaxiDistance> out
        ) throws Exception {
            Double totalDistance = accumulatedDistance.value();
            if (totalDistance == null) {
                totalDistance = 0.0;
            }

            TaxiLocation previous = lastKnownLocation.value();
            lastKnownLocation.update(current);

            if (previous != null) {
                double segment = Haversine.computeDistance(
                    previous.getLatitude(), previous.getLongitude(),
                    current.getLatitude(), current.getLongitude()
                );
                totalDistance += segment;
            }

            accumulatedDistance.update(totalDistance);
            out.collect(new TaxiDistance(current.getTaxiId(), totalDistance));
        }
    }

    // Optimized Log Sink ========================================================
    private static class LogSink implements SinkFunction<String> {
        private final String label;
        private static final Logger logger = LoggerFactory.getLogger(LogSink.class);
        
        LogSink(String label) {
            this.label = label;
        }
        
        @Override
        public void invoke(String message, Context context) {
            logger.info("[{}] {}", label, message);
        }
    }
}