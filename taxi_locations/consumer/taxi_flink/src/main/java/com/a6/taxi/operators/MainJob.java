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
import java.util.Date;

public class MainJob {

    private static final Logger log = LoggerFactory.getLogger(MainJob.class);

    // Define side outputs for different alert types
    private static final OutputTag<String> SPEED_ALERTS = new OutputTag<String>("speed-alerts") {};
    private static final OutputTag<String> ZONE_ALERTS = new OutputTag<String>("zone-alerts") {};
    private static final OutputTag<String> ZONE_EXIT_ALERTS = new OutputTag<String>("zone-exit-alerts") {};
    private static final OutputTag<String> VALIDATION_ALERTS = new OutputTag<String>("validation-alerts") {};

    // Constants
    private static final double CENTER_LAT = 39.9163;
    private static final double CENTER_LON = 116.3972;
    private static final double WARNING_RADIUS = 10.0;
    // Legacy constants (kept for backward compatibility)
    private static final double MAX_RADIUS = 15.0;
    private static final double MAX_SPEED_KMH = 50.0;
    private static final long STATE_TTL_HOURS = 6;
    
    // Data validation thresholds
    private static final double MAX_REALISTIC_DISTANCE = 5000.0;     // km - max realistic total distance
    private static final double MAX_SEGMENT_DISTANCE = 50.0;         // km - max distance between two points
    private static final double MIN_SEGMENT_DISTANCE = 0.001;        // km - minimum meaningful distance

    private static final double MIN_REALISTIC_SPEED = 0.0;           // km/h - minimum speed
    private static final double MAX_TAXI_SPEED = 120.0;              // km/h - max realistic speed (used for validation & capping)
    private static final double MAX_REALISTIC_AVG_SPEED = 80.0;      // km/h - max realistic average speed
    private static final long MAX_TIME_GAP_MS = 3600000;             // 1 hour in milliseconds
    private static final long MIN_TIME_GAP_MS = 1000;                // 1 second in milliseconds
    
    // Beijing area bounds for location validation
    private static final double MIN_LAT = 39.4;
    private static final double MAX_LAT = 40.4;
    private static final double MIN_LON = 115.8;
    private static final double MAX_LON = 117.4;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance and state recovery
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(300000);
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoints"));
        
        // High-throughput configuration - FIXED: Match TaskManager slots
        env.setParallelism(4);  // Match available TaskManager slots (was 24)
        env.getConfig().setAutoWatermarkInterval(500);  // More frequent watermarks
        env.getConfig().enableObjectReuse();
        env.getConfig().setLatencyTrackingInterval(-1);  // Disable latency tracking

        // Kafka source configuration
        KafkaSource<TaxiLocation> source = KafkaSource.<TaxiLocation>builder()
            .setBootstrapServers("kafka:29092")
            .setTopics("taxi-locations")
            .setGroupId("flink-taxi")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new TaxiLocationDeserializer())
            .setProperty("partition.discovery.interval.ms", "30000")  // Dynamic partition discovery
            .build();

        // Watermark strategy with optimized settings
        WatermarkStrategy<TaxiLocation> watermarkStrategy = WatermarkStrategy
            .<TaxiLocation>forBoundedOutOfOrderness(Duration.ofSeconds(15))
            .withTimestampAssigner((element, recordTimestamp) -> {
                try {
                    return FastDateFormat.parse(element.getTimestamp());
                } catch (ParseException e) {
                    log.warn("Failed to parse timestamp for taxi {}: {}", element.getTaxiId(), element.getTimestamp());
                    return System.currentTimeMillis();
                }
            })
            .withIdleness(Duration.ofMinutes(5));  // Longer idleness timeout

        // Main stream
        DataStream<TaxiLocation> rawLocationStream = env.fromSource(
            source, 
            watermarkStrategy,
            "Kafka Source"
        );

        // LOCATION FILTERING ====================================================
        // Filter out taxis that are outside the 15km radius with stateful tracking
        SingleOutputStreamOperator<TaxiLocation> locationStream = rawLocationStream
            .keyBy(TaxiLocation::getTaxiId)
            .process(new ZoneFilter())
            .name("ZoneFilter")
            .uid("zone-filter");

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
        
        // Handle zone exit alerts from the filter
        DataStream<String> zoneExitAlerts = locationStream
            .getSideOutput(ZONE_EXIT_ALERTS);
        
        zoneExitAlerts.addSink(new LogSink("ZONE EXIT ALERT"));
        
        // Handle validation alerts from all processors
        DataStream<String> validationAlerts = locationStream
            .getSideOutput(VALIDATION_ALERTS);
        
        validationAlerts.addSink(new LogSink("VALIDATION ALERT"));
        
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

    // Data Validation Utilities =================================================
    private static class DataValidator {
        
        /**
         * Validate location data for basic integrity and Beijing area bounds
         */
        public static boolean validateLocation(TaxiLocation location) {
            if (location == null) return false;
            
            double lat = location.getLatitude();
            double lon = location.getLongitude();
            
            // Check for invalid coordinates
            if (Double.isNaN(lat) || Double.isNaN(lon) || 
                Double.isInfinite(lat) || Double.isInfinite(lon)) {
                return false;
            }
            
            // Check if within valid lat/lon ranges
            if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
                return false;
            }
            
            // Check if within Beijing area
            if (lat < MIN_LAT || lat > MAX_LAT || lon < MIN_LON || lon > MAX_LON) {
                return false;
            }
            
            // Check timestamp validity
            if (location.getTimestamp() == null || location.getTimestamp().trim().isEmpty()) {
                return false;
            }
            
            try {
                Date timestamp = FastDateFormat.FORMATTER.get().parse(location.getTimestamp());
                long now = System.currentTimeMillis();
                long timestampMs = timestamp.getTime();
                
                // Check if timestamp is reasonable (not too far in future, allow historical data)
                if (timestampMs > now + 3600000) {  // Only reject future timestamps
                    return false;
                }
                // Remove the old data check to allow historical taxi data (2008)
            } catch (ParseException e) {
                return false;
            }
            
            return true;
        }
        
        /**
         * Validate speed value
         */
        public static boolean validateSpeed(double speed) {
            return !Double.isNaN(speed) && !Double.isInfinite(speed) && 
                   speed >= MIN_REALISTIC_SPEED && speed <= MAX_TAXI_SPEED;
        }
        
        /**
         * Validate distance value
         */
        public static boolean validateDistance(double distance) {
            return !Double.isNaN(distance) && !Double.isInfinite(distance) && 
                   distance >= 0 && distance <= MAX_REALISTIC_DISTANCE;
        }
        
        /**
         * Validate segment distance between two points
         */
        public static boolean validateSegmentDistance(double segmentDistance) {
            return !Double.isNaN(segmentDistance) && !Double.isInfinite(segmentDistance) && 
                   segmentDistance >= MIN_SEGMENT_DISTANCE && segmentDistance <= MAX_SEGMENT_DISTANCE;
        }
        
        /**
         * Validate time gap between two timestamps
         */
        public static boolean validateTimeGap(String timestamp1, String timestamp2) {
            try {
                long time1 = FastDateFormat.parse(timestamp1);
                long time2 = FastDateFormat.parse(timestamp2);
                long timeDiff = Math.abs(time2 - time1);
                return timeDiff >= MIN_TIME_GAP_MS && timeDiff <= MAX_TIME_GAP_MS;
            } catch (ParseException e) {
                return false;
            }
        }
        
        /**
         * Generate validation failure message
         */
        public static String getValidationMessage(String type, String taxiId, String reason) {
            return String.format("❌ VALIDATION: %s - Taxi %s - %s", type, taxiId, reason);
        }
    }

    // Zone Filter ===============================================================
    public static class ZoneFilter 
        extends KeyedProcessFunction<String, TaxiLocation, TaxiLocation> {
        
        private transient ValueState<Boolean> isInZone;
        private final StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.hours(STATE_TTL_HOURS))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanupInRocksdbCompactFilter(1000)
            .build();

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>(
                "isInZone", 
                Boolean.class
            );
            descriptor.enableTimeToLive(ttlConfig);
            isInZone = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
            TaxiLocation location, 
            Context context, 
            Collector<TaxiLocation> out
        ) throws Exception {
            
            // VALIDATION STEP 1: Basic data validation
            if (!DataValidator.validateLocation(location)) {
                context.output(VALIDATION_ALERTS, 
                    DataValidator.getValidationMessage("LOCATION", location.getTaxiId(), 
                        "Invalid location data - coordinates, timestamp, or Beijing bounds"));
                return; // Skip processing invalid data
            }
            
            double distance = Haversine.computeDistance(
                CENTER_LAT, CENTER_LON,
                location.getLatitude(), location.getLongitude()
            );
            
            Boolean wasInZone = isInZone.value();
            boolean currentlyInZone = distance <= MAX_RADIUS;
            
            if (currentlyInZone) {
                // Taxi is within the zone - process the data
                out.collect(location);
                isInZone.update(true);
                
                // Log when taxi enters the zone for the first time
                if (wasInZone == null || !wasInZone) {
                    log.info("Taxi {} entered the tracking zone. Distance: {:.2f} km", 
                            location.getTaxiId(), distance);
                }
            } else {
                // Taxi is outside the zone
                if (wasInZone != null && wasInZone) {
                    // Taxi just left the zone - log and clean up state
                    log.info("Taxi {} left the tracking zone. Distance: {:.2f} km. Discarding future data.", 
                            location.getTaxiId(), distance);
                    
                    // Emit zone exit alert
                    context.output(ZONE_EXIT_ALERTS,
                        "🚫 Taxi " + location.getTaxiId() + 
                        " permanently left tracking zone. Distance: " + String.format("%.2f", distance) + " km. No longer tracking."
                    );
                    
                    isInZone.clear(); // Clear state to save memory
                }
                // Don't emit the location data for taxis outside the zone
            }
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

            // VALIDATION STEP 2: Time gap validation
            if (!DataValidator.validateTimeGap(previous.getTimestamp(), current.getTimestamp())) {
                context.output(SPEED_ALERTS, 
                    DataValidator.getValidationMessage("TIME_GAP", current.getTaxiId(), 
                        "Invalid time gap between location updates"));
                return;
            }

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
            
            // VALIDATION STEP 3: Segment distance validation
            if (!DataValidator.validateSegmentDistance(dist)) {
                context.output(SPEED_ALERTS, 
                    DataValidator.getValidationMessage("SEGMENT_DISTANCE", current.getTaxiId(), 
                        String.format("Invalid segment distance: %.2f km", dist)));
                return;
            }
            
            double hours = timeDiff / 3600000.0;
            double speed = dist / hours;

            // VALIDATION STEP 4: Speed validation
            if (!DataValidator.validateSpeed(speed)) {
                context.output(SPEED_ALERTS, 
                    DataValidator.getValidationMessage("SPEED", current.getTaxiId(), 
                        String.format("Invalid speed: %.2f km/h", speed)));
                return;
            }

            // Validate and cap speed (keep existing logic for compatibility)
            if (speed > MAX_TAXI_SPEED) {
                log.debug("Capped speed for taxi {}: {:.2f} km/h", current.getTaxiId(), speed);
                speed = MAX_TAXI_SPEED;
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
            
            // VALIDATION STEP 5: Speed input validation (additional check)
            if (!DataValidator.validateSpeed(speed.getSpeed())) {
                context.output(SPEED_ALERTS, 
                    DataValidator.getValidationMessage("AVG_SPEED_INPUT", speed.getTaxiId(), 
                        String.format("Invalid speed input for average calculation: %.2f km/h", speed.getSpeed())));
                return;
            }
            
            Tuple2<Integer, Double> current = speedStats.value();
            if (current == null) {
                current = Tuple2.of(0, 0.0);
            }

            int count = current.f0 + 1;
            double total = current.f1 + speed.getSpeed();
            double avg = total / count;

            // VALIDATION STEP 6: Average speed validation
            if (Double.isNaN(avg) || avg > MAX_REALISTIC_AVG_SPEED) {
                if (Double.isNaN(avg)) {
                    log.warn("NaN in average speed for taxi {}", speed.getTaxiId());
                    speedStats.clear();
                } else {
                    context.output(SPEED_ALERTS, 
                        DataValidator.getValidationMessage("AVG_SPEED", speed.getTaxiId(), 
                            String.format("Average speed too high: %.2f km/h", avg)));
                }
                return;
            }

            out.collect(new TaxiAverageSpeed(speed.getTaxiId(), avg));
            speedStats.update(Tuple2.of(count, total));
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
                
                // VALIDATION STEP 7: Segment distance validation (additional check)
                if (!DataValidator.validateSegmentDistance(segment)) {
                    context.output(VALIDATION_ALERTS, 
                        DataValidator.getValidationMessage("DISTANCE_SEGMENT", current.getTaxiId(), 
                            String.format("Invalid distance segment: %.2f km", segment)));
                    return;
                }
                
                totalDistance += segment;
            }

            // VALIDATION STEP 8: Total distance validation
            if (!DataValidator.validateDistance(totalDistance)) {
                context.output(VALIDATION_ALERTS, 
                    DataValidator.getValidationMessage("TOTAL_DISTANCE", current.getTaxiId(), 
                        String.format("Invalid total distance: %.2f km", totalDistance)));
                        
                // Reset distance to prevent further anomalies
                totalDistance = 0.0;
                accumulatedDistance.clear();
                lastKnownLocation.clear();
                log.warn("Reset distance tracking for taxi {} due to anomaly", current.getTaxiId());
                return;
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