package com.a6.taxi.validators;

import com.a6.taxi.dto.TaxiLocation;
import com.a6.taxi.dto.TaxiSpeed;
import com.a6.taxi.dto.TaxiDistance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Data validation utility for taxi tracking system
 * Implements comprehensive validation rules to filter out anomalous data
 */
public class TaxiDataValidator {
    
    private static final Logger log = LoggerFactory.getLogger(TaxiDataValidator.class);
    
    // Validation thresholds
    private static final double MAX_REALISTIC_DISTANCE = 5000.0;     // km
    private static final double MAX_SEGMENT_DISTANCE = 50.0;         // km
    private static final double MIN_SEGMENT_DISTANCE = 0.001;        // km
    private static final double MAX_REALISTIC_SPEED = 120.0;         // km/h
    private static final double MIN_REALISTIC_SPEED = 0.0;           // km/h
    private static final double MAX_REALISTIC_AVG_SPEED = 80.0;      // km/h
    private static final double MAX_ACCELERATION = 50.0;             // km/h/s
    
    // Beijing area bounds
    private static final double MIN_LAT = 39.4;
    private static final double MAX_LAT = 40.4;
    private static final double MIN_LON = 115.8;
    private static final double MAX_LON = 117.4;
    
    // Time validation
    private static final long MAX_TIME_GAP_MS = 3600000;  // 1 hour
    private static final long MIN_TIME_GAP_MS = 1000;     // 1 second
    
    // Forbidden City center for radius validation
    private static final double CENTER_LAT = 39.9163;
    private static final double CENTER_LON = 116.3972;
    private static final double MAX_RADIUS_KM = 15.0;
    
    /**
     * Validate taxi location data
     */
    public static class LocationValidationResult {
        public final boolean isValid;
        public final String reason;
        public final TaxiLocation location;
        
        public LocationValidationResult(boolean isValid, String reason, TaxiLocation location) {
            this.isValid = isValid;
            this.reason = reason;
            this.location = location;
        }
    }
    
    /**
     * Validate taxi speed data
     */
    public static class SpeedValidationResult {
        public final boolean isValid;
        public final String reason;
        public final TaxiSpeed speed;
        
        public SpeedValidationResult(boolean isValid, String reason, TaxiSpeed speed) {
            this.isValid = isValid;
            this.reason = reason;
            this.speed = speed;
        }
    }
    
    /**
     * Validate taxi distance data
     */
    public static class DistanceValidationResult {
        public final boolean isValid;
        public final String reason;
        public final TaxiDistance distance;
        
        public DistanceValidationResult(boolean isValid, String reason, TaxiDistance distance) {
            this.isValid = isValid;
            this.reason = reason;
            this.distance = distance;
        }
    }
    
    /**
     * Validate location coordinates
     */
    public static LocationValidationResult validateLocation(TaxiLocation location) {
        if (location == null) {
            return new LocationValidationResult(false, "Null location", null);
        }
        
        double lat = location.getLatitude();
        double lon = location.getLongitude();
        
        // Check for invalid coordinates
        if (Double.isNaN(lat) || Double.isNaN(lon) || Double.isInfinite(lat) || Double.isInfinite(lon)) {
            return new LocationValidationResult(false, "Invalid coordinates (NaN or Infinite)", location);
        }
        
        // Check if within valid lat/lon ranges
        if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
            return new LocationValidationResult(false, "Coordinates outside valid range", location);
        }
        
        // Check if within Beijing area
        if (lat < MIN_LAT || lat > MAX_LAT || lon < MIN_LON || lon > MAX_LON) {
            return new LocationValidationResult(false, "Location outside Beijing area", location);
        }
        
        // Check timestamp validity
        if (location.getTimestamp() == null || location.getTimestamp().trim().isEmpty()) {
            return new LocationValidationResult(false, "Invalid timestamp", location);
        }
        
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date timestamp = sdf.parse(location.getTimestamp());
            
            // Check if timestamp is reasonable (not too old or in future)
            long now = System.currentTimeMillis();
            long timestampMs = timestamp.getTime();
            
            if (timestampMs > now + 3600000) { // 1 hour in future
                return new LocationValidationResult(false, "Timestamp in future", location);
            }
            
            if (now - timestampMs > 86400000L * 365) { // 1 year old
                return new LocationValidationResult(false, "Timestamp too old", location);
            }
            
        } catch (ParseException e) {
            return new LocationValidationResult(false, "Invalid timestamp format", location);
        }
        
        return new LocationValidationResult(true, "Valid location", location);
    }
    
    /**
     * Validate speed data
     */
    public static SpeedValidationResult validateSpeed(TaxiSpeed speed) {
        if (speed == null) {
            return new SpeedValidationResult(false, "Null speed", null);
        }
        
        double speedValue = speed.getSpeed();
        
        // Check for invalid speed values
        if (Double.isNaN(speedValue) || Double.isInfinite(speedValue)) {
            return new SpeedValidationResult(false, "Invalid speed (NaN or Infinite)", speed);
        }
        
        // Check speed range
        if (speedValue < MIN_REALISTIC_SPEED) {
            return new SpeedValidationResult(false, "Speed too low: " + speedValue + " km/h", speed);
        }
        
        if (speedValue > MAX_REALISTIC_SPEED) {
            return new SpeedValidationResult(false, "Speed too high: " + speedValue + " km/h", speed);
        }
        
        return new SpeedValidationResult(true, "Valid speed", speed);
    }
    
    /**
     * Validate distance data
     */
    public static DistanceValidationResult validateDistance(TaxiDistance distance) {
        if (distance == null) {
            return new DistanceValidationResult(false, "Null distance", null);
        }
        
        double distanceValue = distance.getDistance();
        
        // Check for invalid distance values
        if (Double.isNaN(distanceValue) || Double.isInfinite(distanceValue)) {
            return new DistanceValidationResult(false, "Invalid distance (NaN or Infinite)", distance);
        }
        
        // Check distance range
        if (distanceValue < 0) {
            return new DistanceValidationResult(false, "Negative distance: " + distanceValue + " km", distance);
        }
        
        if (distanceValue > MAX_REALISTIC_DISTANCE) {
            return new DistanceValidationResult(false, "Distance too high: " + distanceValue + " km", distance);
        }
        
        return new DistanceValidationResult(true, "Valid distance", distance);
    }
    
    /**
     * Validate segment distance between two locations
     */
    public static boolean validateSegmentDistance(double segmentDistance) {
        if (Double.isNaN(segmentDistance) || Double.isInfinite(segmentDistance)) {
            return false;
        }
        
        return segmentDistance >= MIN_SEGMENT_DISTANCE && segmentDistance <= MAX_SEGMENT_DISTANCE;
    }
    
    /**
     * Validate time gap between two timestamps
     */
    public static boolean validateTimeGap(String timestamp1, String timestamp2) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date1 = sdf.parse(timestamp1);
            Date date2 = sdf.parse(timestamp2);
            
            long timeDiff = Math.abs(date2.getTime() - date1.getTime());
            
            return timeDiff >= MIN_TIME_GAP_MS && timeDiff <= MAX_TIME_GAP_MS;
        } catch (ParseException e) {
            return false;
        }
    }
    
    /**
     * Validate taxi ID format
     */
    public static boolean validateTaxiId(String taxiId) {
        return taxiId != null && !taxiId.trim().isEmpty() && taxiId.matches("\\d+");
    }
    
    /**
     * Quick validation for location within tracking radius
     */
    public static boolean isWithinTrackingRadius(double lat, double lon) {
        double distance = computeDistance(CENTER_LAT, CENTER_LON, lat, lon);
        return distance <= MAX_RADIUS_KM;
    }
    
    /**
     * Compute distance using Haversine formula (same as your existing code)
     */
    private static double computeDistance(double lat1, double lon1, double lat2, double lon2) {
        double EARTH_RADIUS_KM = 6371.0;
        double latDiffRad = Math.toRadians(lat2 - lat1);
        double lonDiffRad = Math.toRadians(lon2 - lon1);
        
        double lat1Rad = Math.toRadians(lat1);
        double lat2Rad = Math.toRadians(lat2);
        
        double sinLat = Math.sin(latDiffRad / 2);
        double sinLon = Math.sin(lonDiffRad / 2);
        
        double a = sinLat * sinLat + Math.cos(lat1Rad) * Math.cos(lat2Rad) * sinLon * sinLon;
        double centralAngle = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        
        return EARTH_RADIUS_KM * centralAngle;
    }
    
    /**
     * Log validation failure
     */
    public static void logValidationFailure(String type, String taxiId, String reason) {
        log.warn("Validation failed - {}: Taxi {} - {}", type, taxiId, reason);
    }
}
