package com.a6.taxi.taxi_flink;

public class Haversine {
    private static final double EARTH_RADIUS_KM = 6371.0;
    /*
     * @param lat1 Latitude of the starting point in degrees
     * @param lon1 Longitude of the starting point in degrees
     * @param lat2 Latitude of the destination point in degrees
     * @param lon2 Longitude of the destination point in degrees
     * @return Distance between the two points in kilometers
     */
    public static double computeDistance(double lat1, double lon1, double lat2, double lon2) {
        double latDiffRad = Math.toRadians(lat2 - lat1);
        double lonDiffRad = Math.toRadians(lon2 - lon1);

        double lat1Rad = Math.toRadians(lat1);
        double lat2Rad = Math.toRadians(lat2);

        double sinLat = Math.sin(latDiffRad / 2);
        double sinLon = Math.sin(lonDiffRad / 2);

        double a = sinLat * sinLat
                 + Math.cos(lat1Rad) * Math.cos(lat2Rad) * sinLon * sinLon;

        double centralAngle = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return EARTH_RADIUS_KM * centralAngle;
    }
}
