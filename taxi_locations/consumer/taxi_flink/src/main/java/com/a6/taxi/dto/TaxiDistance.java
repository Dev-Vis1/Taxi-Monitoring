package Dto;

public class TaxiDistance {
    private String taxi_id;
    private double distance;

    public TaxiDistance() {}

    public TaxiDistance(String taxi_id, double distance) {
        this.taxi_id = taxi_id;
        this.distance = distance;
    }

    public String getTaxi_id() {
        return taxi_id;
    }

    public double getDistance() {
        return distance;
    }
}
