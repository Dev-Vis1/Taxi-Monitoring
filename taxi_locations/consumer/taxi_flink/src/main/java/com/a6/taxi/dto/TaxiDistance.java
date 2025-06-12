package com.a6.taxi.dto;

public class TaxiDistance {
    private String taxiId;
    private double distance;

    public TaxiDistance() {
    }

    public TaxiDistance(String taxiId, double distance) {
        this.taxiId = taxiId;
        this.distance = distance;
    }

    public String getTaxiId() {
        return taxiId;
    }

    public double getDistance() {
        return distance;
    }
}