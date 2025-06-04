package com.a6.taxi.dto;

import lombok.Getter;

public class TaxiAverageSpeed {
    private String taxi_id;
    @Getter
    private double averageSpeed;

    public TaxiAverageSpeed() {}

    public TaxiAverageSpeed(String taxi_id, double averageSpeed) {
        this.taxi_id = taxi_id;
        this.averageSpeed = averageSpeed;
    }

    public String getTaxi_id() {
        return taxi_id;
    }

}
