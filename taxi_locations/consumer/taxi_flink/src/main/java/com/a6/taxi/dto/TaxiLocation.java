package com.a6.taxi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@NoArgsConstructor
@AllArgsConstructor
public final class TaxiLocation {
    @JsonProperty("taxi_id") // <-- Fix here
    private String taxiId;
    private double latitude;
    private double longitude;
    private String timestamp;
}