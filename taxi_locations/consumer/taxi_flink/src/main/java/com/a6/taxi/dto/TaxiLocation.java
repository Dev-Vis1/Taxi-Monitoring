package com.a6.taxi.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public final class TaxiLocation {
    private String taxiId;
    private double latitude;
    private double longitude;
    private String timestamp;
}
