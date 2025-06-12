package com.a6.taxi.dto;

import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class TaxiAverageSpeed {
    private String taxiId;
    private double averageSpeed;
}