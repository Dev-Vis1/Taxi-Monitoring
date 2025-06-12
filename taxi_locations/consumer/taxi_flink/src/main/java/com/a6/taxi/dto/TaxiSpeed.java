package com.a6.taxi.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public final class TaxiSpeed {
    private final String taxiId;
    private final double speed;
}
