package dto;

import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor


public class TaxiAverageSpeed {
    private String taxi_id;
    private double averageSpeed;

    public TaxiAverageSpeed() {}

    public TaxiAverageSpeed(String taxi_id, double averageSpeed) {
        this.taxi_id = taxi_id;
        this.averageSpeed = averageSpeed;
    }

}
