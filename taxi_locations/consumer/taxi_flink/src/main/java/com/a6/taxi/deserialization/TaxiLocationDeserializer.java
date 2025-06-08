package com.a6.taxi.deserialization;

import com.a6.taxi.dto.TaxiLocation;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class TaxiLocationDeserializer implements DeserializationSchema<TaxiLocation> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public TaxiLocation deserialize(byte[] data) throws IOException {
        return mapper.readValue(data, TaxiLocation.class);
    }

    @Override
    public boolean isEndOfStream(TaxiLocation location) {
        // Continuous stream; no end-of-stream marker
        return false;
    }

    @Override
    public TypeInformation<TaxiLocation> getProducedType() {
        return TypeInformation.of(TaxiLocation.class);
    }
}