package org.dav.vehicle_rider.messages;

import java.math.BigDecimal;
import java.util.UUID;

public interface VehicleMessageConversionRatesDetails {
    public UUID getRiderId();
    public String getFiatCurrencyCode();
    public void setConversionRate(BigDecimal conversionRate);
}
