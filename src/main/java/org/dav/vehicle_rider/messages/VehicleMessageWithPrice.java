package org.dav.vehicle_rider.messages;

import java.math.BigDecimal;

public interface VehicleMessageWithPrice {
    public BigDecimal getBasePrice();
    public BigDecimal getPricePerMinute();
    public void setBasePrice(BigDecimal basePrice);
    public void setPricePerMinute(BigDecimal pricePerMinute);
}
