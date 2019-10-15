package org.dav.vehicle_rider.messages;

import java.math.BigDecimal;

public interface VehicleMessageWithAuthTxn {
    public String getPaymentMethodId();
    public BigDecimal getAuthAmount();
    public String getAuthCurrency();
    public void setAuthTxn(String authTxn);
}
