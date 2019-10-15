package org.dav.vehicle_rider.messages;

import java.math.BigDecimal;
import java.util.UUID;

public interface VehicleMessageWithOwnerDetails {
    public UUID getOwnerId();
    public void setAuthCurrency(String authCurrency);
    public void setFiatCurrencyCode(String fiatCurrencyCode);
    public void setPricingUsingFiat(boolean pricingUsingFiat);
    public void setCommissionFactor(BigDecimal commissionFactor);
    public void setPaymentDAVFactor(BigDecimal paymentDAVFactor);
    public void setCompanyName(String companyName);
}
