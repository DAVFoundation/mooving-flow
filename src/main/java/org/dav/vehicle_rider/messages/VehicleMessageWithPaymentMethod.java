package org.dav.vehicle_rider.messages;

import java.util.UUID;

public interface VehicleMessageWithPaymentMethod {
    public UUID getRiderId();
    public void setPaymentMethodId(String paymentMethodId);
}
