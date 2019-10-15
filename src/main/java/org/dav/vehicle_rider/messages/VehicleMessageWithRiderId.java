package org.dav.vehicle_rider.messages;

import java.util.UUID;

public interface VehicleMessageWithRiderId {
    public UUID getRiderId();
    public void setRiderId(UUID riderId);
}
