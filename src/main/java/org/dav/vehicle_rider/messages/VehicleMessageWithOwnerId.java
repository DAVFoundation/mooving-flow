package org.dav.vehicle_rider.messages;

import java.util.UUID;

public interface VehicleMessageWithOwnerId {
    public UUID getOwnerId();
    public void setOwnerId(UUID ownerId);
}
