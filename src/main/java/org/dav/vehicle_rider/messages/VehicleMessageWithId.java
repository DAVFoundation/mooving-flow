package org.dav.vehicle_rider.messages;

import java.util.UUID;

public interface VehicleMessageWithId {
    public UUID getVehicleId();
    public void setVehicleId(UUID vehicleId);
}
