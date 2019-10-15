package org.dav.vehicle_rider.messages;

import java.math.BigDecimal;
import java.util.UUID;

public interface VehicleMessageWithDetails {
    public String getQrCode();
    public void setAuthAmount(BigDecimal authAmount);
    public void setVehicleId(UUID vehicleId);
    public void setOwnerId(UUID ownerId);
    public void setDeviceId(String deviceId);
    public void setVendor(String vendor);
    public void setGeoHash(String geoHash);
    public void setBatteryPercentage(byte batteryPercentage);
}
