package org.dav.vehicle_rider.messages;

import org.joda.time.LocalDate;

import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;

public interface VehicleMessageWithRideDetails {
    public void setOwnerId(UUID _ownerId);
    public void setAuthTransactionId(String authTransactionId);
    public void setStartBatteryPercentage(byte batteryPercentage);
    public void setStartTime(Date startTime);
    public String getStartGeoHash();
    public void setStartGeoHash(String geoHash);
    public void setLastGeoHash(String geoHash); // ride.lastGeoHash
    public void setLastBatteryPercentage(byte lastBatteryPercentage);
    public void setLastTime(Date lastTime);
    public Date getLastTime();
    public void setEffectiveDate(LocalDate effectiveDate);
    public void setRideDistance(BigDecimal distance);
}
