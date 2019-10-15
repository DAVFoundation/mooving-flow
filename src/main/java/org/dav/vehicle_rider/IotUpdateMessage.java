package org.dav.vehicle_rider;

import java.io.Serializable;
import java.util.Date;
import javax.annotation.Nullable;

public class IotUpdateMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    public String vendor;

    public String deviceId;

    @Nullable
    public Integer interval;

    public Date timestamp;

    @Nullable
    public Float batteryVoltage;

    @Nullable
    public Float batteryPercentage;

    @Nullable
    public Float internalBatteryVoltage;

    @Nullable
    public Float internalBatteryPercentage;

    @Nullable
    public Float longitude;

    @Nullable
    public Float latitude;

    public String geohash;

    @Nullable
    public Float heading;

    @Nullable
    public Float altitude;

    public Boolean alarm;

    public String state;
}
