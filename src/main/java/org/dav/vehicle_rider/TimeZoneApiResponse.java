package org.dav.vehicle_rider;

import java.io.Serializable;

import com.google.gson.annotations.SerializedName;

public class TimeZoneApiResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    @SerializedName("dstOffset")
    public int DstOffset;

    @SerializedName("rawOffset")
    public int RawOffset;
}