package org.dav.vehicle_rider.BlueSnapRequests;

import java.io.Serializable;
import java.math.BigDecimal;

import com.google.gson.annotations.SerializedName;

public class VendorInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    @SerializedName("vendor")
    public String vendor;

    @SerializedName("commissionPercent")
    public BigDecimal commissionPercent;

    public VendorInfo(String vendor, BigDecimal commissionPercent) {
        this.vendor = vendor;
        this.commissionPercent = commissionPercent;
    }
}