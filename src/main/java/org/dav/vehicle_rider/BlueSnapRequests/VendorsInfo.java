package org.dav.vehicle_rider.BlueSnapRequests;

import java.io.Serializable;
import java.math.BigDecimal;

import com.google.gson.annotations.SerializedName;

public class VendorsInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    @SerializedName("vendorInfo") 
    public VendorInfo[] vendorInfos;

    public VendorsInfo(String vendor, BigDecimal commissionPercent) {
        this.vendorInfos = new VendorInfo[1];
        this.vendorInfos[0] = new VendorInfo(vendor, commissionPercent);
    }
}