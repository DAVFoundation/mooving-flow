package org.dav.vehicle_rider.payment;

import com.google.gson.annotations.SerializedName;
import org.apache.avro.reflect.Nullable;

import java.io.Serializable;

public class BlueSnapInfoResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable
    @SerializedName("cardLastFourDigits")
    public String last4;

    @Nullable
    @SerializedName("cardType")
    public String brand;

    public BlueSnapInfoResponse(String last4, String brand) {
        this.last4 = last4;
        this.brand = brand;
    }
}