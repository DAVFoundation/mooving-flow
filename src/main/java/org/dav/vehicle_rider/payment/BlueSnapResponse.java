package org.dav.vehicle_rider.payment;

import java.io.Serializable;

import com.google.gson.annotations.SerializedName;

public class BlueSnapResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    @SerializedName("transactionId")
    public String transactionId;

    public BlueSnapResponse(String transactionId) {
        this.transactionId = transactionId;
    }
}