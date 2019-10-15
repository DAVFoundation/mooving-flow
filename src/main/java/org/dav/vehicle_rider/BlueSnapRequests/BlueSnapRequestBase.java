package org.dav.vehicle_rider.BlueSnapRequests;

import java.io.Serializable;

import com.google.gson.annotations.SerializedName;

public abstract class BlueSnapRequestBase implements Serializable {

    private static final long serialVersionUID = 1L;
    
    @SerializedName("cardTransactionType")
    public String cardTransactionType;

    protected BlueSnapRequestBase(String cardTransactionType) {
        this.cardTransactionType = cardTransactionType;
    }
}