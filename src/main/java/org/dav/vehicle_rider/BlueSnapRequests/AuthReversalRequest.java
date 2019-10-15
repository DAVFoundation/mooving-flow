package org.dav.vehicle_rider.BlueSnapRequests;

import com.google.gson.annotations.SerializedName;

public class AuthReversalRequest extends BlueSnapRequestBase {

    private static final long serialVersionUID = 1L;

    @SerializedName("transactionId")
    public String transactionId;

    public AuthReversalRequest(String transactionId) {
        super("AUTH_REVERSAL");
        this.transactionId = transactionId;
    }

}