package org.dav.vehicle_rider.BlueSnapRequests;

import java.math.BigDecimal;

import com.google.gson.annotations.SerializedName;

public class AuthRequest extends BlueSnapRequestBase {

    private static final long serialVersionUID = 1L;

    @SerializedName("vaultedShopperId")
    public String vaultedShopperId;

    @SerializedName("amount")
    public BigDecimal amount;
    
    @SerializedName("currency")
    public String currency;

    public AuthRequest(String vaultedShopperId, String currency, BigDecimal amount) {
        super("AUTH_ONLY");
        this.vaultedShopperId = vaultedShopperId;
        this.currency = currency;
        this.amount = amount;
    }
}