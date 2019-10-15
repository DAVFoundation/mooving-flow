package org.dav.vehicle_rider.BlueSnapRequests;

import java.math.BigDecimal;

import com.google.gson.annotations.SerializedName;

public class AuthCaptureRequest extends BlueSnapRequestBase {

    private static final long serialVersionUID = 1L;

    @SerializedName("vaultedShopperId")
    public String vaultedShopperId;

    @SerializedName("amount")
    public BigDecimal amount;
    
    @SerializedName("currency")
    public String currency;

    @SerializedName("vendorsInfo")
    public VendorsInfo vendorsInfo;

    public AuthCaptureRequest(String vaultedShopperId, String currency, BigDecimal amount, String vendor, BigDecimal commissionPercent) {
        super("AUTH_CAPTURE");
        this.vaultedShopperId = vaultedShopperId;
        this.currency = currency;
        this.amount = amount;
        this.vendorsInfo = new VendorsInfo(vendor, commissionPercent);
    }

}