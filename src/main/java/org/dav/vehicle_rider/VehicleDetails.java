package org.dav.vehicle_rider;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.math.BigDecimal;
import java.util.UUID;

public class VehicleDetails {
    @SerializedName("qrCode")
    @Expose(deserialize=true)
    private String _qrCode;

    @SerializedName("id")
    @Expose(deserialize=true)
    private UUID _id;

    @SerializedName("batteryPercentage")
    @Expose(deserialize=true)
    private short _batteryPercentage;

    @SerializedName("status")
    @Expose(deserialize=true)
    private String _status;

    @SerializedName("model")
    @Expose(deserialize=true)
    private String _model;

    @SerializedName("geoHash")
    @Expose(deserialize=true)
    private String _geoHash;

    @SerializedName("operator_name")
    @Expose(deserialize=true)
    private String _operatorName;

    @SerializedName("base_price")
    @Expose(deserialize=true)
    private BigDecimal _basePrice;

    @SerializedName("price_per_minute")
    @Expose(deserialize=true)
    private BigDecimal _pricePerMinute;

    public VehicleDetails(UUID id, String qrCode, short batteryPercentage, String status, String model,
        String geoHash, String operatorName, BigDecimal basePrice, BigDecimal pricePerMinute) {
        _id = id;
        _qrCode = qrCode;
        _batteryPercentage = batteryPercentage;
        _status = status;
        _model = model;
        _geoHash = geoHash;
        _operatorName = operatorName;
        _basePrice = basePrice;
        _pricePerMinute = pricePerMinute;
    }

    public String toJSONString() {
        final GsonBuilder builder = new GsonBuilder();
        builder.excludeFieldsWithoutExposeAnnotation();
        final Gson gson = builder.create();
        return gson.toJson(this, VehicleDetails.class);
    }
}
