package org.dav.vehicle_rider.cassandra;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.math.BigDecimal;
import java.util.UUID;

@Table(name = "vehicles_qr_code", keyspace = "vehicle_rider")
public class VehicleQrCode extends Vehicle {

    private static final long serialVersionUID = 1L;

    @PartitionKey
    @Column(name = "qr_code")
    public String qrCode;
    
    @Column(name = "id")
    public UUID id;
    
    @Column(name = "owner_id")
    public UUID ownerId;

    @Column(name = "status")
    public String status;

    @Column(name = "battery_percentage")
    public byte batteryPercentage;

    @Column(name = "model")
    public String model;

    @Column(name = "device_id")
    public String deviceId;

    @Column(name = "base_price")
    public BigDecimal basePrice;

    @Column(name = "price_per_minute")
    public BigDecimal pricePerMinute;

    @Column(name = "geo_hash")
    public String geoHash;

    @Column(name = "geo_hash_1")
    public String geoHash1;

    @Column(name = "geo_hash_2")
    public String geoHash2;

    @Column(name = "geo_hash_3")
    public String geoHash3;

    @Column(name = "geo_hash_4")
    public String geoHash4;

    @Column(name = "geo_hash_5")
    public String geoHash5;

    @Column(name = "geo_hash_6")
    public String geoHash6;

    @Column(name = "geo_hash_7")
    public String geoHash7;

    @Column(name = "geo_hash_8")
    public String geoHash8;

    @Column(name = "geo_hash_9")
    public String geoHash9;

    @Column(name = "geo_hash_10")
    public String geoHash10;

    @Column(name = "operator_name")
    public String operatorName;

    @Column(name = "vendor")
    public String vendor;

    @Column(name = "device_state")
    public String deviceState;

    @Column(name = "battery_voltage_factor_alpha")
    public BigDecimal batteryVoltageFactorAlpha;

    @Column(name = "battery_voltage_factor_beta")
    public BigDecimal batteryVoltageFactorBeta;

    public VehicleQrCode() {
    }
}