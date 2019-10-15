package org.dav.vehicle_rider.cassandra;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.api.services.bigquery.model.TableRow;

import org.apache.avro.reflect.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.UUID;

@Table(name = "vehicles", keyspace = "vehicle_rider")
public class Vehicle implements Serializable {

    private static final long serialVersionUID = 1L;

    @PartitionKey
    @Column(name = "id")
    public UUID id;

    @Nullable
    @Column(name = "owner_id")
    public UUID ownerId;

    @Nullable
    @Column(name = "status")
    public String status;

    @Nullable
    @Column(name = "battery_percentage")
    public byte batteryPercentage;

    @Nullable
    @Column(name = "model")
    public String model;

    @Nullable
    @Column(name = "qr_code")
    public String qrCode;

    @Nullable
    @Column(name = "device_id")
    public String deviceId;

    @Nullable
    @Column(name = "base_price")
    public BigDecimal basePrice;

    @Nullable
    @Column(name = "price_per_minute")
    public BigDecimal pricePerMinute;

    @Nullable
    @Column(name = "reward_base")
    public BigDecimal rewardBase;

    @Nullable
    @Column(name = "reward_factor")
    public BigDecimal rewardFactor;

    @Nullable
    @Column(name = "geo_hash")
    public String geoHash;

    @Nullable
    @Column(name = "geo_hash_1")
    public String geoHash1;

    @Nullable
    @Column(name = "geo_hash_2")
    public String geoHash2;

    @Nullable
    @Column(name = "geo_hash_3")
    public String geoHash3;

    @Nullable
    @Column(name = "geo_hash_4")
    public String geoHash4;

    @Nullable
    @Column(name = "geo_hash_5")
    public String geoHash5;

    @Nullable
    @Column(name = "geo_hash_6")
    public String geoHash6;

    @Nullable
    @Column(name = "geo_hash_7")
    public String geoHash7;

    @Nullable
    @Column(name = "geo_hash_8")
    public String geoHash8;

    @Nullable
    @Column(name = "geo_hash_9")
    public String geoHash9;

    @Nullable
    @Column(name = "geo_hash_10")
    public String geoHash10;

    @Nullable
    @Column(name = "operator_name")
    public String operatorName;

    @Nullable
    @Column(name = "vendor")
    public String vendor;

    @Nullable
    @Column(name = "device_state")
    public String deviceState;

    @Nullable
    @Column(name = "battery_voltage_factor_alpha")
    public BigDecimal batteryVoltageFactorAlpha;

    @Nullable
    @Column(name = "battery_voltage_factor_beta")
    public BigDecimal batteryVoltageFactorBeta;

    public Vehicle() {
    }

    public Vehicle(UUID ownerId) {
        this.ownerId = ownerId;
    }

    public TableRow serializeToTableRow() {
        TableRow vehicleTableRow = new TableRow();
        vehicleTableRow.set("id", this.id);
        vehicleTableRow.set("owner_id", this.ownerId);
        vehicleTableRow.set("status", this.status);
        vehicleTableRow.set("battery_percentage", this.batteryPercentage);
        vehicleTableRow.set("model", this.model);
        vehicleTableRow.set("qr_code", this.qrCode);
        vehicleTableRow.set("device_id", this.deviceId);
        vehicleTableRow.set("base_price", this.basePrice);
        vehicleTableRow.set("price_per_minute", this.pricePerMinute);
        vehicleTableRow.set("reward_base", this.rewardBase);
        vehicleTableRow.set("reward_factor", this.rewardFactor);
        vehicleTableRow.set("geo_hash", this.geoHash);
        vehicleTableRow.set("geo_hash_1", this.geoHash1);
        vehicleTableRow.set("geo_hash_2", this.geoHash2);
        vehicleTableRow.set("geo_hash_3", this.geoHash3);
        vehicleTableRow.set("geo_hash_4", this.geoHash4);
        vehicleTableRow.set("geo_hash_5", this.geoHash5);
        vehicleTableRow.set("geo_hash_6", this.geoHash6);
        vehicleTableRow.set("geo_hash_7", this.geoHash7);
        vehicleTableRow.set("geo_hash_8", this.geoHash8);
        vehicleTableRow.set("geo_hash_9", this.geoHash9);
        vehicleTableRow.set("geo_hash_10", this.geoHash10);
        vehicleTableRow.set("operator_name", this.operatorName);
        vehicleTableRow.set("vendor", this.vendor);
        vehicleTableRow.set("device_state", this.deviceState);
        vehicleTableRow.set("battery_voltage_factor_alpha", this.batteryVoltageFactorAlpha);
        vehicleTableRow.set("battery_voltage_factor_beta", this.batteryVoltageFactorBeta);
        return vehicleTableRow;
    }
}
