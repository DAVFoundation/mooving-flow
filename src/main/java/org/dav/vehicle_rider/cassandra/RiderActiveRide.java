
package org.dav.vehicle_rider.cassandra;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.apache.avro.reflect.Nullable;

@Table(name = "rider_active_rides", keyspace = "vehicle_rider")
public class RiderActiveRide implements Serializable {

    private static final long serialVersionUID = 1L;

    @PartitionKey(0)
    @Column(name = "rider_id")
    public UUID riderId;

    @Column(name = "vehicle_id")
    public UUID vehicleId;

    @Column(name = "start_time")
    public Date startTime;

    @Column(name = "start_geohash")
    public String startGeoHash;

    @Column(name = "start_battery_percentage")
    public byte startBatteryPercentage;

    @Column(name = "last_time")
    @Nullable
    public Date lastTime;

    @Column(name = "last_geohash")
    @Nullable
    public String lastGeoHash;

    @Column(name = "distance")
    @Nullable
    public BigDecimal distance;

    @Column(name = "last_battery_percentage")
    @Nullable
    public byte lastBatteryPercentage;

    @Column(name = "transaction_id")
    @Nullable
    public String transactionId;

    public RiderActiveRide() {
    }

    public RiderActiveRide(UUID riderId, UUID vehicleId, String startGeoHash,
            byte startBatteryPercentage, String transactionId, BigDecimal distance) {
        this();
        this.riderId = riderId;
        this.vehicleId = vehicleId;
        this.startGeoHash = startGeoHash;
        this.startTime = new Date();
        this.startBatteryPercentage = startBatteryPercentage;
        this.transactionId = transactionId;
        this.distance = distance;
    }
}
