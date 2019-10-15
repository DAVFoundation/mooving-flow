package org.dav.vehicle_rider.cassandra;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.UUID;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;
import com.google.api.services.bigquery.model.TableRow;

import org.apache.avro.reflect.Nullable;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

@Table(name = "vehicle_stats_daily", keyspace = "vehicle_rider")
public class VehicleStatsDaily implements Serializable {

    private static final long serialVersionUID = 1L;

    @Transient
    public org.joda.time.LocalDate Date;

    @PartitionKey(0)
    @Column(name = "owner_id")
    public UUID ownerId;

    @Column(name = "vehicle_id")
    public UUID vehicleId;

    @Column(name = "date")
    public LocalDate getDate() {
        return LocalDate.fromYearMonthDay(Date.getYear(), Date.getMonthOfYear(), Date.getDayOfMonth());
    }

    @Column(name = "date")
    public void setDate(LocalDate localDate) {
        Date = new org.joda.time.LocalDate(localDate.getMillisSinceEpoch());
    }

    @Column(name = "total_rides_count")
    public int totalRidesCount;

    @Column(name = "total_dav_revenue")
    public BigDecimal totalDavRevenue;

    @Column(name = "total_fiat_revenue")
    public BigDecimal totalFiatRevenue;

    @Column(name = "currency_code")
    public String currencyCode;

    @Nullable
    @Column(name = "last_parking_image_url")
    public String lastParkingImageUrl;

    @Nullable
    @Column(name = "rating_1_count")
    public int ratingCount1 ;

    @Nullable
    @Column(name = "rating_2_count")
    public int ratingCount2;

    @Nullable
    @Column(name = "rating_3_count")
    public int ratingCount3;

    @Nullable
    @Column(name = "rating_4_count")
    public int ratingCount4;

    @Nullable
    @Column(name = "rating_5_count")
    public int ratingCount5;

    @Nullable
    @Column(name = "total_rides_count_accumulate")
    public int totalRidesCountAccumulate;

    @Nullable
    @Column(name = "total_dav_revenue_accumulate")
    public BigDecimal totalDavRevenueAccumulate;

    @Nullable
    @Column(name = "total_fiat_revenue_accumulate")
    public BigDecimal totalFiatRevenueAccumulate;


    public VehicleStatsDaily() {

    }

    public VehicleStatsDaily(UUID ownerId, org.joda.time.LocalDate date, UUID vehicleiId, String currencyCode, int totalRidesCount,
            BigDecimal totalDavRevenue, BigDecimal totalFiatRevenue, String lastParkingImageUrl, int[] feedbackRatingCount,
                             int totalRidesCountAccumulate, BigDecimal totalDavRevenueAccumulate, BigDecimal totalFiatRevenueAccumulate) {
        this.ownerId = ownerId;
        this.Date = date;
        this.vehicleId = vehicleiId;
        this.currencyCode = currencyCode;
        this.totalRidesCount = totalRidesCount;
        this.totalDavRevenue = totalDavRevenue;
        this.totalFiatRevenue = totalFiatRevenue;
        this.lastParkingImageUrl = lastParkingImageUrl;
        this.ratingCount1 = feedbackRatingCount[0];
        this.ratingCount2 = feedbackRatingCount[1];
        this.ratingCount3 = feedbackRatingCount[2];
        this.ratingCount4 = feedbackRatingCount[3];
        this.ratingCount5 = feedbackRatingCount[4];
        this.totalRidesCountAccumulate = totalRidesCountAccumulate;
        this.totalDavRevenueAccumulate = totalDavRevenueAccumulate;
        this.totalFiatRevenueAccumulate = totalFiatRevenueAccumulate;
    }

    public TableRow serializeToTableRow() {
        DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
        TableRow vehicleStatsDailyRow = new TableRow();

        vehicleStatsDailyRow.set("owner_id", this.ownerId);
        vehicleStatsDailyRow.set("vehicle_id", this.vehicleId);
        vehicleStatsDailyRow.set("date", this.Date != null ? this.Date.toString(dateFormatter) : null);
        vehicleStatsDailyRow.set("total_rides_count", this.totalRidesCount);
        vehicleStatsDailyRow.set("total_dav_revenue", this.totalDavRevenue);
        vehicleStatsDailyRow.set("total_fiat_revenue", this.totalFiatRevenue);
        vehicleStatsDailyRow.set("last_parking_image_url", lastParkingImageUrl);
        vehicleStatsDailyRow.set("currency_code", this.currencyCode);
        vehicleStatsDailyRow.set("rating_1_count", this.ratingCount1);
        vehicleStatsDailyRow.set("rating_2_count", this.ratingCount2);
        vehicleStatsDailyRow.set("rating_3_count", this.ratingCount3);
        vehicleStatsDailyRow.set("rating_4_count", this.ratingCount4);
        vehicleStatsDailyRow.set("rating_5_count", this.ratingCount5);
        vehicleStatsDailyRow.set("total_rides_count_accumulate", this.totalRidesCountAccumulate);
        vehicleStatsDailyRow.set("total_dav_revenue_accumulate", this.totalDavRevenueAccumulate);
        vehicleStatsDailyRow.set("total_fiat_revenue_accumulate", this.totalFiatRevenueAccumulate);
        return vehicleStatsDailyRow;
    }
}