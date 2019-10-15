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

@Table(name = "owner_stats_daily", keyspace = "vehicle_rider")
public class OwnerStatsDaily implements Serializable {

    private static final long serialVersionUID = 1L;

    @Transient
    public org.joda.time.LocalDate Date;

    @PartitionKey
    @Column(name = "owner_id")
    public UUID ownerId;

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
    @Column(name = "total_rides_count_accumulate")
    public int totalRidesCountAccumulate;

    @Nullable
    @Column(name = "total_dav_revenue_accumulate")
    public BigDecimal totalDavRevenueAccumulate;

    @Nullable
    @Column(name = "total_fiat_revenue_accumulate")
    public BigDecimal totalFiatRevenueAccumulate;


    public OwnerStatsDaily() {

    }

    public OwnerStatsDaily(UUID ownerId, org.joda.time.LocalDate date, String currencyCode, int totalRidesCount,
            BigDecimal totalDavRevenue, BigDecimal totalFiatRevenue, int totalRidesCountAccumulate, BigDecimal totalDavRevenueAccumulate,
                           BigDecimal totalFiatRevenueAccumulate) {
        this.ownerId = ownerId;
        this.Date = date;
        this.currencyCode = currencyCode;
        this.totalRidesCount = totalRidesCount;
        this.totalDavRevenue = totalDavRevenue;
        this.totalFiatRevenue = totalFiatRevenue;
        this.totalRidesCountAccumulate = totalRidesCountAccumulate;
        this.totalDavRevenueAccumulate = totalDavRevenueAccumulate;
        this.totalFiatRevenueAccumulate = totalFiatRevenueAccumulate;
    }

    public TableRow serializeToTableRow() {
        DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
        TableRow ownerStatsDailyRow = new TableRow();
        ownerStatsDailyRow.set("owner_id", this.ownerId);
        ownerStatsDailyRow.set("date", this.Date != null ? this.Date.toString(dateFormatter) : null);
        ownerStatsDailyRow.set("total_rides_count", this.totalRidesCount);
        ownerStatsDailyRow.set("total_dav_revenue", this.totalDavRevenue);
        ownerStatsDailyRow.set("total_fiat_revenue", this.totalFiatRevenue);
        ownerStatsDailyRow.set("currency_code", this.currencyCode);
        ownerStatsDailyRow.set("total_rides_count_accumulate", this.totalRidesCountAccumulate);
        ownerStatsDailyRow.set("total_dav_revenue_accumulate", this.totalDavRevenueAccumulate);
        ownerStatsDailyRow.set("total_fiat_revenue_accumulate", this.totalFiatRevenueAccumulate);
        return ownerStatsDailyRow;
    }
}