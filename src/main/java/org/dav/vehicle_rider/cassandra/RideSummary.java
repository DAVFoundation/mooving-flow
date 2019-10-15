package org.dav.vehicle_rider.cassandra;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
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

@Table(name = "rides_summary", keyspace = "vehicle_rider")
public class RideSummary implements Serializable {

    private static final long serialVersionUID = 1L;

    @Transient
    public org.joda.time.LocalDate EffectiveDate;

    @Nullable
    @Column(name = "owner_id")
    public UUID ownerId;

    @PartitionKey(0)
    @Column(name = "rider_id")
    public UUID riderId;

    @PartitionKey(1)
    @Column(name = "vehicle_id")
    public UUID vehicleId;

    @Column(name = "start_time")
    public Date startTime;

    @Nullable
    @Column(name = "end_time")
    public Date endTime;

    @Nullable
    @Column(name = "effective_date")
    public LocalDate getEffectiveDate() {
        return LocalDate.fromYearMonthDay(EffectiveDate.getYear(), EffectiveDate.getMonthOfYear(),
                EffectiveDate.getDayOfMonth());
    }

    @Nullable
    @Column(name = "effective_date")
    public void setEffectiveDate(LocalDate localDate) {
        this.EffectiveDate = new org.joda.time.LocalDate(localDate.getMillisSinceEpoch());
    }

    @Nullable
    @Column(name = "start_geohash")
    public String startGeoHash;

    @Nullable
    @Column(name = "end_geohash")
    public String endGeoHash;

    @Nullable
    @Column(name = "parking_image_url")
    public String parkingImageUrl;

    @Nullable
    @Column(name = "rating")
    public Byte rating;

    @Nullable
    @Column(name = "price")
    public BigDecimal price;

    @Nullable
    @Column(name = "currency_code")
    public String currencyCode;

    @Nullable
    @Column(name = "conversion_rate")
    public BigDecimal conversionRate;

    @Nullable
    @Column(name = "commission_factor")
    public BigDecimal commissionFactor;

    @Nullable
    @Column(name = "commission")
    public BigDecimal commission;

    @Nullable
    @Column(name = "payment_dav_factor")
    public BigDecimal paymentDavFactor;

    @Nullable
    @Column(name = "payment_method_dav")
    public Boolean paymentMethodDav;

    @Nullable
    @Column(name = "auth_transaction_id")
    public String authTransactionId;

    @Nullable
    @Column(name = "capture_transaction_id")
    public String captureTransactionId;

    @Nullable
    @Column(name = "dav_rider_transaction")
    public String davRiderTransaction;

    @Nullable
    @Column(name = "fiat_rider_transaction")
    public String fiatRiderTransaction;

    @Nullable
    @Column(name = "dav_owner_transaction")
    public String davOwnerTransaction;

    @Nullable
    @Column(name = "fiat_owner_transaction")
    public String fiatOwnerTransaction;

    @Nullable
    @Column(name = "distance")
    public BigDecimal distance;

    @Nullable
    @Column(name = "dav_awarded")
    public BigDecimal davAwarded;

    public RideSummary() {

    }

    public RideSummary(UUID ownerId, UUID riderId, UUID vehicleId, Date startTime, Date endTime,
            org.joda.time.LocalDate effectiveDate, String startGeoHash, String endGeoHash, String parkingImageUrl,
            Byte rating, BigDecimal price, String currencyCode, BigDecimal conversionRate, BigDecimal commissionFactor,
            BigDecimal commission, BigDecimal paymentDavFactor, String authTransactionId,
            BigDecimal distance, BigDecimal davAwarded) {

        this.ownerId = ownerId;
        this.riderId = riderId;
        this.vehicleId = vehicleId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.EffectiveDate = effectiveDate;
        this.startGeoHash = startGeoHash;
        this.endGeoHash = endGeoHash;
        this.parkingImageUrl = parkingImageUrl;
        this.rating = rating;
        this.price = price;
        this.currencyCode = currencyCode;
        this.conversionRate = conversionRate;
        this.commissionFactor = commissionFactor;
        this.commission = commission;
        this.paymentDavFactor = paymentDavFactor;
        this.paymentMethodDav = null;
        this.authTransactionId = authTransactionId;
        this.distance = distance;
        this.captureTransactionId = null;
        this.distance = distance;
        this.davAwarded = davAwarded;
    }

    public TableRow serializeToTableRow() {
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
        TableRow rideSummaryRow = new TableRow();
        rideSummaryRow.set("owner_id", this.ownerId);
        rideSummaryRow.set("rider_id", this.riderId);
        rideSummaryRow.set("vehicle_id", this.vehicleId);
        rideSummaryRow.set("start_time", this.startTime != null ? dateTimeFormat.format(this.startTime) : null);
        rideSummaryRow.set("end_time", this.endTime != null ? dateTimeFormat.format(this.endTime) : null);
        rideSummaryRow.set("effective_date",
                this.EffectiveDate != null ? this.EffectiveDate.toString(dateFormatter) : null);
        rideSummaryRow.set("start_geohash", this.startGeoHash);
        rideSummaryRow.set("end_geohash", this.endGeoHash);
        rideSummaryRow.set("parking_image_url", this.parkingImageUrl);
        rideSummaryRow.set("rating", this.rating);
        rideSummaryRow.set("price", this.price);
        rideSummaryRow.set("currency_code", this.currencyCode);
        rideSummaryRow.set("conversion_rate", this.conversionRate);
        rideSummaryRow.set("commission_factor", this.commissionFactor);
        rideSummaryRow.set("commission", this.commission);
        rideSummaryRow.set("payment_dav_factor", this.paymentDavFactor);
        rideSummaryRow.set("payment_method_dav", this.paymentMethodDav);
        rideSummaryRow.set("auth_transaction_id", this.authTransactionId);
        rideSummaryRow.set("capture_transaction_id", this.captureTransactionId);
        rideSummaryRow.set("dav_rider_transaction", this.davRiderTransaction);
        rideSummaryRow.set("fiat_rider_transaction", this.fiatRiderTransaction);
        rideSummaryRow.set("dav_owner_transaction", this.davOwnerTransaction);
        rideSummaryRow.set("fiat_owner_transaction", this.fiatOwnerTransaction);
        rideSummaryRow.set("distance", this.distance);
        rideSummaryRow.set("dav_awarded", this.davAwarded);
        return rideSummaryRow;
    }
}