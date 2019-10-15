package org.dav.vehicle_rider.cassandra;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.reflect.Nullable;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

@Table(name = "rides_summary_effective_date", keyspace = "vehicle_rider")
public class RideSummaryEffectiveDate implements Serializable {

    private static final long serialVersionUID = 1L;

    @Transient
    public org.joda.time.LocalDate EffectiveDate;

    @Nullable
    @PartitionKey
    @Column(name = "effective_date")
    public LocalDate getEffectiveDate() {
        return LocalDate.fromYearMonthDay(EffectiveDate.getYear(), EffectiveDate.getMonthOfYear(),
                EffectiveDate.getDayOfMonth());
    }

    @Nullable
    @PartitionKey
    @Column(name = "effective_date")
    public void setEffectiveDate(LocalDate localDate) {
        this.EffectiveDate = new org.joda.time.LocalDate(localDate.getMillisSinceEpoch());
    }

    @Nullable
    @Column(name = "owner_id")
    public UUID ownerId;

    @Column(name = "rider_id")
    public UUID riderId;

    @Column(name = "vehicle_id")
    public UUID vehicleId;

    @Column(name = "start_time")
    public Date startTime;

    @Nullable
    @Column(name = "end_time")
    public Date endTime;

    @Column(name = "parking_image_url")
    public String parkingImageUrl;

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
    @Column(name = "rating")
    public byte rating;

    public RideSummaryEffectiveDate() {

    }

    public RideSummaryEffectiveDate(RideSummary rideSummary) {
        this(rideSummary.ownerId, rideSummary.riderId, rideSummary.vehicleId, rideSummary.startTime,
                rideSummary.endTime, rideSummary.parkingImageUrl, rideSummary.EffectiveDate, rideSummary.price,
                rideSummary.currencyCode, rideSummary.conversionRate, rideSummary.commissionFactor,
                rideSummary.commission, rideSummary.paymentDavFactor, rideSummary.paymentMethodDav);
    }

    public RideSummaryEffectiveDate(UUID ownerId, UUID riderId, UUID vehicleId, Date startTime, Date endTime,
            String parkingImageUrl, org.joda.time.LocalDate effectiveDate, BigDecimal price, String currencyCode,
            BigDecimal conversionRate, BigDecimal commissionFactor, BigDecimal commission, BigDecimal paymentDavFactor,
            Boolean paymentMethodDav) {

        this.ownerId = ownerId;
        this.riderId = riderId;
        this.vehicleId = vehicleId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.EffectiveDate = effectiveDate;
        this.parkingImageUrl = parkingImageUrl;
        this.price = price;
        this.currencyCode = currencyCode;
        this.conversionRate = conversionRate;
        this.commissionFactor = commissionFactor;
        this.commission = commission;
        this.paymentDavFactor = paymentDavFactor;
        this.paymentMethodDav = paymentMethodDav;
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
        rideSummaryRow.set("parking_image_url", this.parkingImageUrl);
        rideSummaryRow.set("price", this.price);
        rideSummaryRow.set("currency_code", this.currencyCode);
        rideSummaryRow.set("conversion_rate", this.conversionRate);
        rideSummaryRow.set("commission_factor", this.commissionFactor);
        rideSummaryRow.set("commission", this.commission);
        rideSummaryRow.set("payment_dav_factor", this.paymentDavFactor);
        rideSummaryRow.set("payment_method_dav", this.paymentMethodDav);
        return rideSummaryRow;
    }
}