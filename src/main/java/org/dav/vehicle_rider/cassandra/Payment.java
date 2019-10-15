package org.dav.vehicle_rider.cassandra;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.api.services.bigquery.model.TableRow;

@Table(name = "payments", keyspace = "vehicle_rider")
public class Payment implements Serializable {

    private static final long serialVersionUID = 1L;

    @Column(name = "owner_id")
    public UUID ownerId;

    @PartitionKey
    @Column(name = "rider_id")
    public UUID riderId;

    @Column(name = "transaction_time")
    public Date transactionTime;

    @Column(name = "transaction_id")
    public String transactionId;

    @Column(name = "vaulted_shopper_id")
    public String vaultedShopperId;

    @Column(name = "vendor_id")
    public String vendor;

    @Column(name = "commission_factor")
    public BigDecimal commissionFactor;

    @Column(name = "commission_factor_for_dav_reward")
    public BigDecimal commissionFactorForDavReward;

    @Column(name = "currency_code")
    public String currencyCode;

    @Column(name = "amount")
    public BigDecimal amount;

    public Payment() {

    }
    
    public Payment(UUID ownerId, UUID riderId, Date transactionTime, String transactionId, String vaultedShopperId,
            String vendor, BigDecimal commissionFactor, BigDecimal commissionFactorForDavReward, String currencyCode, BigDecimal amount) {
        this.ownerId = ownerId;
        this.riderId = riderId;
        this.transactionTime = transactionTime;
        this.transactionId = transactionId;
        this.vaultedShopperId = vaultedShopperId;
        this.vendor = vendor;
        this.commissionFactor = commissionFactor;
        this.commissionFactorForDavReward = commissionFactorForDavReward;
        this.currencyCode = currencyCode;
        this.amount = amount;
    }

    public TableRow serializeToTableRow() {
        TableRow ownerPaymentRow = new TableRow();
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        ownerPaymentRow.set("owner_id", this.ownerId);
        ownerPaymentRow.set("rider_id", this.riderId);
        ownerPaymentRow.set("transaction_time", this.transactionTime != null ? dateTimeFormat.format(this.transactionTime) : null);
        ownerPaymentRow.set("transaction_id", this.transactionId);
        ownerPaymentRow.set("vaulted_shopper_id", this.vaultedShopperId);
        ownerPaymentRow.set("vendor_id", this.vendor);
        ownerPaymentRow.set("commission_factor", this.commissionFactor);
        ownerPaymentRow.set("commission_factor_for_dav_reward", this.commissionFactorForDavReward.setScale(9, RoundingMode.DOWN));
        ownerPaymentRow.set("currency_code", this.currencyCode);
        ownerPaymentRow.set("amount", this.amount);
        return ownerPaymentRow;
    }
}
