package org.dav.vehicle_rider.cassandra;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.api.services.bigquery.model.TableRow;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

@Table(name = "failed_txns", keyspace = "vehicle_rider")
public class FailedTxn implements Serializable {
    
    private static final long serialVersionUID = 1L;

    @PartitionKey
    @Column(name = "from_address")
    public String fromAddress;

    @Column(name = "to_address")
    public String toAddress;

    @Column(name = "owner_id")
    public UUID ownerId;

    @Column(name= "amount")
    public BigDecimal amount;

    @Column(name = "txn_type")
    public String txnType;

    @Column(name = "insertion_time")
    public Date insertionTime;

    public FailedTxn() {

    }
    
    public FailedTxn(String fromAddress, String toAddress, UUID ownerId, BigDecimal amount, String txnType) {
        this.fromAddress = fromAddress;
        this.toAddress = toAddress;
        this.ownerId = ownerId;
        this.amount = amount;
        this.txnType = txnType;
        this.insertionTime = new Date();
    }

    public TableRow serializeToTableRow() {
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        TableRow failedTxnRow = new TableRow();
        failedTxnRow.set("from_address", this.fromAddress);
        failedTxnRow.set("to_address", this.toAddress);
        failedTxnRow.set("owner_id", this.ownerId);
        failedTxnRow.set("amount", this.amount);
        failedTxnRow.set("txn_type", this.txnType);
        failedTxnRow.set("insertion_time", this.insertionTime != null ? dateTimeFormat.format(this.insertionTime) : null);
        return failedTxnRow;
    }
}