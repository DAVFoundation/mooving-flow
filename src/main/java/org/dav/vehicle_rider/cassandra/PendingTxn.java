package org.dav.vehicle_rider.cassandra;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;

@Table(name = "pending_txns", keyspace = "vehicle_rider")
public class PendingTxn implements Serializable {
    
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

    public PendingTxn(String fromAddress, String toAddress, UUID ownerId, BigDecimal amount, String txnType) {
        this.fromAddress = fromAddress;
        this.toAddress = toAddress;
        this.ownerId = ownerId;
        this.amount = amount;
        this.txnType = txnType;
        this.insertionTime = new Date();
    }
}