package org.dav.vehicle_rider.token_payment;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.UUID;

public class TokenPaymentMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    String fromAddr;

    String toAddr;

    TransactionType transactionType;

    UUID ownerId;

    BigDecimal amount;

    public TokenPaymentMessage(TransactionType transactionType, BigDecimal amount) {
        this.transactionType = transactionType;
        this.amount = amount;
    }

    public TokenPaymentMessage(TransactionType transactionType, BigDecimal amount, UUID ownerId) {
        this(transactionType, amount);
        this.ownerId = ownerId;
    }
    
    public String getFromAddr() {
        return fromAddr;
    }

    public void setFromAddr(String fromAddr) {
        this.fromAddr = fromAddr;
    }

    public String getToAddr() {
        return toAddr;
    }

    public void setToAddr(String toAddr) {
        this.toAddr = toAddr;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public UUID getOwnerId() {
        return this.ownerId;
    }

    public TransactionType getTransactionType() {
        return this.transactionType;
    }
}
