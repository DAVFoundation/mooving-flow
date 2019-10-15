package org.dav.vehicle_rider.messages;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.UUID;

public class OwnerDetailsForPayment implements Serializable {

    private static final long serialVersionUID = 1L;

    private UUID _ownerId;
    private String _vendor;
    private BigDecimal _commissionFactor;
    private BigDecimal _davBalanceBase;
    private BigDecimal _davBalanceDelta;

    public OwnerDetailsForPayment(UUID ownerId, String vendor, BigDecimal commissionFactor, BigDecimal davBalanceBase) {
        this._ownerId = ownerId;
        this._vendor = vendor;
        this._commissionFactor = commissionFactor;
        this._davBalanceBase = davBalanceBase;
    }

    public UUID getOwnerId() {
        return _ownerId;
    }

    public String getVendor() {
        return _vendor;
    }

    public BigDecimal getCommissionFactor() {
        return _commissionFactor;
    }

    public BigDecimal getDavBalanceBase() {
        return _davBalanceBase;
    }

    public BigDecimal getDavBalanceDelta() {
        return _davBalanceDelta;
    }

    public void setDavBalanceDelta(BigDecimal davBalanceDelta) {
        this._davBalanceDelta = davBalanceDelta;
    }
}