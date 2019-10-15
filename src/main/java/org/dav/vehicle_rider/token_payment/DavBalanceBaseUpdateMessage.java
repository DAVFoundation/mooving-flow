package org.dav.vehicle_rider.token_payment;

import org.dav.vehicle_rider.messages.IDavBalanceBaseUpdateMessage;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.UUID;

public class DavBalanceBaseUpdateMessage implements Serializable, IDavBalanceBaseUpdateMessage {

    private static final long serialVersionUID = 1L;
    
    private UUID _ownerId;
    private BigDecimal _davBalanceBase;
    
    public DavBalanceBaseUpdateMessage(UUID ownerId, BigDecimal davBalanceBase) {
        this._ownerId = ownerId;
        this._davBalanceBase = davBalanceBase;
    }

    @Override
    public UUID getOwnerId() {
        return this._ownerId;
    }

    @Override
    public BigDecimal getDavBalanceBase() {
        return this._davBalanceBase;
    }
}