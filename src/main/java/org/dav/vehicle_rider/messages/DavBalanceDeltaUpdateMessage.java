package org.dav.vehicle_rider.messages;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.UUID;

public class DavBalanceDeltaUpdateMessage implements IDavBalanceDeltaUpdateMessage, Serializable {

    private static final long serialVersionUID = 1L;
    
    private UUID _ownerId;
    private BigDecimal _davBalanceDelta;
    
    public DavBalanceDeltaUpdateMessage(UUID ownerId, BigDecimal davBalanceDelta) {
        this._ownerId = ownerId;
        this._davBalanceDelta = davBalanceDelta;
    }

    public UUID getOwnerId() {
        return this._ownerId;
    }

    public BigDecimal getDavBalanceDelta() {
        return this._davBalanceDelta;
    }
}