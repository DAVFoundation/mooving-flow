package org.dav.vehicle_rider.messages;

import java.math.BigDecimal;
import java.util.UUID;

public interface IDavBalanceDeltaUpdateMessage {
    
    UUID getOwnerId();
    BigDecimal getDavBalanceDelta();
}