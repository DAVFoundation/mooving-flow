package org.dav.vehicle_rider.messages;

import java.math.BigDecimal;
import java.util.UUID;

public interface IDavBalanceBaseUpdateMessage {
    UUID getOwnerId();

    BigDecimal getDavBalanceBase();
}
