package org.dav.vehicle_rider.rider_payment;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.UUID;

import org.apache.avro.reflect.Nullable;

public class TransactionBasicDetails implements Serializable {

    private static final long serialVersionUID = 1L;

    public UUID RiderId;

    public UUID OwnerId;

    @Nullable
    public String VaultedShopperId;

    @Nullable
    public String Vendor;

    @Nullable
    public BigDecimal CommissionFactor;

    @Nullable
    public BigDecimal CommissionFactorForDavReward;


    public TransactionBasicDetails(UUID riderId, UUID ownerId) {
        this.RiderId = riderId;
        this.OwnerId = ownerId;
    }
}