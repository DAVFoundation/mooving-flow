package org.dav.vehicle_rider;

import com.datastax.driver.core.BoundStatement;
import org.slf4j.Logger;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.UUID;

public class UpdateOwnerDavBalance extends CassandraDoFnBase<UpdateDavBalance.UpdateDavBalanceMessage, UpdateDavBalance.UpdateDavBalanceMessage> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "UPDATE vehicle_rider.owner_dav_balance SET dav_balance_delta = dav_balance_delta + ? where id=?";

    public UpdateOwnerDavBalance(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        UpdateDavBalance.UpdateDavBalanceMessage updateDavBalanceMessage = context.element();
        UUID ownerId = updateDavBalanceMessage.ownerId;
        _log.info(String.format("start update DAV balance for owner %s", ownerId));
        BigDecimal ridePrice = updateDavBalanceMessage.price;
        BigDecimal davRate = updateDavBalanceMessage.davRate;
        BigDecimal ownerDavBalanceDelta = ridePrice.divide(davRate, RoundingMode.HALF_UP);
        try {
            BoundStatement bound = _prepared.bind(ownerDavBalanceDelta.multiply(new BigDecimal(100)).longValue(), ownerId);
            this.executeBoundStatement(bound);
        } catch (Exception ex) {
            String errorMessage = String.format("Error while trying to update owner dav balance: ownerId=%s err: %s", ownerId, ex);
            _log.error(errorMessage, ex);
        }
        context.output(updateDavBalanceMessage);
    }
}
