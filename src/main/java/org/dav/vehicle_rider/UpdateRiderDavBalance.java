package org.dav.vehicle_rider;

import com.datastax.driver.core.BoundStatement;
import org.slf4j.Logger;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.UUID;

public class UpdateRiderDavBalance extends CassandraDoFnBase<UpdateDavBalance.UpdateDavBalanceMessage, UpdateDavBalance.UpdateDavBalanceMessage> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "UPDATE vehicle_rider.riders SET dav_balance=? where id=?";

    public UpdateRiderDavBalance(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        UpdateDavBalance.UpdateDavBalanceMessage updateDavBalanceMessage = context.element();
        UUID riderId = updateDavBalanceMessage.riderId;
        _log.info(String.format("start update DAV balance for rider %s", riderId));
        BigDecimal riderDavBalance = updateDavBalanceMessage.riderDavBalance;
        BigDecimal ridePrice = updateDavBalanceMessage.price;
        BigDecimal davRate = updateDavBalanceMessage.davRate;
        BigDecimal davAwarded = updateDavBalanceMessage.davAwarded;
        boolean paymentMethodDav = updateDavBalanceMessage.paymentMethodDav;
        if (paymentMethodDav) {
            riderDavBalance = riderDavBalance.subtract(ridePrice.divide(davRate, RoundingMode.HALF_UP));
        } else  {
            riderDavBalance = riderDavBalance.add(davAwarded);
        }
        try {
            BoundStatement bound = _prepared.bind(riderDavBalance, riderId);
            this.executeBoundStatement(bound);
        } catch (Exception ex) {
            String errorMessage = String.format("Error while trying to update rider dav balance: riderId=%s", riderId);
            _log.error(errorMessage, ex);
        }
        context.output(updateDavBalanceMessage);
    }
}
