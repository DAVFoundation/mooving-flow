package org.dav.vehicle_rider;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.slf4j.Logger;

import java.math.BigDecimal;
import java.util.UUID;

public class GetRiderDavBalance extends CassandraDoFnBase<UpdateDavBalance.UpdateDavBalanceMessage, UpdateDavBalance.UpdateDavBalanceMessage> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "select * from vehicle_rider.riders where id=?";

    public GetRiderDavBalance(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        UpdateDavBalance.UpdateDavBalanceMessage updateDavBalanceMessage = context.element();
        UUID riderId = updateDavBalanceMessage.riderId;
        try {
            BoundStatement bound = _prepared.bind(riderId);
            ResultSet resultSet = this.executeBoundStatement(bound);
            Row rider = resultSet.one();
            if (rider.isNull("dav_balance")) {
                updateDavBalanceMessage.riderDavBalance = new BigDecimal(0);
            } else {
                BigDecimal davBalance = rider.getDecimal("dav_balance");
                updateDavBalanceMessage.riderDavBalance = davBalance;
            }
        } catch (Exception ex) {
            String errorMessage = String.format("Error while trying to query rider: riderId=%s", riderId);
            _log.error(errorMessage, ex);
        }
        context.output(updateDavBalanceMessage);
    }
}
