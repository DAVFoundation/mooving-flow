package org.dav.vehicle_rider;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.dav.vehicle_rider.EndRideFlow.EndRideMessage;
import org.slf4j.Logger;

import java.math.BigDecimal;

public class IsDavPaymentAllowed extends CassandraDoFnBase<EndRideMessage, EndRideMessage> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "select * from vehicle_rider.riders where id=?";

    public IsDavPaymentAllowed(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        EndRideMessage endRideMessage = context.element();
        try {
            BoundStatement bound = _prepared.bind(endRideMessage.getRiderId());
            ResultSet resultSet = this.executeBoundStatement(bound);
            Row rider = resultSet.one();
            BigDecimal davBalance = rider.getDecimal("dav_balance");
            if (davBalance == null) {
                davBalance = BigDecimal.ZERO;
            }
            BigDecimal davRate = endRideMessage.getConversionRate();
            BigDecimal totalPrice = endRideMessage.getRidePrice();
            Boolean isDavPaymentAllowed = davBalance.multiply(davRate).compareTo(totalPrice) > -1;
            endRideMessage.setPaymentMethodDAV(isDavPaymentAllowed);
        } catch (Exception ex) {
            endRideMessage.setPaymentMethodDAV(false);
            String errorMessage = String.format("Error while trying to query rider: riderId=%s",
                    endRideMessage.getRiderId());
            _log.error(errorMessage, ex);
        }
        context.output(endRideMessage);
    }
}
