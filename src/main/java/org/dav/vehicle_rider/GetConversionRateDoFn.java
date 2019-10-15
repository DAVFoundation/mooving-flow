package org.dav.vehicle_rider;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.slf4j.Logger;

import java.math.BigDecimal;

public class GetConversionRateDoFn extends CassandraDoFnBase<EndRideFlow.EndRideMessage, EndRideFlow.EndRideMessage> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "SELECT * FROM vehicle_rider.dav_conversion_rate WHERE currency_code=?";

    public GetConversionRateDoFn(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        EndRideFlow.EndRideMessage lockMessage = context.element();
        try {
            BoundStatement bound = _prepared.bind(lockMessage.getFiatCurrencyCode());
            ResultSet resultSet = this.executeBoundStatement(bound);
            Row conversionRate = resultSet.one();
            lockMessage.setConversionRate(conversionRate.getDecimal("price"));
        } catch (Exception ex) {
            String errorMessage = String.format("Error while trying to query conversion rate: rideId=%s",
                    lockMessage.getRiderId());
            _log.error(errorMessage, ex);
            // set default value instead of failing
            lockMessage.setConversionRate(BigDecimal.ONE);
        }
        context.output(lockMessage);
    }
}