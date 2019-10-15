package org.dav.vehicle_rider.rider_payment;

import com.datastax.driver.core.*;

import org.dav.vehicle_rider.CassandraDoFnBase;
import org.dav.vehicle_rider.cassandra.RideSummary;
import org.slf4j.Logger;

public class UpdateRidePayed extends CassandraDoFnBase<RideSummary, RideSummary> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "update vehicle_rider.rides_summary set capture_transaction_id=? where rider_id=? and vehicle_id=? and start_time=?";

    public UpdateRidePayed(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        RideSummary rideSummary = context.element();
        try {
            BoundStatement bound = _prepared.bind(rideSummary.captureTransactionId, rideSummary.riderId, rideSummary.vehicleId, rideSummary.startTime);
            this.executeBoundStatement(bound);
            context.output(context.element());
        } catch (Exception ex) {
            String errorMessage = String.format(
                    "Error while trying to update rides_summary: riderId=%s, vehicleId=%s, startTime=%s",
                    rideSummary.riderId, rideSummary.vehicleId, rideSummary.startTime);
            _log.error(errorMessage, ex);
        }
    }
}
