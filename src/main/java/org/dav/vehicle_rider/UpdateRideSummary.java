package org.dav.vehicle_rider;

import com.datastax.driver.core.BoundStatement;
import org.slf4j.Logger;

import java.util.Date;
import java.util.UUID;

public abstract class UpdateRideSummary extends CassandraDoFnBase<UpdateDavBalance.UpdateDavBalanceMessage, UpdateDavBalance.UpdateDavBalanceMessage> {

    private static final long serialVersionUID = 1L;

    public UpdateRideSummary(String cassandraHost, String query, Logger log) {
        super(cassandraHost, query, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        UpdateDavBalance.UpdateDavBalanceMessage updateDavBalanceMessage = context.element();
        Boolean isPaymentMethodDAV = updateDavBalanceMessage.paymentMethodDav;
        UUID riderId = updateDavBalanceMessage.riderId;
        UUID vehicleId = updateDavBalanceMessage.vehicleId;
        Date startTime = updateDavBalanceMessage.startTime;
        try {
            BoundStatement bound = _prepared.bind(isPaymentMethodDAV, riderId, vehicleId, startTime);
            this.executeBoundStatement(bound);
        } catch (Exception ex) {
            _log.error(String.format("Error while trying to set payment_method to rides_summary: rider_id=%s, vehicle_id=%s, start_time=%s, payment_method_dav=%s, err: %s",
                    riderId, vehicleId, startTime, isPaymentMethodDAV, ex), ex);
        }
        context.output(updateDavBalanceMessage);
    }
}

