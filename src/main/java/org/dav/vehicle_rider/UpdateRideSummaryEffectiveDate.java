package org.dav.vehicle_rider;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.LocalDate;
import org.slf4j.Logger;

import java.util.Date;
import java.util.UUID;

public class UpdateRideSummaryEffectiveDate extends CassandraDoFnBase<UpdateDavBalance.UpdateDavBalanceMessage, UpdateDavBalance.UpdateDavBalanceMessage> {

    private static final long serialVersionUID = 1L;

    private static final String QUERY = "UPDATE vehicle_rider.rides_summary_effective_date SET payment_method_dav=? WHERE effective_date=? and rider_id=? and vehicle_id=? and start_time=?";

    public UpdateRideSummaryEffectiveDate(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        UpdateDavBalance.UpdateDavBalanceMessage updateDavBalanceMessage = context.element();
        Boolean isPaymentMethodDAV = updateDavBalanceMessage.paymentMethodDav;
        UUID riderId = updateDavBalanceMessage.riderId;
        UUID vehicleId = updateDavBalanceMessage.vehicleId;
        Date startTime = updateDavBalanceMessage.startTime;
        LocalDate effectiveDate = LocalDate.fromYearMonthDay(
                updateDavBalanceMessage.effectiveDate.getYear(),
                updateDavBalanceMessage.effectiveDate.getMonthOfYear(),
                updateDavBalanceMessage.effectiveDate.getDayOfMonth());
        try {
            BoundStatement bound = _prepared.bind(isPaymentMethodDAV, effectiveDate, riderId, vehicleId, startTime);
            this.executeBoundStatement(bound);
        } catch (Exception ex) {
            _log.error(String.format("Error while trying to set payment_method to rides_summary_effective_date: rider_id=%s, vehicle_id=%s, start_time=%s, payment_method_dav=%s effectiveDate=%s, err: %s",
                    riderId, vehicleId, startTime, isPaymentMethodDAV, effectiveDate, ex), ex);
        }
        context.output(updateDavBalanceMessage);
    }
}

