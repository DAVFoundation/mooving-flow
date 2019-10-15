package org.dav.vehicle_rider;

import com.datastax.driver.core.BoundStatement;
import io.reactivex.Observable;
import org.dav.vehicle_rider.EndRideFlow.EndRideMessage;
import org.slf4j.Logger;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class GetPaymentMethod extends CassandraDoFnBase<EndRideMessage, EndRideMessage> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "select * from vehicle_rider.rides_summary where rider_id=? and vehicle_id=? and start_time=?";

    public GetPaymentMethod(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    private void isPaymentMethodDAV(EndRideMessage endRideMessage, ProcessContext context) throws InterruptedException {

        UUID riderId = endRideMessage.getRiderId();
        UUID vehicleId = endRideMessage.getVehicleId();
        Date startTime = endRideMessage.getStartTime();

        BoundStatement bound = _prepared.bind(riderId, vehicleId, startTime);
        Observable<Boolean> observable = Observable
                .interval(0, 30, TimeUnit.SECONDS)
                .map((next) -> this.executeBoundStatement(bound))
                .map((results) -> results.one())
                .filter((row) -> !row.isNull("payment_method_dav"))
                .map((row) -> row.getBool("payment_method_dav"))
                .timeout(400, TimeUnit.SECONDS)
                .take(1);

        observable.subscribe(
                (res) -> {
                    endRideMessage.setPaymentMethodDAV(res);
                    context.output(endRideMessage);
                },
                (err) -> {
                    _log.info("no payment method selected, set default payment");
                    endRideMessage.setPaymentMethodDAV(false);
                    context.output(endRideMessage);
                });

    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        EndRideMessage endRideMessage = context.element();
        _log.info(String.format("Start polling rides_summary"));
        try {
            this.isPaymentMethodDAV(endRideMessage, context);
        } catch (Exception ex) {
            endRideMessage.setPaymentMethodDAV(false);
            context.output(endRideMessage);
            String errorMessage = String.format("Error while trying to query rides_summary: riderId=%s, vehicleId=%s, startTime=%s",
                    endRideMessage.getRiderId(), endRideMessage.getVehicleId(), endRideMessage.getStartTime());
            _log.error(errorMessage, ex);
        }
    }
}
