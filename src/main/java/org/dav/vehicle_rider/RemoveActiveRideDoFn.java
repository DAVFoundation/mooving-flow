package org.dav.vehicle_rider;

import com.datastax.driver.core.*;
import org.dav.vehicle_rider.CassandraDoFnBase;
import org.dav.vehicle_rider.LockVehicleFlow.LockVehicleMessage;
import org.dav.vehicle_rider.messages.VehicleMessageWithRideDetails;
import org.dav.vehicle_rider.messages.VehicleMessageWithRiderId;
import org.slf4j.Logger;

public class RemoveActiveRideDoFn<T extends VehicleMessageWithRideDetails & VehicleMessageWithRiderId>
        extends CassandraDoFnBase<T, T> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "DELETE FROM vehicle_rider.rider_active_rides where rider_id=?";

    public RemoveActiveRideDoFn(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        T lockMessage = context.element();
        try {
            BoundStatement bound = _prepared.bind(lockMessage.getRiderId());
            this.executeBoundStatement(bound);
            context.output(lockMessage);
        } catch (Exception ex) {
            String errorMessage = String.format("Error while trying to remove active rides: rideId=%s", lockMessage.getRiderId());
            _log.error(errorMessage, ex.toString());
        }
    }
}