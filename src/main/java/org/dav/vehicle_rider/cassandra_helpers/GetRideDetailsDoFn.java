package org.dav.vehicle_rider.cassandra_helpers;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import org.dav.vehicle_rider.CassandraDoFnBase;
import org.dav.vehicle_rider.cassandra.RiderActiveRide;
import org.dav.vehicle_rider.messages.VehicleMessageWithId;
import org.dav.vehicle_rider.messages.VehicleMessageWithRideDetails;
import org.dav.vehicle_rider.messages.VehicleMessageWithRiderId;
import org.slf4j.Logger;

import java.util.Date;

public class GetRideDetailsDoFn<T extends VehicleMessageWithRideDetails &
        VehicleMessageWithRiderId &
        VehicleMessageWithId> extends CassandraDoFnBase<T, T> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "select * from vehicle_rider.rider_active_rides where rider_id=?";

    private Logger _log;
    protected String _cassandraHost;

    public GetRideDetailsDoFn(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        T lockMessage = context.element();
        try {
            MappingManager mappingManager = new MappingManager(_session);
            Mapper<RiderActiveRide> mapper = mappingManager.mapper(RiderActiveRide.class);
            BoundStatement bound = _prepared.bind(lockMessage.getRiderId());
            ResultSet resultSet = this.executeBoundStatement(bound);
            Result<RiderActiveRide> results = mapper.map(resultSet);
            for (RiderActiveRide ride : results) {
                lockMessage.setVehicleId(ride.vehicleId);
                lockMessage.setAuthTransactionId(ride.transactionId);
                lockMessage.setStartGeoHash(ride.startGeoHash);
                lockMessage.setLastGeoHash(ride.lastGeoHash); // ride.lastGeoHash
                lockMessage.setStartBatteryPercentage(ride.startBatteryPercentage);
                lockMessage.setLastBatteryPercentage(ride.lastBatteryPercentage);
                lockMessage.setStartTime(ride.startTime);
                lockMessage.setRideDistance(ride.distance);
                lockMessage.setLastTime(new Date()); // ride.startTime.getTime().getTime()
                context.output(lockMessage);
            }
        } catch (Exception ex) {
            String errorMessage = String.format("Error while trying to query active rides: rideId=%s",
                    lockMessage.getRiderId());
            _log.error(errorMessage, ex);
        }
    }
}