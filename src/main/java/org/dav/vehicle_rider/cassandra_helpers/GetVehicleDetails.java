package org.dav.vehicle_rider.cassandra_helpers;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import org.dav.vehicle_rider.CassandraDoFnBase;
import org.dav.vehicle_rider.cassandra.Vehicle;
import org.dav.vehicle_rider.EndRideFlow.EndRideMessage;
import org.slf4j.Logger;

public class GetVehicleDetails extends CassandraDoFnBase<EndRideMessage, EndRideMessage> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "select * from vehicle_rider.vehicles where id=?";

    public GetVehicleDetails(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        EndRideMessage endRideMessage = context.element();
        try {
            MappingManager mappingManager = new MappingManager(_session);
            Mapper<Vehicle> mapper = mappingManager.mapper(Vehicle.class);
            BoundStatement bound = _prepared.bind(endRideMessage.getVehicleId());
            // TODO: if vehicleID == ffffffff-ffff-ffff-ffff-ffffffffffff don't charge for
            // ride!
            ResultSet resultSet = this.executeBoundStatement(bound);
            Result<Vehicle> results = mapper.map(resultSet);
            for (Vehicle vehicles : results) {
                endRideMessage.setOwnerId(vehicles.ownerId);
                endRideMessage.setBasePrice(vehicles.basePrice);
                endRideMessage.setPricePerMinute(vehicles.pricePerMinute);
                endRideMessage.setRewardBase(vehicles.rewardBase);
                endRideMessage.setRewardFactor(vehicles.rewardFactor);
                endRideMessage.setDeviceId(vehicles.deviceId);
                endRideMessage.setVendor(vehicles.vendor);
                context.output(endRideMessage);
            }
        } catch (Exception ex) {
            String errorMessage = String.format("Error while trying to query vehicles: rideId=%s",
                    endRideMessage.getRiderId());
            _log.error(errorMessage, ex);
        }
    }
}