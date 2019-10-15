package org.dav.vehicle_rider.cassandra_helpers;

import java.util.ArrayList;
import java.util.List;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import org.dav.vehicle_rider.CassandraDoFnBase;
import org.dav.vehicle_rider.VehicleDetails;
import org.dav.vehicle_rider.VehicleList;
import org.dav.vehicle_rider.SearchVehiclesFlow.SearchVehiclesMessage;
import org.dav.vehicle_rider.cassandra.Vehicle;
import org.slf4j.Logger;

public class GetAllAvailableVehicles extends CassandraDoFnBase<SearchVehiclesMessage, VehicleList> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "select * from vehicle_rider.vehicles where status='available';";

    public GetAllAvailableVehicles(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        try {
            SearchVehiclesMessage searchMessage = context.element();
            List<String> requiredArea = searchMessage.getGeoHashes();
            short searchPrefixLength = searchMessage.getSearchPrefixLength();

            MappingManager mappingManager = new MappingManager(_session);
            Mapper<Vehicle> mapper = mappingManager.mapper(Vehicle.class);
            BoundStatement bound = _prepared.bind();
            ResultSet resultSet = this.executeBoundStatement(bound);
            Result<Vehicle> results = mapper.map(resultSet);

            List<VehicleDetails> vehicles = new ArrayList<VehicleDetails>();
            for (Vehicle vehicle : results) {
                String vehicleArea = vehicle.geoHash.substring(0, searchPrefixLength);
                if (requiredArea.contains(vehicleArea)) {
                    VehicleDetails vehicleDetails = new VehicleDetails(vehicle.id, vehicle.qrCode,
                            vehicle.batteryPercentage, vehicle.status, vehicle.model, vehicle.geoHash,
                            vehicle.operatorName , vehicle.basePrice , vehicle.pricePerMinute);
                    vehicles.add(vehicleDetails);
                }
            }
            context.output(new VehicleList(vehicles, searchMessage.getCashPrefix(), (byte) searchPrefixLength));
        } catch (Exception ex) {
            _log.error("error while trying to get vehicles for search results", ex);
        }
    }
}