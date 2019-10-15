package org.dav.vehicle_rider.cassandra_helpers;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import org.dav.vehicle_rider.CassandraDoFnBase;
import org.dav.vehicle_rider.cassandra.VehicleQrCode;
import org.dav.vehicle_rider.messages.VehicleMessageWithDetails;
import org.slf4j.Logger;

import java.math.BigDecimal;

public class GetVehicleDetailsByQrCode<T extends VehicleMessageWithDetails> extends CassandraDoFnBase<T, T> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "select * from vehicle_rider.vehicles_qr_code where qr_code=?";

    private double defaultAuthAmountMinutes;

    public GetVehicleDetailsByQrCode(String cassandraHost, double defaultAuthAmountMinutes,
            Logger log) {
        super(cassandraHost, QUERY, log);
        this.defaultAuthAmountMinutes = defaultAuthAmountMinutes;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        try {
            MappingManager mappingManager = new MappingManager(_session);
            Mapper<VehicleQrCode> mapper = mappingManager.mapper(VehicleQrCode.class);
            T unlockMessage = context.element();
            BoundStatement bound = _prepared.bind(unlockMessage.getQrCode().toLowerCase());
            ResultSet resultSet = this.executeBoundStatement(bound);
            Result<VehicleQrCode> results = mapper.map(resultSet);
            if (results.isExhausted()) {
                _log.error("Failed to get details for vehicle", unlockMessage.getQrCode().toLowerCase());
            } else {
                VehicleQrCode vehicleQrCode = results.one();
                double basePrice = vehicleQrCode.basePrice.doubleValue();
                double perMinutePrice = vehicleQrCode.pricePerMinute.doubleValue();
                unlockMessage.setAuthAmount(
                        BigDecimal.valueOf(basePrice + perMinutePrice * defaultAuthAmountMinutes));
                unlockMessage.setVehicleId(vehicleQrCode.id);
                unlockMessage.setOwnerId(vehicleQrCode.ownerId);
                unlockMessage.setDeviceId(vehicleQrCode.deviceId);
                unlockMessage.setGeoHash(vehicleQrCode.geoHash);
                unlockMessage.setVendor(vehicleQrCode.vendor);
                unlockMessage.setBatteryPercentage(vehicleQrCode.batteryPercentage);
                context.output(unlockMessage);
            }
        } catch (Exception ex) {
            _log.error("error while trying to get vehicle details", ex);
        }
    }
}
