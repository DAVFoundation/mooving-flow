package org.dav.vehicle_rider;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import org.dav.vehicle_rider.cassandra.RideSummary;
import org.slf4j.Logger;

import java.util.Date;
import java.util.UUID;

public class GetRideData extends CassandraDoFnBase<UpdateDavBalance.UpdateDavBalanceMessage, UpdateDavBalance.UpdateDavBalanceMessage> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "select * from vehicle_rider.rides_summary where rider_id=? and vehicle_id=? and start_time=?";

    public GetRideData(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        UpdateDavBalance.UpdateDavBalanceMessage updateDavBalanceMessage = context.element();
        UUID riderId = updateDavBalanceMessage.riderId;
        UUID vehicleId = updateDavBalanceMessage.vehicleId;
        Date startTime = updateDavBalanceMessage.startTime;
        MappingManager mappingManager = new MappingManager(_session);
        Mapper<RideSummary> mapper = mappingManager.mapper(RideSummary.class);
        BoundStatement bound = _prepared.bind(riderId, vehicleId, startTime);
        try {
            ResultSet resultSet = this.executeBoundStatement(bound);
            Result<RideSummary> results = mapper.map(resultSet);
            RideSummary rideSummary = results.one();
            if (rideSummary == null || rideSummary.paymentMethodDav != null) {
                // already updated
                context.output(null);
                return;
            }
            updateDavBalanceMessage.price = rideSummary.price;
            updateDavBalanceMessage.davRate = rideSummary.conversionRate;
            updateDavBalanceMessage.davAwarded = rideSummary.davAwarded;
            updateDavBalanceMessage.ownerId = rideSummary.ownerId;
            updateDavBalanceMessage.effectiveDate = rideSummary.EffectiveDate;
            context.output(updateDavBalanceMessage);
        } catch (Exception ex) {
            context.output(null);
            String errorMessage = String.format("Error while trying to query rides_summary: riderId=%s, vehicleId=%s, startTime=%s err: %s",
                    riderId, vehicleId, startTime, ex);
            _log.error(errorMessage, ex);
        }
    }
}
