package org.dav.vehicle_rider.cassandra_helpers;

import java.math.BigDecimal;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;

import org.apache.beam.sdk.values.KV;
import org.dav.vehicle_rider.CassandraDoFnBase;
import org.dav.vehicle_rider.Helpers;
import org.dav.vehicle_rider.messages.OwnerDetailsForPayment;
import org.dav.vehicle_rider.cassandra.OwnerDavBalance;
import org.dav.vehicle_rider.cassandra.RideSummary;
import org.slf4j.Logger;

public class GetOwnerBalanceDelta extends
        CassandraDoFnBase<KV<OwnerDetailsForPayment, Iterable<RideSummary>>, KV<OwnerDetailsForPayment, Iterable<RideSummary>>> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "select * from vehicle_rider.owner_dav_balance where id=?";

    public GetOwnerBalanceDelta(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        OwnerDetailsForPayment ownerDetailsForPayment = context.element().getKey();
        try {
            MappingManager mappingManager = new MappingManager(_session);
            Mapper<OwnerDavBalance> mapper = mappingManager.mapper(OwnerDavBalance.class);
            BoundStatement bound = _prepared.bind(ownerDetailsForPayment.getOwnerId());
            ResultSet resultSet = this.executeBoundStatement(bound);
            Result<OwnerDavBalance> results = mapper.map(resultSet);
            OwnerDavBalance ownerDavBalance = results.one();
            BigDecimal ownerDavBalanceDelta = Helpers.convertDavBalanceDeltaFromCassandraValue(ownerDavBalance.davBalanceDelta);
            ownerDetailsForPayment.setDavBalanceDelta(ownerDavBalanceDelta);
            context.output(KV.of(ownerDetailsForPayment, context.element().getValue()));
        } catch (Exception ex) {
            String errorMessage = String.format("Error while trying to query owner dav balance delta: ownerId=%s",
                    ownerDetailsForPayment.getOwnerId());
            _log.error(errorMessage, ex);
        }
    }
}
