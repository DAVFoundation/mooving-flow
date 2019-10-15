package org.dav.vehicle_rider.cassandra_helpers;

import java.math.BigDecimal;
import java.util.UUID;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;

import org.apache.beam.sdk.values.KV;
import org.dav.vehicle_rider.CassandraDoFnBase;
import org.dav.vehicle_rider.messages.OwnerDetailsForPayment;
import org.dav.vehicle_rider.cassandra.Owner;
import org.dav.vehicle_rider.cassandra.RideSummary;
import org.slf4j.Logger;

public class GetOwnerDetailsForPayment extends
        CassandraDoFnBase<KV<String, Iterable<RideSummary>>, KV<OwnerDetailsForPayment, Iterable<RideSummary>>> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "select * from vehicle_rider.owners where id=?";

    public GetOwnerDetailsForPayment(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        String ownerId = context.element().getKey();
        try {
            MappingManager mappingManager = new MappingManager(_session);
            Mapper<Owner> mapper = mappingManager.mapper(Owner.class);
            BoundStatement bound = _prepared.bind(UUID.fromString(ownerId));
            ResultSet resultSet = this.executeBoundStatement(bound);
            Result<Owner> results = mapper.map(resultSet);
            Owner owner = results.one();
            OwnerDetailsForPayment ownerDetailsForPayment = new OwnerDetailsForPayment(owner.id, owner.vendor,
                    owner.commissionFactor, owner.davBalanceBase != null ? owner.davBalanceBase : BigDecimal.ZERO);
            context.output(KV.of(ownerDetailsForPayment, context.element().getValue()));
        } catch (Exception ex) {
            String errorMessage = String.format("Error while trying to query owner: ownerId=%s", ownerId);
            _log.error(errorMessage, ex);
        }
    }
}
