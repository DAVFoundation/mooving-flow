package org.dav.vehicle_rider.rider_payment;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;

import org.apache.beam.sdk.values.KV;
import org.dav.vehicle_rider.CassandraDoFnBase;
import org.dav.vehicle_rider.cassandra.RideSummary;
import org.dav.vehicle_rider.cassandra.Rider;
import org.slf4j.Logger;

public class GetVaultedShopperId extends
        CassandraDoFnBase<KV<TransactionBasicDetails, Iterable<RideSummary>>, KV<TransactionBasicDetails, Iterable<RideSummary>>> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "select * from vehicle_rider.riders where id=?";

    public GetVaultedShopperId(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        TransactionBasicDetails transactionBasicDetails = context.element().getKey();
        try {
            MappingManager mappingManager = new MappingManager(_session);
            Mapper<Rider> mapper = mappingManager.mapper(Rider.class);
            BoundStatement bound = _prepared.bind(transactionBasicDetails.RiderId);
            ResultSet resultSet = this.executeBoundStatement(bound);
            Result<Rider> results = mapper.map(resultSet);
            for (Rider rider : results) {
                transactionBasicDetails.VaultedShopperId = rider.paymentMethodId;
                context.output(context.element());
            }
        } catch (Exception ex) {
            String errorMessage = String.format("Error while trying to query rider: riderId=%s",
                    transactionBasicDetails.RiderId);
            _log.error(errorMessage, ex);
        }
    }
}
