package org.dav.vehicle_rider.cassandra_helpers;

import com.datastax.driver.core.BoundStatement;

import org.dav.vehicle_rider.CassandraDoFnBase;
import org.dav.vehicle_rider.Helpers;
import org.dav.vehicle_rider.messages.IDavBalanceDeltaUpdateMessage;
import org.slf4j.Logger;

public class UpdateOwnerDavBalanceDelta<T extends IDavBalanceDeltaUpdateMessage>
        extends CassandraDoFnBase<T, T> {
    private static final long serialVersionUID = 1L;
    private static final String QUERY = "UPDATE vehicle_rider.owner_dav_balance SET dav_balance_delta=dav_balance_delta + ? WHERE id=?";

    public UpdateOwnerDavBalanceDelta(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        T davBalanceDeltaUpdateMessage = context.element();
        try {
            BoundStatement bound = _prepared.bind(
                    Helpers.convertDavBalanceDeltaToCassandraValue(
                            davBalanceDeltaUpdateMessage.getDavBalanceDelta()),
                    davBalanceDeltaUpdateMessage.getOwnerId());
            this.executeBoundStatement(bound);
            context.output(context.element());
        } catch (Exception ex) {
            String errorMessage = String.format("Error while trying to update owner_dav_balance: ownerId=%s",
                    davBalanceDeltaUpdateMessage.getOwnerId());
            _log.error(errorMessage, ex);
        }
    }
}