package org.dav.vehicle_rider.cassandra_helpers;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import org.dav.vehicle_rider.CassandraDoFnBase;
import org.dav.vehicle_rider.messages.VehicleMessageWithPaymentMethod;
import org.dav.vehicle_rider.cassandra.Rider;
import org.slf4j.Logger;

public class GetRiderPaymentMethod<T extends VehicleMessageWithPaymentMethod>
        extends CassandraDoFnBase<T, T> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "select * from vehicle_rider.riders where id=?";

    public GetRiderPaymentMethod(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        T unlockMessage = context.element();
        try {
            MappingManager mappingManager = new MappingManager(_session);
            Mapper<Rider> mapper = mappingManager.mapper(Rider.class);
            BoundStatement bound = _prepared.bind(unlockMessage.getRiderId());
            ResultSet resultSet = this.executeBoundStatement(bound);
            Result<Rider> results = mapper.map(resultSet);
            for (Rider rider : results) {
                unlockMessage.setPaymentMethodId(rider.paymentMethodId);
                context.output(unlockMessage);
            }
        } catch (Exception ex) {
            String errorMessage = String.format("Error while trying to query rider: riderId=%s",
                    unlockMessage.getRiderId());
            _log.error(errorMessage, ex);
        }
    }
}
