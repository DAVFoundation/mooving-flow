package org.dav.vehicle_rider.cassandra_helpers;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import org.dav.vehicle_rider.CassandraDoFnBase;
import org.dav.vehicle_rider.messages.VehicleMessageWithOwnerDetails;
import org.dav.vehicle_rider.cassandra.Owner;
import org.slf4j.Logger;

public class GetOwnerDetailsDoFn<T extends VehicleMessageWithOwnerDetails>
        extends CassandraDoFnBase<T, T> {

    private static final long serialVersionUID = 1L;
    private static final String QUERY = "select * from vehicle_rider.owners where id=?";

    public GetOwnerDetailsDoFn(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        T unlockMessage = context.element();
        try {
            MappingManager mappingManager = new MappingManager(_session);
            Mapper<Owner> mapper = mappingManager.mapper(Owner.class);
            BoundStatement bound = _prepared.bind(unlockMessage.getOwnerId());
            ResultSet resultSet = this.executeBoundStatement(bound);
            Result<Owner> results = mapper.map(resultSet);
            for (Owner owner : results) {
                unlockMessage.setAuthCurrency(owner.fiatCurrencyCode);
                unlockMessage.setFiatCurrencyCode(owner.fiatCurrencyCode);
                unlockMessage.setPricingUsingFiat(owner.pricingUsingFiat);
                unlockMessage.setCommissionFactor(owner.commissionFactor);
                unlockMessage.setPaymentDAVFactor(owner.paymentDavFactor);
                unlockMessage.setCompanyName(owner.companyName);
                context.output(unlockMessage);
            }
        } catch (Exception ex) {
            String errorMessage = String.format("Error while trying to query owner: ownerId=%s",
                    unlockMessage.getOwnerId());
            _log.error(errorMessage, ex);
        }
    }
}
