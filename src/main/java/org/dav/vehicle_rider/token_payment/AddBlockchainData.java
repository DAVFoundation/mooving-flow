package org.dav.vehicle_rider.token_payment;

import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

import org.dav.vehicle_rider.CassandraDoFnBase;
import org.dav.vehicle_rider.cassandra.Owner;
import org.slf4j.Logger;

public class AddBlockchainData extends CassandraDoFnBase<TokenPaymentMessage, TokenPaymentMessage> {
    private static final long serialVersionUID = 1L;
    private static final String QUERY = "select * from vehicle_rider.owners where id=?";

    private String _riderAddress;
    private String _networkOperatorAddress;

    public AddBlockchainData(String cassandraHost, String riderAddress, String networkOperatorAddress, Logger log) {
        super(cassandraHost, QUERY, log);
        _riderAddress = riderAddress;
        _networkOperatorAddress = networkOperatorAddress;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        TokenPaymentMessage tokenPaymentMessage = context.element();
        UUID ownerId = tokenPaymentMessage.getOwnerId();
        TransactionType transactionType = tokenPaymentMessage.getTransactionType();
        if (ownerId != null && transactionType != TransactionType.NetworkOperatorToRider) {
            try {
                MappingManager mappingManager = new MappingManager(_session);
                Mapper<Owner> mapper = mappingManager.mapper(Owner.class);
                BoundStatement bound = _prepared.bind(ownerId);
                ResultSet resultSet = this.executeBoundStatement(bound);
                Owner owner = mapper.map(resultSet).one();
                if (tokenPaymentMessage.transactionType == TransactionType.OwnerToRider) {
                    tokenPaymentMessage.setFromAddr(owner.ethAddr);
                    tokenPaymentMessage.setToAddr(this._riderAddress);
                } else {
                    tokenPaymentMessage.setFromAddr(this._riderAddress);
                    tokenPaymentMessage.setToAddr(owner.ethAddr);
                }
            } catch (Exception ex) {
                String errorMessage = String.format("Error while trying to query owners: ownerId=%s", ownerId);
                _log.error(errorMessage, ex);
            }
        } else {
            tokenPaymentMessage.setFromAddr(this._networkOperatorAddress);
            tokenPaymentMessage.setToAddr(this._riderAddress);
        }
        context.output(tokenPaymentMessage);
    }
}