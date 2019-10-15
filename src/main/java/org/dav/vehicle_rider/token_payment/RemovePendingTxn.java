package org.dav.vehicle_rider.token_payment;

import com.datastax.driver.core.*;
import org.dav.vehicle_rider.CassandraDoFnBase;
import org.slf4j.Logger;
import org.web3j.crypto.Credentials;

public class RemovePendingTxn
        extends CassandraDoFnBase<TokenPaymentMessage, TokenPaymentMessage> {

    private static final long serialVersionUID = 1L;
    private String _riderPK;
    private String _networkOpPK;
    private static final String QUERY = "DELETE FROM vehicle_rider.pending_txns where from_address=?";

    public RemovePendingTxn(String cassandraHost, String riderPrivateKey, String networkOperatorPrivateKey, Logger log) {
        super(cassandraHost, QUERY, log);
        _networkOpPK = networkOperatorPrivateKey;
        _riderPK = riderPrivateKey;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        TokenPaymentMessage tokenPaymentMessage = context.element();
        String credentialsPK = _networkOpPK;
        if (tokenPaymentMessage.transactionType == TransactionType.RiderToOwner) {
            credentialsPK = _riderPK;
        }
        String txSenderAddress = Credentials.create(credentialsPK).getAddress();
        try {
            BoundStatement bound = _prepared.bind(txSenderAddress);
            this.executeBoundStatement(bound);
            context.output(tokenPaymentMessage);
        } catch (Exception ex) {
            String errorMessage = String.format("Error while trying to remove pending txn: from address=%s", tokenPaymentMessage.fromAddr);
            _log.error(errorMessage, ex.toString());
        }
    }
}