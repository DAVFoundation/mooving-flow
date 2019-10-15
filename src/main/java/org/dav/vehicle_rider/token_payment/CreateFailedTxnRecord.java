package org.dav.vehicle_rider.token_payment;

import org.apache.beam.sdk.transforms.DoFn;
import org.dav.vehicle_rider.cassandra.FailedTxn;
import org.dav.vehicle_rider.cassandra.PendingTxn;
import org.slf4j.Logger;
import org.web3j.crypto.Credentials;

public class CreateFailedTxnRecord extends DoFn<TokenPaymentMessage, FailedTxn> {
    private static final long serialVersionUID = 1L;
    private final Logger _log;
    private String _riderPK;
    private String _networkOpPK;
    private static final String QUERY = "select * from vehicle_rider.pending_txns where from_address=?";

    public CreateFailedTxnRecord(String riderPrivateKey, String networkOperatorPrivateKey, Logger log) {
        _log = log;
        _networkOpPK = networkOperatorPrivateKey;
        _riderPK = riderPrivateKey;
    }

    @ProcessElement
    public void ProcessElement(ProcessContext c) {
        TokenPaymentMessage txn = c.element();
        String credentialsPK = _networkOpPK;
        if (txn.transactionType == TransactionType.RiderToOwner) {
            credentialsPK = _riderPK;
        }

        String txSenderAddress = Credentials.create(credentialsPK).getAddress();

        FailedTxn pendingTxn = new FailedTxn(txSenderAddress, txn.toAddr, txn.getOwnerId(), txn.amount,
                txn.transactionType.toString());
        c.output(pendingTxn);
    }
}
