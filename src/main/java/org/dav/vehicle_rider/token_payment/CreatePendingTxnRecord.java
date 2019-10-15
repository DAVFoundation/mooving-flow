package org.dav.vehicle_rider.token_payment;

import org.apache.beam.sdk.transforms.DoFn;
import org.dav.vehicle_rider.cassandra.PendingTxn;
import org.slf4j.Logger;
import org.web3j.crypto.Credentials;

public class CreatePendingTxnRecord extends DoFn<TokenPaymentMessage, PendingTxn> {
    private static final long serialVersionUID = 1L;
    private final Logger _log;
    private String _riderPK;
    private String _networkOpPK;
    private static final String QUERY = "select * from vehicle_rider.pending_txns where from_address=?";

    public CreatePendingTxnRecord(String riderPrivateKey, String networkOperatorPrivateKey, Logger log) {
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

        PendingTxn pendingTxn = new PendingTxn(txSenderAddress, txn.toAddr, txn.getOwnerId(), txn.amount,
                txn.transactionType.toString());
        c.output(pendingTxn);
    }
}
