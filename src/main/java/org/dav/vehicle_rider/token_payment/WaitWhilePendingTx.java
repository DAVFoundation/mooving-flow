package org.dav.vehicle_rider.token_payment;

import com.datastax.driver.core.BoundStatement;
import io.reactivex.Observable;
import org.dav.vehicle_rider.CassandraDoFnBase;
import org.slf4j.Logger;
import org.web3j.crypto.Credentials;

import java.util.concurrent.TimeUnit;


public class WaitWhilePendingTx extends CassandraDoFnBase<TokenPaymentMessage, TokenPaymentMessage> {

    private static final long serialVersionUID = 1L;
    private String _riderPK;
    private String _networkOpPK;
    private static final String QUERY = "select * from vehicle_rider.pending_txns where from_address=?";

    public WaitWhilePendingTx(String cassandraHost, String riderPrivateKey, String networkOperatorPrivateKey, Logger log) {
        super(cassandraHost, QUERY, log);
        _networkOpPK = networkOperatorPrivateKey;
        _riderPK = riderPrivateKey;
    }

    private void WaitWhilePendingTransactionToFinish(TokenPaymentMessage tokenPaymentMessage, ProcessContext context) throws InterruptedException {

        String credentialsPK = _networkOpPK;
        if (tokenPaymentMessage.transactionType == TransactionType.RiderToOwner) {
            credentialsPK = _riderPK;
        }

        String txSenderAddress = Credentials.create(credentialsPK).getAddress();
        BoundStatement bound = _prepared.bind(txSenderAddress);
        Observable<Integer> observable = Observable
                .interval(0, 30, TimeUnit.SECONDS)
                .map((next) -> this.executeBoundStatement(bound))
                .map((results) -> results.getAvailableWithoutFetching())
                .filter((rowCount) -> rowCount < 1)
                .take(1);

        observable.subscribe(
                (rowCount) -> context.output(tokenPaymentMessage),
                (err) -> {
                    _log.error("Error while polling pending transaction", err);
                    context.output(tokenPaymentMessage);
                });

    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        TokenPaymentMessage tokenPaymentMessage = context.element();
        _log.info(String.format("Start polling pending_txns"));
        try {
            this.WaitWhilePendingTransactionToFinish(tokenPaymentMessage, context);
        } catch (Exception ex) {
            context.output(tokenPaymentMessage);
            _log.error("Error while polling pending transaction", ex);
        }
    }
}
