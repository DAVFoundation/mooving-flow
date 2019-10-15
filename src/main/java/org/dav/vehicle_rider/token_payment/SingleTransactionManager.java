package org.dav.vehicle_rider.token_payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.RawTransaction;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.methods.response.EthGetTransactionCount;
import org.web3j.protocol.core.methods.response.EthSendTransaction;
import org.web3j.protocol.exceptions.TransactionException;
import org.web3j.tx.RawTransactionManager;

import java.io.IOException;
import java.math.BigInteger;

/**
 * This is SingleTransactionManager it will retry transaction with the same nonce
 * The nonce is lazy loaded once from the latest block and does not update
 */
public class SingleTransactionManager extends RawTransactionManager {

    private static final Logger log = LoggerFactory.getLogger(SingleTransactionManager.class);
    private final Web3j _web3;
    final Credentials _credentials;
    private BigInteger nonce = null;

    public SingleTransactionManager(
            Web3j web3j, Credentials credentials, int attempts, int sleepDuration) {
        super(web3j, credentials, attempts, sleepDuration);
        _web3 = web3j;
        _credentials = credentials;
    }

    /**
     * The nonce is lazy loaded once from the latest block and does not update
     * @return BigInteger nonce
     * @throws IOException
     */
    @Override
    protected BigInteger getNonce() throws IOException {
        if(nonce == null) {
            EthGetTransactionCount ethGetTransactionCount =
                    _web3.ethGetTransactionCount(
                            _credentials.getAddress(), DefaultBlockParameterName.LATEST)
                            .send();
            nonce = ethGetTransactionCount.getTransactionCount();
        }

        return nonce;
    }

    public EthSendTransaction replaceWithZeroTransaction(BigInteger gasPrice, BigInteger gasLimit, String to, String data, BigInteger value) throws IOException {
        BigInteger nonce = getNonce();
        log.info("Replace with ZERO nonce: "+ nonce.toString());
        log.info("Replace with ZERO gasPrice: "+ gasPrice.toString());
        RawTransaction rawTransaction =
                RawTransaction.createTransaction(nonce, gasPrice, gasLimit, to, value, data);
        return signAndSend(rawTransaction);
    }
}
