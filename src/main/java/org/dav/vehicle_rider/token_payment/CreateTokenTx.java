package org.dav.vehicle_rider.token_payment;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.web3j.crypto.Credentials;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.methods.response.TransactionReceipt;
import org.web3j.protocol.core.methods.response.Web3ClientVersion;
import org.web3j.protocol.exceptions.TransactionException;
import org.web3j.protocol.http.HttpService;

import java.math.BigDecimal;
import java.math.BigInteger;

public class CreateTokenTx extends DoFn<TokenPaymentMessage, TokenPaymentMessage> {

    private static final long serialVersionUID = 1L;

    private final Logger _log;
    private Web3j _web3;
    private final String _contractAddress;
    private final String _ethNodeUrl;
    private String _riderPK;
    private String _networkOpPK;
    private TokenContractGasProvider tokenContractGasProvider;
    SingleTransactionManager transactionManager;
    private TupleTag<TokenPaymentMessage> _successfulTxnTag;
    private TupleTag<TokenPaymentMessage> _failedTxnTag;
    int transactionAttempts = 20;

    public CreateTokenTx(String riderPrivateKey, String networkOperatorPrivateKey, String contractAddress, String ethNodeUrl,
            TupleTag<TokenPaymentMessage> successfulTxnTag, TupleTag<TokenPaymentMessage> failedTxnTag, Logger log) {
        _riderPK = riderPrivateKey;
        _networkOpPK = networkOperatorPrivateKey;
        _contractAddress = contractAddress;
        _ethNodeUrl = ethNodeUrl;
        _successfulTxnTag = successfulTxnTag;
        _failedTxnTag = failedTxnTag;
        _log = log;
    }

    @Setup
    public void setup() {
        try {
            _web3 = Web3j.build(new HttpService(_ethNodeUrl));
            Web3ClientVersion web3ClientVersion = _web3.web3ClientVersion().send();
            String clientVersion = web3ClientVersion.getWeb3ClientVersion();
            _log.info("Initialized Web3 Client Version: " + clientVersion);
        } catch (Exception ex) {
            _log.error("error while trying to initialize web3 client", ex);
        }
    }

    @Teardown
    public void teardown() throws Exception {
        _web3.shutdown();
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
        TokenPaymentMessage tokenPaymentMessage = context.element();
        String credentialsPK = _networkOpPK;
        if (tokenPaymentMessage.transactionType == TransactionType.RiderToOwner) {
            credentialsPK = _riderPK;
        }
        BigDecimal txAmount = tokenPaymentMessage.getAmount();
        String from = tokenPaymentMessage.getFromAddr();
        String to = tokenPaymentMessage.getToAddr();
        DAVTokenABI davTokenABI = getDavTokenContract(credentialsPK);

        TransactionReceipt receipt = sendTx(davTokenABI, from, to, txAmount, tokenPaymentMessage.transactionType);
        if (receipt == null) {
            context.output(_failedTxnTag, tokenPaymentMessage);
        } else {
            context.output(_successfulTxnTag, tokenPaymentMessage);
        }
    }

    public BigInteger getGasPrice() {
        BigInteger gasPrice = BigInteger.ZERO;
        try {
            gasPrice = _web3.ethGasPrice().send().getGasPrice();
        } catch (Exception ex) {
            _log.error("web3 ethGasPrice failed with exception", ex);
        }
        return gasPrice;
    }

    public DAVTokenABI getDavTokenContract(String credentialsPK) {
        Credentials credentials = Credentials.create(credentialsPK); // Rider or Network Operator
        transactionManager = new SingleTransactionManager(_web3, credentials, 20, 15000);
        tokenContractGasProvider = new TokenContractGasProvider(_web3);
        return DAVTokenABI.load(_contractAddress, _web3, transactionManager, tokenContractGasProvider);
    }

    public TransactionReceipt sendTx(DAVTokenABI davTokenABI, String from, String to, BigDecimal txAmount,
            TransactionType transactionType) {
        TransactionReceipt transactionReceipt = null;
        BigInteger vinciAmount = txAmount.multiply(BigDecimal.TEN.pow(2)).toBigInteger()
                .multiply(BigInteger.TEN.pow(16));

        retryLoop: for (int i = 0; i < transactionAttempts; i++) {
            try {
                if (transactionType == TransactionType.OwnerToRider) {
                    transactionReceipt = davTokenABI.transferFrom(from, to, vinciAmount).send();
                } else {
                    transactionReceipt = davTokenABI.transfer(to, vinciAmount).send();
                }
                _log.info("Transaction was fulfilled txHash: " + transactionReceipt.getTransactionHash());
                break retryLoop;
            } catch (TransactionException ex) {
                _log.error(
                        "web3 transaction Type " + transactionType + " of " + txAmount.toString() + "DAV, "
                                + vinciAmount.toString() + " vinci from " + from + " to " + to + " failed with TransactionException ",
                        ex);
                break retryLoop;
            } catch (Exception ex) {
                _log.error(
                        "web3 transaction Type " + transactionType + " of " + txAmount.toString() + "DAV, "
                                + vinciAmount.toString() + " vinci from " + from + " to " + to + " failed with exception ",
                        ex);
            }
        }

        if(transactionReceipt == null) {
            try {
                transactionManager.replaceWithZeroTransaction(
                        tokenContractGasProvider.getGasPrice(),
                        BigInteger.valueOf(22000),
                        from, "",
                        BigInteger.ZERO
                );
            } catch (Exception ex) {
                _log.error("Failed to replace transaction with ZERO", ex);
            }
            _log.error("Transaction was not fulfilled after " + transactionAttempts + " attempts.", transactionReceipt);
        }

        return transactionReceipt;
    }
}
