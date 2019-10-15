package org.dav.vehicle_rider.token_payment;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.web3j.crypto.Credentials;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.methods.response.Web3ClientVersion;
import org.web3j.protocol.http.HttpService;
import org.web3j.tx.gas.DefaultGasProvider;
import java.math.BigDecimal;
import java.math.BigInteger;

public class CreateUpdateBaseBalanceMessage extends DoFn<TokenPaymentMessage, DavBalanceBaseUpdateMessage> {

    private final Logger _log;
    private Web3j _web3;
    private final String _ethNodeUrl;
    private final String _contractAddress;
    private DAVTokenABI _tokenContract;

    public CreateUpdateBaseBalanceMessage(String contractAddress, String ethNodeUrl, Logger log) {
        _ethNodeUrl = ethNodeUrl;
        _contractAddress = contractAddress;
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

        _tokenContract = getDavTokenReadOnlyContract();
    }

    public DAVTokenABI getDavTokenReadOnlyContract() {
        Credentials credentials = Credentials.create("0x0");
        return DAVTokenABI.load(_contractAddress, _web3, credentials, new DefaultGasProvider());
    }

    @Teardown
    public void teardown() throws Exception {
        _web3.shutdown();
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
        TokenPaymentMessage tokenPaymentMessage = context.element();
        String walletAddress = tokenPaymentMessage.getFromAddr();
        if(tokenPaymentMessage.transactionType == TransactionType.RiderToOwner) {
            walletAddress = tokenPaymentMessage.getToAddr();
        }
        BigInteger vinciBaseBalance = _tokenContract.balanceOf(walletAddress).send();
        BigDecimal DAVBaseBalance = new BigDecimal(vinciBaseBalance.divide(BigInteger.TEN.pow(16)));
        DAVBaseBalance = DAVBaseBalance.divide(BigDecimal.TEN.pow(2));

        DavBalanceBaseUpdateMessage davBalanceBaseUpdateMessage = new DavBalanceBaseUpdateMessage(tokenPaymentMessage.getOwnerId(), DAVBaseBalance);
        context.output(davBalanceBaseUpdateMessage);
    }
}
