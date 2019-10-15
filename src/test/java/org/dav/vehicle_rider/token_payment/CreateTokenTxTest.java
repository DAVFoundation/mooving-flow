package org.dav.vehicle_rider.token_payment;

import org.apache.beam.sdk.values.TupleTag;
import org.dav.Json;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.protocol.core.methods.response.TransactionReceipt;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.*;

public class CreateTokenTxTest {

    private static Logger _log = LoggerFactory.getLogger(CreateTokenTxTest.class.getName());

    // This is not a real unit test, it runs real transaction on the blockchain
//    @Test
    public void processElement() {
        TokenPaymentMessage tokenPaymentMessage = new TokenPaymentMessage(TransactionType.NetworkOperatorToRider, BigDecimal.valueOf(8L));
        String credentialsPK = "";
        TupleTag<TokenPaymentMessage> successfulTxnTag = new TupleTag<TokenPaymentMessage>() {

            private static final long serialVersionUID = 1L;
        };
        TupleTag<TokenPaymentMessage> failedTxnTag = new TupleTag<TokenPaymentMessage>() {

            private static final long serialVersionUID = 1L;
        };
        CreateTokenTx createTokenTx = new CreateTokenTx(
                "",
                credentialsPK, // network operator
                "0xd5955343425df2acc5Ca26C31Cf1298cfAefbBD8",
                "https://rinkeby.infura.io/v3/5153bcce69374a518c4afe33b05fe4d6",
                successfulTxnTag,
                failedTxnTag,
                _log);
        createTokenTx.setup();
        DAVTokenABI davTokenContract = createTokenTx.getDavTokenContract(credentialsPK);
        _log.info("gas price: " + createTokenTx.getGasPrice().toString());
        TransactionReceipt txReceipt = createTokenTx.sendTx(
                davTokenContract,
                "0x2884711D6d545CED73E835F0CC0105981927172E",
                "0x114E9991E39d53EF54E63A7c54005DC069f9C2dE",
                BigDecimal.valueOf(70.0),
                TransactionType.NetworkOperatorToRider);
        assertNotNull(txReceipt);
        _log.info("tx finished: " + txReceipt.getTransactionHash());
    }

//    @Test
    public void testInt() {
        BigDecimal d = BigDecimal.valueOf(64.0);
        BigInteger vinciFromDec = BigInteger.valueOf(d.multiply(BigDecimal.TEN.pow(18)).longValue());
        BigInteger vinciFromDec2 = d.multiply(BigDecimal.TEN.pow(2)).toBigInteger().multiply(BigInteger.TEN.pow(16));
        BigInteger n = BigInteger.valueOf(64);
        BigInteger vinciFromInt = n.multiply(BigInteger.TEN.pow(18));
        _log.info("vinciFromInt: " + vinciFromInt.toString());
        _log.info("vinciFromDec: " + vinciFromDec.toString());
        _log.info("vinciFromDec2: " + vinciFromDec2.toString());
    }
}