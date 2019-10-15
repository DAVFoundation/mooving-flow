package org.dav.vehicle_rider.token_payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.protocol.Web3j;
import org.web3j.tx.gas.ContractGasProvider;

import java.math.BigInteger;

public class TokenContractGasProvider implements ContractGasProvider {

    private static final Logger _log = LoggerFactory.getLogger(TokenContractGasProvider.class);
    private Web3j _web3;
    BigInteger gasPrice = null;

    public TokenContractGasProvider(Web3j web3j) {
        _web3 = web3j;
    }

    @Override
    public BigInteger getGasPrice(String contractFunc) {
        return getGasPrice();
    }

    @Override
    public BigInteger getGasPrice() {
        if(gasPrice == null) {
            gasPrice = BigInteger.valueOf(10_000_000_000L);
            try {
                gasPrice = _web3.ethGasPrice().send().getGasPrice();
            } catch (Exception ex) {
                _log.error("web3 ethGasPrice failed with exception", ex);
            }
        } else {
            gasPrice = gasPrice.add(gasPrice.divide(BigInteger.TEN));
        }
        return gasPrice;
    }

    @Override
    public BigInteger getGasLimit(String contractFunc) {
        return getGasLimit();
    }

    @Override
    public BigInteger getGasLimit() {
        return  BigInteger.valueOf(100_000);
    }
}
