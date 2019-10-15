package org.dav.vehicle_rider.rider_payment;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.dav.vehicle_rider.CassandraDoFnBase;
import org.dav.vehicle_rider.Helpers;
import org.dav.vehicle_rider.cassandra.Payment;
import org.dav.vehicle_rider.cassandra.RideSummary;
import org.dav.vehicle_rider.cassandra.Rider;
import org.dav.vehicle_rider.messages.DavBalanceDeltaUpdateMessage;
import org.dav.vehicle_rider.messages.OwnerDetailsForPayment;
import org.dav.vehicle_rider.payment.BlueSnapAPI;
import org.dav.vehicle_rider.token_payment.TokenPaymentMessage;
import org.dav.vehicle_rider.token_payment.TransactionType;
import org.slf4j.Logger;

public class PaymentTransactionCreator
        extends CassandraDoFnBase<KV<OwnerDetailsForPayment, Iterable<RideSummary>>, Payment> {

    private static final long serialVersionUID = 1L;

    private static final String QUERY = "select * from vehicle_rider.riders where id=?";

    private TupleTag<Payment> _paymentTag;
    private TupleTag<RideSummary> _rideSummaryTag;
    private TupleTag<DavBalanceDeltaUpdateMessage> _davBalanceTag;
    private TupleTag<TokenPaymentMessage> _tokenPaymentTag;

    private BlueSnapAPI _blueSnapAPI;

    protected PaymentTransactionCreator(String apiUser, String apiPass, String apiUrl, String cassandraHost, Logger log,
            TupleTag<Payment> paymentTag, TupleTag<RideSummary> rideSummaryTag,
            TupleTag<DavBalanceDeltaUpdateMessage> davBalanceTag, TupleTag<TokenPaymentMessage> tokenPaymentTag) {
        super(cassandraHost, QUERY, log);
        _blueSnapAPI = new BlueSnapAPI(apiUser, apiPass, apiUrl, log);
        _paymentTag = paymentTag;
        _rideSummaryTag = rideSummaryTag;
        _davBalanceTag = davBalanceTag;
        _tokenPaymentTag = tokenPaymentTag;
    }

    @ProcessElement
    public void ProcessElement(ProcessContext context) {
        try {
            KV<OwnerDetailsForPayment, Iterable<RideSummary>> idsToRides = context.element();
            OwnerDetailsForPayment ownerDetailsForPayment = context.element().getKey();
            Iterable<RideSummary> rides = idsToRides.getValue();

            String vendor = ownerDetailsForPayment.getVendor();
            BigDecimal ownerCommissionFactor = ownerDetailsForPayment.getCommissionFactor();
            BigDecimal ownerDavBalanceBase = ownerDetailsForPayment.getDavBalanceBase();
            BigDecimal ownerDavBalanceDelta = ownerDetailsForPayment.getDavBalanceDelta();
            BigDecimal amountToDecreaseFromDavBalanceDelta = BigDecimal.ZERO;
            HashMap<UUID, String> riderToVaultedShopperId = new HashMap<UUID, String>();

            MappingManager mappingManager = new MappingManager(_session);
            Mapper<Rider> riderMapper = mappingManager.mapper(Rider.class);
            for (RideSummary rideSummary : rides) {
                if (rideSummary.authTransactionId != null) {
                    _blueSnapAPI.createAuthReversalTxn(rideSummary.authTransactionId);
                }
                if (!rideSummary.paymentMethodDav) {
                    String vaultedShopperId = riderToVaultedShopperId.get(rideSummary.riderId);
                    if (vaultedShopperId == null) {
                        BoundStatement riderBound = _prepared.bind(rideSummary.riderId);
                        ResultSet riderResultSet = this.executeBoundStatement(riderBound);
                        Result<Rider> riderResults = riderMapper.map(riderResultSet);
                        Rider rider = riderResults.one();
                        vaultedShopperId = rider.paymentMethodId;
                        riderToVaultedShopperId.put(rider.id, vaultedShopperId);
                    }

                    BigDecimal ridePriceInDav = Helpers.calculateDavPrice(rideSummary.price,
                            rideSummary.conversionRate);
                    BigDecimal commissionFactorForDavReward;
                    if (ridePriceInDav.compareTo(ownerDavBalanceBase.add(ownerDavBalanceDelta)) <= 0) {
                        ownerDavBalanceDelta = ownerDavBalanceDelta.subtract(ridePriceInDav);
                        commissionFactorForDavReward = BigDecimal.ZERO;
                        amountToDecreaseFromDavBalanceDelta = amountToDecreaseFromDavBalanceDelta
                                .add(rideSummary.davAwarded);
                    } else {
                        commissionFactorForDavReward = Helpers
                                .calculateFiatPrice(rideSummary.davAwarded, rideSummary.conversionRate)
                                .divide(rideSummary.price, 10, RoundingMode.DOWN);
                        context.output(_tokenPaymentTag, new TokenPaymentMessage(TransactionType.NetworkOperatorToRider,
                                rideSummary.davAwarded));
                    }
                    BigDecimal combinedCommission = ownerCommissionFactor.add(commissionFactorForDavReward)
                            .multiply(BigDecimal.valueOf(100));
                    String transactionId = _blueSnapAPI.createAuthAndCaptureTxn(vaultedShopperId,
                            rideSummary.currencyCode, rideSummary.price, vendor, combinedCommission);
                    if (transactionId != null) {
                        rideSummary.captureTransactionId = transactionId;
                        Payment payment = new Payment(rideSummary.ownerId, rideSummary.riderId, new Date(),
                                transactionId, vaultedShopperId, vendor, ownerCommissionFactor,
                                commissionFactorForDavReward, rideSummary.currencyCode, rideSummary.price);
                        context.output(_paymentTag, payment);
                    }
                } else {
                    rideSummary.captureTransactionId = "not fiat transaction";

                }
                context.output(_rideSummaryTag, rideSummary);
            }
            if (amountToDecreaseFromDavBalanceDelta.compareTo(BigDecimal.ZERO) == 1) {
                context.output(_davBalanceTag, new DavBalanceDeltaUpdateMessage(ownerDetailsForPayment.getOwnerId(),
                        amountToDecreaseFromDavBalanceDelta.multiply(BigDecimal.valueOf(-1))));
                context.output(_tokenPaymentTag, new TokenPaymentMessage(TransactionType.OwnerToRider,
                        amountToDecreaseFromDavBalanceDelta, ownerDetailsForPayment.getOwnerId()));
            }
        } catch (Exception ex) {
            this._log.info("error while calculating payments", ex);
        }
    }
}