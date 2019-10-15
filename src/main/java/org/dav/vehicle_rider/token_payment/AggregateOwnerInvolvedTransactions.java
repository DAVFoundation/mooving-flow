package org.dav.vehicle_rider.token_payment;

import java.math.BigDecimal;
import java.util.UUID;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class AggregateOwnerInvolvedTransactions
        extends DoFn<KV<String, Iterable<TokenPaymentMessage>>, TokenPaymentMessage> {

    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(ProcessContext context) {
        KV<String, Iterable<TokenPaymentMessage>> ownerIdToTokenPayments = context.element();
        BigDecimal ownerPaymentToRider = BigDecimal.ZERO;
        for (TokenPaymentMessage tokenPayment : ownerIdToTokenPayments.getValue()) {
            TransactionType transactionType = tokenPayment.getTransactionType();
            if (transactionType == TransactionType.OwnerToRider) {
                ownerPaymentToRider = ownerPaymentToRider.add(tokenPayment.getAmount());
            } else {
                ownerPaymentToRider = ownerPaymentToRider.subtract(tokenPayment.getAmount());
            }
        }
        TokenPaymentMessage aggregatedTransaction;
        if (ownerPaymentToRider != BigDecimal.ZERO) {
            if (ownerPaymentToRider.compareTo(BigDecimal.ZERO) == 1) {
                aggregatedTransaction = new TokenPaymentMessage(TransactionType.OwnerToRider, ownerPaymentToRider,
                        UUID.fromString(ownerIdToTokenPayments.getKey()));
            } else {
                aggregatedTransaction = new TokenPaymentMessage(TransactionType.RiderToOwner,
                        ownerPaymentToRider.multiply(BigDecimal.valueOf(-1)),
                        UUID.fromString(ownerIdToTokenPayments.getKey()));
            }
            context.output(aggregatedTransaction);
        }
    }
}