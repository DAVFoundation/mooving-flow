package org.dav.vehicle_rider.payment;

import java.io.IOException;
import java.net.MalformedURLException;
import org.apache.beam.sdk.transforms.DoFn;
import org.dav.vehicle_rider.messages.VehicleMessageWithAuthTxn;
import org.slf4j.Logger;

public class CreateAuthTxn<T extends VehicleMessageWithAuthTxn> extends DoFn<T,T> {

    private static final long serialVersionUID = 1L;
    private final Logger _log;
    private BlueSnapAPI _blueSnapAPI;

    public CreateAuthTxn(String apiUser, String apiPass, String apiUrl, Logger log) {
        _blueSnapAPI = new BlueSnapAPI(apiUser, apiPass, apiUrl, log);
        _log = log;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws MalformedURLException, IOException {
        T unlockMessage = context.element();
        if (unlockMessage.getPaymentMethodId() != null) {
            String txnId = _blueSnapAPI.createAuthTxn(unlockMessage.getPaymentMethodId(), unlockMessage.getAuthAmount(),
                    unlockMessage.getAuthCurrency());
            if (txnId != null) {
                unlockMessage.setAuthTxn(txnId);
            } else {
                // Write auth error to database instead of return
            }
        }
        context.output(unlockMessage);
    }
}
