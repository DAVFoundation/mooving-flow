package org.dav.vehicle_rider.BlueSnapRequests;

import org.apache.beam.sdk.transforms.DoFn;
import org.dav.vehicle_rider.payment.BlueSnapAPI;
import org.dav.vehicle_rider.payment.BlueSnapInfoResponse;
import org.dav.vehicle_rider.rider_payment.InvoiceGenerator;
import org.slf4j.Logger;

public class GetPaymentInfo extends DoFn<InvoiceGenerator, InvoiceGenerator> {

    private static final long serialVersionUID = 1L;
    private final Logger _log;
    private final BlueSnapAPI blueSnapAPI;

    public GetPaymentInfo(Logger log, String bluesnapApiUser, String bluesnapApiPass, String bluesnapUrl) {
        this._log = log;
        this.blueSnapAPI = new BlueSnapAPI(bluesnapApiUser, bluesnapApiPass, bluesnapUrl, log, "/services/2/vaulted-shoppers/");
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        try {
            InvoiceGenerator invoiceData = c.element();
            String paymentMethodId = invoiceData.getPaymentMethodId();
            BlueSnapInfoResponse paymentInfo = blueSnapAPI.getPaymentInfo(paymentMethodId);
            invoiceData.setPaymentInfo(paymentInfo.brand, paymentInfo.last4);
            c.output(invoiceData);
        } catch (Exception ex) {
            this._log.info(String.format("error while get payment info for invoice: ", ex));
        }
    }
}