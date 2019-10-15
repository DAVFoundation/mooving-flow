package org.dav.vehicle_rider;

import org.slf4j.Logger;

public class UpdateRideSummaryPaymentMethodDav extends UpdateRideSummary {

    private final static String QUERY = "UPDATE vehicle_rider.rides_summary SET payment_method_dav=?, dav_awarded=0 WHERE rider_id=? and vehicle_id=? and start_time=?";
    
    public UpdateRideSummaryPaymentMethodDav(String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
    }

	private static final long serialVersionUID = 1L;

}