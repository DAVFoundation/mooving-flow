package org.dav.vehicle_rider;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class Helpers {
    public static BigDecimal calculateDavPrice(BigDecimal fiatPrice, BigDecimal conversionRate) {
        return fiatPrice.divide(conversionRate, 2, RoundingMode.DOWN);
    }

    public static BigDecimal calculateFiatPrice(BigDecimal davPrice, BigDecimal conversionRate) {
        return davPrice.multiply(conversionRate);
    }

    public static BigDecimal convertDavBalanceDeltaFromCassandraValue(long davBalanceDelta) {
        return new BigDecimal(davBalanceDelta).divide(BigDecimal.valueOf(100));
    }

    public static long convertDavBalanceDeltaToCassandraValue(BigDecimal davBalanceDelta) {
        return davBalanceDelta.multiply(BigDecimal.valueOf(100)).longValue();
    }
}