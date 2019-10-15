package org.dav.vehicle_rider.owner_stats;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.core.LocalDate;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.TupleTag;
import org.dav.vehicle_rider.CassandraDoFnBase;
import java.util.UUID;

import org.apache.beam.sdk.values.KV;
import org.dav.vehicle_rider.cassandra.RideSummaryEffectiveDate;
import org.dav.vehicle_rider.cassandra.VehicleStatsDaily;
import org.slf4j.Logger;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.util.Date;

public class RidesToVehicleStats extends CassandraDoFnBase<KV<String, CoGbkResult>, VehicleStatsDaily> {

    private static final long serialVersionUID = 1L;
    private final Logger _log;
    private final org.joda.time.LocalDate _date;
    private static final String QUERY = "select * from vehicle_rider.vehicle_stats_daily_date WHERE date=? AND vehicle_id=?";
    private TupleTag<Iterable<RideSummaryEffectiveDate>> _rideSummariesTag;

    public RidesToVehicleStats(String cassandraHost, org.joda.time.LocalDate date,
            TupleTag<Iterable<RideSummaryEffectiveDate>> rideSummariesTag, Logger log) {
        super(cassandraHost, QUERY, log);
        this._date = date;
        this._log = log;
        this._rideSummariesTag = rideSummariesTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        try {
            UUID vehicleId = UUID.fromString(c.element().getKey());
            org.joda.time.LocalDate yesterday = this._date.minusDays(1);
            LocalDate datastaxDateYesterday = LocalDate.fromYearMonthDay(yesterday.getYear(),
                    yesterday.getMonthOfYear(), yesterday.getDayOfMonth());
            MappingManager mappingManager = new MappingManager(_session);
            Mapper<VehicleStatsDaily> mapper = mappingManager.mapper(VehicleStatsDaily.class);
            BoundStatement bound = _prepared.bind(datastaxDateYesterday, vehicleId);
            ResultSet resultSet = this.executeBoundStatement(bound);
            Result<VehicleStatsDaily> results = mapper.map(resultSet);
            VehicleStatsDaily yesterdayStatistics = results.one();
            Iterable<RideSummaryEffectiveDate> rideSummaries = c.element().getValue().getOnly(this._rideSummariesTag,
                    null);
            VehicleStatsDaily vehicleStatsDaily = this.getStatsOfToday(rideSummaries, yesterdayStatistics);
            if (vehicleStatsDaily != null) {
                _log.info(String.format(
                        "vehicle stats, id: %s, owner id: %s, fiat currency: %s, total ride count: %s, total fiat revenue: %s, total dav revenue: %s, accumulate ride count: %s, accumulate fiat revenue: %s, accumulate dav revenue: %s",
                        vehicleStatsDaily.vehicleId, vehicleStatsDaily.ownerId, vehicleStatsDaily.currencyCode,
                        vehicleStatsDaily.totalRidesCount, vehicleStatsDaily.totalFiatRevenue,
                        vehicleStatsDaily.totalDavRevenue, vehicleStatsDaily.totalRidesCountAccumulate,
                        vehicleStatsDaily.totalFiatRevenueAccumulate, vehicleStatsDaily.totalDavRevenueAccumulate));
            }
            c.output(vehicleStatsDaily);
        } catch (Exception ex) {
            this._log.info("error while calculating vehicle stats: ", ex);
        }
    }

    private VehicleStatsDaily getStatsOfToday(Iterable<RideSummaryEffectiveDate> todayRides,
            VehicleStatsDaily yesterdayStats) {
        org.joda.time.LocalDate currentDate = org.joda.time.LocalDate.now(DateTimeZone.UTC);
        if (todayRides == null) {
            if (yesterdayStats == null) {
                return null;
            }
            VehicleStatsDaily vehicleStatsDaily = new VehicleStatsDaily(yesterdayStats.ownerId, currentDate,
                    yesterdayStats.vehicleId, yesterdayStats.currencyCode, 0, new BigDecimal(0), new BigDecimal(0),
                    yesterdayStats.lastParkingImageUrl, new int[5], yesterdayStats.totalRidesCountAccumulate,
                    yesterdayStats.totalDavRevenueAccumulate, yesterdayStats.totalFiatRevenueAccumulate);
            return vehicleStatsDaily;
        }
        Iterable<RideSummaryEffectiveDate> rideSummaries = todayRides;
        BigDecimal totalFiatRevenue = BigDecimal.ZERO;
        BigDecimal totalDavRevenue = BigDecimal.ZERO;
        int[] feedbackRatingCount = new int[5];
        int totalRidesCount = 0;
        String lastParkingImageUrl = "";
        Date lastDate = new Date(Long.MIN_VALUE);
        for (RideSummaryEffectiveDate rideSummary : rideSummaries) {
            totalRidesCount++;
            if (rideSummary.endTime != null && rideSummary.endTime.after(lastDate)) {
                lastDate = rideSummary.endTime;
                lastParkingImageUrl = rideSummary.parkingImageUrl;
            }
            if (rideSummary.rating > 0) {
                feedbackRatingCount[rideSummary.rating - 1]++;
            }
            BigDecimal amountWithoutCommission = rideSummary.price.subtract(rideSummary.commission);
            BigDecimal rideDavPartInOriginalCurrency = amountWithoutCommission.multiply(rideSummary.paymentDavFactor);
            BigDecimal rideFiatPartInOriginalCurrency = amountWithoutCommission.subtract(rideDavPartInOriginalCurrency);
            BigDecimal rideFiatPayment, rideDavPayment;
            if (rideSummary.currencyCode.equals("DAV")) {
                rideDavPayment = rideDavPartInOriginalCurrency;
                rideFiatPayment = rideFiatPartInOriginalCurrency.multiply(rideSummary.conversionRate);
            } else {
                if (rideSummary.paymentMethodDav) {
                    rideFiatPayment = rideFiatPartInOriginalCurrency;
                    rideDavPayment = rideDavPartInOriginalCurrency.divide(rideSummary.conversionRate);
                } else {
                    rideFiatPayment = rideFiatPartInOriginalCurrency;
                    rideDavPayment = rideDavPartInOriginalCurrency.divide(rideSummary.conversionRate);
                }
            }
            totalFiatRevenue = totalFiatRevenue.add(rideFiatPayment);
            totalDavRevenue = totalDavRevenue.add(rideDavPayment);
        }
        RideSummaryEffectiveDate firstRideSummary = rideSummaries.iterator().next();
        String fiatCurrency = firstRideSummary.currencyCode;
        org.joda.time.LocalDate forDate = firstRideSummary.EffectiveDate;
        int totalRidesCountAccumulate;
        BigDecimal totalFiatRevenueAccumulate, totalDavRevenueAccumulate;
        if (yesterdayStats != null) {
            totalRidesCountAccumulate = totalRidesCount + yesterdayStats.totalRidesCountAccumulate;
            totalFiatRevenueAccumulate = yesterdayStats.totalFiatRevenueAccumulate == null ? totalFiatRevenue
                    : totalFiatRevenue.add(yesterdayStats.totalFiatRevenueAccumulate);
            totalDavRevenueAccumulate = yesterdayStats.totalDavRevenueAccumulate == null ? totalDavRevenue
                    : totalDavRevenue.add(yesterdayStats.totalDavRevenueAccumulate);
        } else {
            totalRidesCountAccumulate = totalRidesCount;
            totalFiatRevenueAccumulate = totalFiatRevenue;
            totalDavRevenueAccumulate = totalDavRevenue;
        }
        VehicleStatsDaily vehicleStatsDaily = new VehicleStatsDaily(firstRideSummary.ownerId, forDate,
                firstRideSummary.vehicleId, fiatCurrency, totalRidesCount, totalDavRevenue, totalFiatRevenue,
                lastParkingImageUrl, feedbackRatingCount, totalRidesCountAccumulate, totalDavRevenueAccumulate,
                totalFiatRevenueAccumulate);
        return vehicleStatsDaily;
    }
}