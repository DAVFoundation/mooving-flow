package org.dav.vehicle_rider.owner_stats;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.DoFn;

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.dav.vehicle_rider.cassandra.OwnerStatsDaily;
import org.dav.vehicle_rider.cassandra.VehicleStatsDaily;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class VehiclesStatsToOwnerStats extends DoFn<KV<String, Iterable<VehicleStatsDaily>>, OwnerStatsDaily> {

    private static final long serialVersionUID = 1L;
    private final Logger _log;

    public VehiclesStatsToOwnerStats(Logger log) {
        this._log = log;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        try {
            Iterable<VehicleStatsDaily> vehicleDailyStatsCollection = c.element().getValue();
            BigDecimal totalFiatRevenue = BigDecimal.ZERO;
            BigDecimal totalDavRevenue = BigDecimal.ZERO;
            int totalRidesCount = 0;
            BigDecimal totalFiatRevenueAccumulate = BigDecimal.ZERO;
            BigDecimal totalDavRevenueAccumulate = BigDecimal.ZERO;
            int totalRidesCountAccumulate = 0;
            for (VehicleStatsDaily vehicleStats : vehicleDailyStatsCollection) {
                totalRidesCount += vehicleStats.totalRidesCount;
                totalFiatRevenue = totalFiatRevenue.add(vehicleStats.totalFiatRevenue);
                totalDavRevenue = totalDavRevenue.add(vehicleStats.totalDavRevenue);
                totalRidesCountAccumulate += vehicleStats.totalRidesCountAccumulate;
                totalFiatRevenueAccumulate = vehicleStats.totalFiatRevenueAccumulate == null
                        ? totalFiatRevenueAccumulate
                        : totalFiatRevenueAccumulate.add(vehicleStats.totalFiatRevenueAccumulate);
                totalDavRevenueAccumulate = vehicleStats.totalDavRevenueAccumulate == null ? totalDavRevenueAccumulate
                        : totalDavRevenueAccumulate.add(vehicleStats.totalDavRevenueAccumulate);
            }
            VehicleStatsDaily firstVehicleStats = vehicleDailyStatsCollection.iterator().next();
            DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
            _log.info(String.format(
                    "taking date from vehicle stats, owner id: %s, vehicle id: %s, date: %s, fiat currency: %s, total ride count: %s, total fiat revenue: %s, total dav revenue: %s, accumulate ride count: %s, accumulate fiat revenue: %s, accumulate dav revenue: %s",
                    firstVehicleStats.ownerId, firstVehicleStats.vehicleId,
                    firstVehicleStats.Date.toString(dateFormatter), firstVehicleStats.currencyCode,
                    firstVehicleStats.totalRidesCount, firstVehicleStats.totalFiatRevenue,
                    firstVehicleStats.totalDavRevenue, firstVehicleStats.totalRidesCountAccumulate,
                    firstVehicleStats.totalFiatRevenueAccumulate, firstVehicleStats.totalDavRevenueAccumulate));
            LocalDate forDate = firstVehicleStats.Date;
            String fiatCurrency = firstVehicleStats.currencyCode;
            OwnerStatsDaily ownerStatsDaily = new OwnerStatsDaily(firstVehicleStats.ownerId, forDate, fiatCurrency,
                    totalRidesCount, totalDavRevenue, totalFiatRevenue, totalRidesCountAccumulate,
                    totalDavRevenueAccumulate, totalFiatRevenueAccumulate);
            _log.info(String.format(
                    "owner stats, id: %s, date: %s, fiat currency: %s, total ride count: %s, total fiat revenue: %s, total dav revenue: %s, accumulate ride count: %s, accumulate fiat revenue: %s, accumulate dav revenue: %s",
                    ownerStatsDaily.ownerId, ownerStatsDaily.Date.toString(dateFormatter), ownerStatsDaily.currencyCode,
                    ownerStatsDaily.totalRidesCount, ownerStatsDaily.totalFiatRevenue, ownerStatsDaily.totalDavRevenue,
                    ownerStatsDaily.totalRidesCountAccumulate, ownerStatsDaily.totalFiatRevenueAccumulate,
                    ownerStatsDaily.totalDavRevenueAccumulate));
            c.output(ownerStatsDaily);
        } catch (Exception ex) {
            this._log.info("error while calculating owner stats: ", ex);
        }
    }
}