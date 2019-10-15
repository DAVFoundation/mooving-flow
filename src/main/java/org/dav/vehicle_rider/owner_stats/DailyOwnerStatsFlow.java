package org.dav.vehicle_rider.owner_stats;

import java.util.Arrays;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.dav.config.Config;
import org.dav.vehicle_rider.cassandra.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DailyOwnerStatsFlow {

    private static final Logger LOG = LoggerFactory.getLogger(DailyOwnerStatsFlow.class);
    private static final String CASSANDRA_SCEHME_NAME = "vehicle_rider";

    private static void runOwnerStatsWithSpecificDate(LocalDate date, Config config, Pipeline pipeline) {

        PCollection<KV<String, Iterable<RideSummaryEffectiveDate>>> vehicleIdToRides = pipeline
                .apply("read ride summaries from cassandra",
                        CassandraIO.<RideSummaryEffectiveDate>read().withHosts(Arrays.asList(config.cassandraSeed()))
                                .withWhere(String.format("effective_date='%s'", date)).withPort(config.cassandraPort())
                                .withKeyspace(CASSANDRA_SCEHME_NAME).withTable("rides_summary_effective_date")
                                .withEntity(RideSummaryEffectiveDate.class)
                                .withCoder(SerializableCoder.of(RideSummaryEffectiveDate.class)))
                .apply("transform to KV of vehicle id and ride summary",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                                        TypeDescriptor.of(RideSummaryEffectiveDate.class)))
                                .via(ride -> KV.of(ride.vehicleId.toString(), ride)))
                .apply("group rides for the same vehicle", GroupByKey.<String, RideSummaryEffectiveDate>create());

        PCollection<KV<String, String>> vehicleIds = pipeline.apply("cassandra read all Vehicles",
                CassandraIO.<Vehicle>read().withHosts(Arrays.asList(config.cassandraSeed()))
                        .withPort(config.cassandraPort()).withKeyspace(CASSANDRA_SCEHME_NAME).withTable("vehicles")
                        .withEntity(Vehicle.class).withCoder(SerializableCoder.of(Vehicle.class)))
                .apply("transform vehicles vehicleIds",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                                .via(vehicle -> KV.of(vehicle.id.toString(), vehicle.id.toString())));


        final TupleTag<Iterable<RideSummaryEffectiveDate>> vehicleDailyStatsTag = new TupleTag<>();
        final TupleTag<String> vehicleIdsTag = new TupleTag<>();

        PCollection<VehicleStatsDaily> vehicleDailyStats = KeyedPCollectionTuple.of(vehicleIdsTag, vehicleIds)
                .and(vehicleDailyStatsTag, vehicleIdToRides).apply(CoGroupByKey.<String>create())
                .apply("summarize rides per vehicle",
                        ParDo.of(new RidesToVehicleStats(config.cassandraSeed(), date, vehicleDailyStatsTag, LOG)))
                .apply("Filter NULLs", Filter.by(obj -> obj != null));


        vehicleDailyStats.apply("write vehicle daily stats to cassandra",
                CassandraIO.<VehicleStatsDaily>write().withHosts(Arrays.asList(config.cassandraSeed()))
                        .withPort(config.cassandraPort()).withKeyspace(CASSANDRA_SCEHME_NAME)
                        .withEntity(VehicleStatsDaily.class));


        vehicleDailyStats.apply("transform to KV of owner id and ride vehicle daily stats",
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(VehicleStatsDaily.class)))
                        .via(vehicleStats -> {
                            return KV.of(vehicleStats.ownerId.toString(), vehicleStats);
                        }))
                .apply("group vehicle stats for the same owner", GroupByKey.<String, VehicleStatsDaily>create())
                .apply(ParDo.of(new VehiclesStatsToOwnerStats(LOG))).apply("write owner daily stats to cassandra",
                        CassandraIO.<OwnerStatsDaily>write().withHosts(Arrays.asList(config.cassandraSeed()))
                                .withPort(config.cassandraPort()).withKeyspace(CASSANDRA_SCEHME_NAME)
                                .withEntity(OwnerStatsDaily.class));
    }

    public static void main(String[] args) {

        DateTime dt = new DateTime(DateTimeZone.UTC);
        LocalDate currentDate = LocalDate.now(DateTimeZone.UTC);

        int hour = dt.getHourOfDay();
        LocalDate additionalDate;
        if (hour >= 12) {
            additionalDate = currentDate.plusDays(1);
        } else {
            additionalDate = currentDate.minusDays(1);
        }
        LOG.info(String.format("starting daily stats for dates: %s, %s", currentDate, additionalDate));

        Config config = Config.create(false, args);
        Pipeline pipeline = Pipeline.create(config.pipelineOptions());

        runOwnerStatsWithSpecificDate(currentDate, config, pipeline);
        runOwnerStatsWithSpecificDate(additionalDate, config, pipeline);

        pipeline.run();
    }
}