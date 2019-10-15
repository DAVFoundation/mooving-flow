package org.dav.vehicle_rider.rider_payment;

import java.util.Arrays;
import java.util.Date;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.common.serialization.StringSerializer;
import org.dav.config.Config;
import org.dav.Json;
import org.dav.vehicle_rider.cassandra.Payment;
import org.dav.vehicle_rider.cassandra.RideSummary;
import org.dav.vehicle_rider.cassandra_helpers.GetOwnerBalanceDelta;
import org.dav.vehicle_rider.cassandra_helpers.GetOwnerDetailsForPayment;
import org.dav.vehicle_rider.cassandra_helpers.UpdateOwnerDavBalanceDelta;
import org.dav.vehicle_rider.messages.DavBalanceDeltaUpdateMessage;
import org.dav.vehicle_rider.token_payment.TokenPaymentMessage;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class RiderPaymentFlow {

    private static final Logger log = LoggerFactory.getLogger(RiderPaymentFlow.class);
    private static final String CASSANDRA_SCEHME_NAME = "vehicle_rider";

    public static void runDailyPaymentFlow(long ridesFromTime, long ridesUntilTime, String[] args) {
        Config config = Config.create(false, args);
        Pipeline pipeline = Pipeline.create(config.pipelineOptions());

        ParameterTool parameters = ParameterTool.fromArgs(args);
        String bluesnapApiUser = parameters.get("bluesnap_api_user");
        String bluesnapApiPass = parameters.get("bluesnap_api_pass");
        String bluesnapUrl = parameters.get("bluesnap_url");

        TupleTag<Payment> paymentTag = new TupleTag<Payment>() {

            private static final long serialVersionUID = 1L;
        };
        TupleTag<RideSummary> rideSummaryTag = new TupleTag<RideSummary>() {

            private static final long serialVersionUID = 1L;
        };
        TupleTag<DavBalanceDeltaUpdateMessage> davBalanceTag = new TupleTag<DavBalanceDeltaUpdateMessage>() {

            private static final long serialVersionUID = 1L;
        };
        TupleTag<TokenPaymentMessage> tokenPaymentTag = new TupleTag<TokenPaymentMessage>() {

            private static final long serialVersionUID = 1L;
        };

        PCollectionTuple output = pipeline
                .apply("Read all ride summaries",
                        CassandraIO.<RideSummary>read().withHosts(Arrays.asList(config.cassandraSeed()))
                                .withPort(config.cassandraPort()).withKeyspace(CASSANDRA_SCEHME_NAME)
                                .withTable("rides_summary").withEntity(RideSummary.class)
                                .withCoder(SerializableCoder.of(RideSummary.class)))

                .apply("Filter ride summaries",
                        Filter.by(rideSummary -> rideSummary.endTime != null
                                && rideSummary.endTime.getTime() >= ridesFromTime
                                && rideSummary.endTime.getTime() <= ridesUntilTime
                                && rideSummary.captureTransactionId == null && rideSummary.paymentMethodDav != null))
                .apply("map ride summaries to owner id key",
                        MapElements.into(
                                TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(RideSummary.class)))
                                .via(ride -> {
                                    return KV.of(ride.ownerId.toString(), ride);
                                }))
                .apply("group ride summaries owner id", GroupByKey.<String, RideSummary>create())
                .apply("get owner details", ParDo.of(new GetOwnerDetailsForPayment(config.cassandraSeed(), log)))
                .apply("get owner dav balance delta", ParDo.of(new GetOwnerBalanceDelta(config.cassandraSeed(), log)))
                .apply("Calculating transaction and create fiat transaction",
                        ParDo.of(new PaymentTransactionCreator(bluesnapApiUser, bluesnapApiPass, bluesnapUrl,
                                config.cassandraSeed(), log, paymentTag, rideSummaryTag, davBalanceTag,
                                tokenPaymentTag)).withOutputTags(paymentTag,
                                        TupleTagList.of(rideSummaryTag).and(davBalanceTag).and(tokenPaymentTag)));

        output.get(paymentTag).apply("Write payments to cassandra",
                CassandraIO.<Payment>write().withHosts(Arrays.asList(config.cassandraSeed()))
                        .withPort(config.cassandraPort()).withKeyspace(CASSANDRA_SCEHME_NAME)
                        .withEntity(Payment.class));

        output.get(rideSummaryTag).apply("Update ride to payed",
                ParDo.of(new UpdateRidePayed(config.cassandraSeed(), log)));

        output.get(davBalanceTag).apply("Update owner dav balance delta",
                ParDo.of(new UpdateOwnerDavBalanceDelta<DavBalanceDeltaUpdateMessage>(config.cassandraSeed(), log)));

        output.get(tokenPaymentTag)
                .apply("Json Serialize",
                        MapElements.into(TypeDescriptors.strings())
                                .via(tokenPaymentMessage -> Json.format(tokenPaymentMessage, false)))
                .apply("Send token payment message",
                        KafkaIO.<Void, String>write()
                                .withBootstrapServers(config.kafkaSeed() + ":" + config.kafkaPort())
                                .withTopic("token-payment").withValueSerializer(StringSerializer.class).values());

        pipeline.run();
    }

    public static void main(String[] args) {

        ParameterTool parameters = ParameterTool.fromArgs(args);
        String paymentIntervalProperty = parameters.get("payment_interval_millis");

        if (paymentIntervalProperty != null) {
            long paymenInterval = Long.parseLong(paymentIntervalProperty);
            Date now = new Date();
            long fromTime = now.getTime() - ((long) (paymenInterval * 1.5));
            long untilTime = now.getTime();

            runDailyPaymentFlow(fromTime, untilTime, args);
        } else {
            log.error("property payment_interval_millis is unset");
        }
    }
}
