package org.dav.vehicle_rider;

import java.io.Serializable;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.dav.Json;
import org.dav.config.Config;
import org.dav.vehicle_rider.LockVehicleStatusUpdate;
import org.dav.vehicle_rider.UnlockVehicleStatusUpdate;
import org.dav.vehicle_rider.messages.VehicleMessageWithId;
import org.dav.vehicle_rider.VehicleStatus;
import org.dav.vehicle_rider.messages.JobMessage;
import org.dav.vehicle_rider.messages.VehicleMessageWithDeviceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import avro.shaded.com.google.common.collect.ImmutableMap;

public class UncordonFlow {
    private static final Logger LOG = LoggerFactory.getLogger(UncordonFlow.class);

    public static void main(String[] args) {
        Config config = Config.create(true, args);

        TupleTag<UncordonFlowMessage> successTag = new TupleTag<UncordonFlowMessage>() {

            private static final long serialVersionUID = 1L;
        };
        TupleTag<UncordonFlowMessage> failTag = new TupleTag<UncordonFlowMessage>() {

            private static final long serialVersionUID = 1L;
        };

        Pipeline p = Pipeline.create(config.pipelineOptions());
        p.apply("Read from kafka", KafkaIO.<Long, String>read().withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withBootstrapServers(String.format("%s:%s", config.kafkaSeed(), config.kafkaPort()))
                .withTopic("uncordon-vehicle").updateConsumerProperties(ImmutableMap.of("group.id", "UncordonFlow"))
                .withReadCommitted().commitOffsetsInFinalize().withoutMetadata()).apply(Values.create())

                .apply(MapElements.into(TypeDescriptor.of(UncordonFlowMessage.class)).via(message -> {
                    try {
                        return Json.parse(message, UncordonFlowMessage.class, false);
                    } catch (Exception e) {
                        LOG.error("Parse JSON", e);
                        return null;
                    }
                })).apply("Filter NULLs", Filter.by(obj -> obj != null))

                .apply("Set User Job To Started",
                        ParDo.of(new UpdateUserJobState<>(UserJobState.started, config.cassandraSeed(), LOG)))

                .apply(new LockVehicleStatusUpdate<UncordonFlowMessage>(VehicleStatus.notavailable.toString(),
                        VehicleStatus.notavailable.toString(), successTag, failTag, config, LOG))

                .apply(new UnlockVehicleStatusUpdate<UncordonFlowMessage>(VehicleStatus.available.toString(), config,
                        LOG));
        p.run();
    }

    static class UncordonFlowMessage
            implements JobMessage, VehicleMessageWithId, VehicleMessageWithDeviceId, Serializable {
        private static final long serialVersionUID = 1L;
        public UUID jobId;
        public UUID vehicleId;

        @Override
        public UUID getJobId() {
            return this.jobId;
        }

        @Override
        public UUID getVehicleId() {
            return this.vehicleId;
        }

        @Override
        public void setVehicleId(UUID vehicleId) {
            this.vehicleId = vehicleId;
        }

        @Override
        public String getDeviceId() {
            return null;
        }

        @Override
        public void setDeviceId(String deviceId) {
        }
    }
}
