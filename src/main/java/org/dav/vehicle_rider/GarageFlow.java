package org.dav.vehicle_rider;

import avro.shaded.com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.dav.Json;
import org.dav.config.Config;
import org.dav.vehicle_rider.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.UUID;

public class GarageFlow {
    private static final Logger LOG = LoggerFactory.getLogger(UngarageFlow.class);

    public static void main(String[] args) throws Exception {
        Config config = Config.create(true, args);

        ParameterTool parameters = ParameterTool.fromArgs(args);

        TupleTag<GarageFlowMessage> successTag = new TupleTag<GarageFlowMessage>() {

            private static final long serialVersionUID = 1L;
        };
        TupleTag<GarageFlowMessage> failTag = new TupleTag<GarageFlowMessage>() {

            private static final long serialVersionUID = 1L;
        };

        Pipeline p = Pipeline.create(config.pipelineOptions());
        PCollection<GarageFlowMessage> afterUnlock = p
                .apply("Read from kafka", KafkaIO.<Long, String>read().withKeyDeserializer(LongDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withBootstrapServers(String.format("%s:%s", config.kafkaSeed(), config.kafkaPort()))
                        .withTopic("garage-vehicle").updateConsumerProperties(ImmutableMap.of("group.id", "GarageFlow"))
                        .withReadCommitted().commitOffsetsInFinalize().withoutMetadata())
                .apply(Values.create())

                .apply(MapElements.into(TypeDescriptor.of(GarageFlowMessage.class)).via(message -> {
                    try {
                        return Json.parse(message, GarageFlowMessage.class, false);
                    } catch (Exception e) {
                        LOG.error("Parse JSON", e);
                        return null;
                    }
                })).apply("Filter NULLs", Filter.by(obj -> obj != null))

                .apply("Set User Job To Started",
                        ParDo.of(new UpdateUserJobState<>(UserJobState.started, config.cassandraSeed(), LOG)))

                .apply(new LockVehicleStatusUpdate<GarageFlowMessage>(VehicleStatus.notavailable.toString(),
                        VehicleStatus.notavailable.toString(), successTag, failTag, config, LOG))

                .apply(ParDo.of(new SetVehicleState<>(SetVehicleState.State.Unlocked, config)));

        afterUnlock.apply(Filter.by(unlockVehicle -> !(unlockVehicle.stateChanged)))
                .apply(new UnlockVehicleStatusUpdate<GarageFlowMessage>(VehicleStatus.notavailable.toString(), config,
                        LOG, true));

        afterUnlock.apply(Filter.by(unlockVehicle -> unlockVehicle.stateChanged)).apply(
                new UnlockVehicleStatusUpdate<GarageFlowMessage>(VehicleStatus.maintenance.toString(), config, LOG));
        p.run();
    }

    static class GarageFlowMessage implements JobMessage, VehicleMessageWithId, VehicleMessageWithDeviceId,
            VehicleMessageWithStateChanged, VehicleMessageWithVendor, Serializable {
        private static final long serialVersionUID = 1L;
        public UUID jobId;
        public UUID vehicleId;
        public String deviceId;
        public boolean stateChanged;
        public String vendor;

        public String getVendor() {
            return vendor;
        }

        public void setVendor(String vendor) {
            this.vendor = vendor;
        }

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
            return this.deviceId;
        }

        @Override
        public void setDeviceId(String deviceId) {
            this.deviceId = deviceId;
        }

        @Override
        public void setStateChanged(boolean stateChanged) {
            this.stateChanged = stateChanged;
        }
    }
}
