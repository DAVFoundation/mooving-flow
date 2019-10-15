package org.dav.vehicle_rider;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.common.serialization.StringSerializer;
import org.dav.Json;
import org.dav.config.Config;
import org.dav.vehicle_rider.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.UUID;

public class LockVehicleFlow {

    private static final Logger LOG = LoggerFactory.getLogger(LockVehicleFlow.class);

    public static void runLockVehicleFlow(String[] args) {
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.create(true, args);

        ParameterTool parameters = ParameterTool.fromArgs(args);

        TupleTag<LockVehicleMessage> successTag = new TupleTag<LockVehicleMessage>() {

            private static final long serialVersionUID = 1L;
        };
        TupleTag<LockVehicleMessage> failTag = new TupleTag<LockVehicleMessage>() {

            private static final long serialVersionUID = 1L;
        };

        Pipeline p = Pipeline.create(config.pipelineOptions());

        PCollection<LockVehicleMessage> afterLock = p
                .apply("ReadLines", KafkaIO.readBytes()
                        .withBootstrapServers(config.kafkaSeed() + ":" + config.kafkaPort())
                        .withTopic("lock-vehicle")
                        .updateConsumerProperties(ImmutableMap.of("group.id", "lock-vehicle"))
                        .commitOffsetsInFinalize().withoutMetadata())

                .apply(MapElements.into(TypeDescriptors.strings()).via(t -> {
                    return new String(t.getValue());
                }))

                .apply(MapElements.into(TypeDescriptor.of(LockVehicleMessage.class))
                        .via(message -> {
                            try {
                                return Json.parse(message, LockVehicleMessage.class, false);
                            } catch (Exception e) {
                                LOG.info("Error deserializing lock message: " + e.toString());
                                throw e;
                            }
                        }))

                .apply(new LockVehicleStatusUpdate<LockVehicleMessage>(
                        VehicleStatus.onmission.toString(), VehicleStatus.onmission.toString(),
                        successTag, failTag, config, LOG))

                .apply(ParDo.of(new SetVehicleState<>(SetVehicleState.State.Locked, config)));

        afterLock.apply(Filter.by(lockVehicle -> lockVehicle.stateChanged))
                .apply(new UnlockVehicleStatusUpdate<LockVehicleMessage>(
                        VehicleStatus.available.toString(), config, LOG));

        afterLock.apply(Filter.by(lockVehicle -> !(lockVehicle.stateChanged)))
                .apply(new UnlockVehicleStatusUpdate<LockVehicleMessage>(
                        VehicleStatus.maintenance.toString(), config, LOG))
                .apply("Json Serialize",
                        MapElements.into(TypeDescriptors.strings()).via(lockVehicle -> {
                            return Json.format(lockVehicle, true);
                        }))
                .apply(KafkaIO.<Void, String>write()
                        .withBootstrapServers(config.kafkaSeed() + ":" + config.kafkaPort())
                        .withTopic("lock-failed").withValueSerializer(StringSerializer.class)
                        .values());

        p.run();
    }

    public static class LockVehicleMessage implements JobMessage, Serializable,
            VehicleMessageWithId, VehicleMessageWithDeviceId, VehicleMessageWithStateChanged,
            VehicleMessageWithVendor {

        private static final long serialVersionUID = 1L;
        public String deviceId;
        public boolean stateChanged;
        public UUID jobId;
        public UUID vehicleId;
        public String vendor;

        public String getVendor() {
            return vendor;
        }

        public void setVendor(String vendor) {
            this.vendor = vendor;
        }

        @Override
        public void setStateChanged(boolean stateChanged) {
            this.stateChanged = stateChanged;
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
        public UUID getVehicleId() {
            return this.vehicleId;
        }

        @Override
        public void setVehicleId(UUID vehicleId) {
            this.vehicleId = vehicleId;
        }

        @Override
        public UUID getJobId() {
            return this.jobId;
        }
    }
}
