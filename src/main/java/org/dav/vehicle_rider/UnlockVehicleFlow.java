package org.dav.vehicle_rider;

import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
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
import org.dav.vehicle_rider.cassandra.RiderActiveRide;
import org.dav.vehicle_rider.cassandra_helpers.GetOwnerDetailsDoFn;
import org.dav.vehicle_rider.cassandra_helpers.GetRiderPaymentMethod;
import org.dav.vehicle_rider.cassandra_helpers.GetVehicleDetailsByQrCode;
import org.dav.vehicle_rider.messages.*;
import org.dav.vehicle_rider.payment.CreateAuthTxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.UUID;

public class UnlockVehicleFlow {
    private static final Logger LOG = LoggerFactory.getLogger(UnlockVehicleFlow.class);

    public static void main(String[] args) throws Exception {

        Config config = Config.create(true, args);

        ParameterTool parameters = ParameterTool.fromArgs(args);
        String bluesnapApiUser = parameters.get("bluesnap_api_user");
        String bluesnapApiPass = parameters.get("bluesnap_api_pass");
        String bluesnapUrl = parameters.get("bluesnap_url");
        String defaultAuthAmountMinutesProperty = parameters.get("default_auth_amount_minutes");
        double defaultAuthAmountMinutes = defaultAuthAmountMinutesProperty != null
                ? Double.parseDouble(defaultAuthAmountMinutesProperty)
                : 0;

        TupleTag<UnlockVehicleMessage> successTag = new TupleTag<UnlockVehicleMessage>() {

            private static final long serialVersionUID = 1L;
        };
        TupleTag<UnlockVehicleMessage> failTag = new TupleTag<UnlockVehicleMessage>() {

            private static final long serialVersionUID = 1L;
        };

        Pipeline p = Pipeline.create(config.pipelineOptions());

        PCollection<UnlockVehicleMessage> unlockVehicleMessageAfterUnlock = p
                .apply("Read Kafka",
                        KafkaIO.<Long, String>read().withBootstrapServers(config.kafkaSeed() + ":" + config.kafkaPort())
                                .withTopic("unlock-vehicle").withKeyDeserializer(LongDeserializer.class)
                                .withValueDeserializer(StringDeserializer.class)
                                .updateConsumerProperties(ImmutableMap.of("group.id", "my_beam_app_1"))
                                .withReadCommitted().commitOffsetsInFinalize().withoutMetadata())

                .apply("Take Values", Values.<String>create())

                .apply("Parse JSON", MapElements.into(TypeDescriptor.of(UnlockVehicleMessage.class)).via(message -> {
                    try {
                        return Json.parse(message, UnlockVehicleMessage.class, true);
                    } catch (Exception e) {
                        LOG.error("Parse JSON", e);
                        return null;
                    }
                })).apply("Filter NULLs", Filter.by(obj -> obj != null))
                .apply("Set User Job To Started",
                        ParDo.of(new UpdateUserJobState<>(UserJobState.started, config.cassandraSeed(), LOG)))

                .apply("Get Vehicle Details",
                        ParDo.of(
                                new GetVehicleDetailsByQrCode<>(config.cassandraSeed(), defaultAuthAmountMinutes, LOG)))

                .apply(new LockVehicleStatusUpdate<UnlockVehicleMessage>(VehicleStatus.available.toString(),
                        VehicleStatus.available.toString(), successTag, failTag, config, LOG))

                .apply("Get Owner Details", ParDo.of(new GetOwnerDetailsDoFn<>(config.cassandraSeed(), LOG)))

                .apply("Get Rider Payment Method", ParDo.of(new GetRiderPaymentMethod<>(config.cassandraSeed(), LOG)))

                .apply("Create Auth Txn",
                        ParDo.of(new CreateAuthTxn<>(bluesnapApiUser, bluesnapApiPass, bluesnapUrl, LOG)))

                .apply(ParDo.of(new SetVehicleState<>(SetVehicleState.State.Unlocked, config)));

        unlockVehicleMessageAfterUnlock.apply(Filter.by(unlockVehicle -> !(unlockVehicle.stateChanged)))
                .apply(new UnlockVehicleStatusUpdate<UnlockVehicleMessage>(VehicleStatus.available.toString(), config,
                        LOG, true));

        unlockVehicleMessageAfterUnlock.apply(Filter.by(unlockVehicle -> unlockVehicle.stateChanged))
                .apply(new UnlockVehicleStatusUpdate<UnlockVehicleMessage>(VehicleStatus.onmission.toString(), config,
                        LOG))
                .apply("Create RiderActiveRide",
                        MapElements.into(TypeDescriptor.of(RiderActiveRide.class)).via(unlockVehicle -> {
                            try {
                                return new RiderActiveRide(unlockVehicle.getRiderId(), unlockVehicle.getVehicleId(),
                                        unlockVehicle.getGeoHash(), unlockVehicle.getBatteryPercentage(),
                                        unlockVehicle.getAuthTxn(), new BigDecimal(0));
                            } catch (Exception e) {
                                LOG.error("Create RiderActiveRide", e);
                                return null;
                            }
                        }))
                .apply("Filter NULLs", Filter.by(obj -> obj != null))

                .apply(MapElements.into(TypeDescriptor.of(RiderActiveRide.class)).via(t -> {
                    return t;
                })).apply("Write Cassandra",
                        CassandraIO.<RiderActiveRide>write().withHosts(Arrays.asList(config.cassandraSeed()))
                                .withPort(config.cassandraPort()).withKeyspace("vehicle_rider")
                                .withEntity(RiderActiveRide.class));

        p.run();
    }

    public static class UnlockVehicleMessage implements Serializable, JobMessage, VehicleMessageWithId,
            VehicleMessageWithDeviceId, VehicleMessageWithAuthTxn, VehicleMessageWithDetails,
            VehicleMessageWithOwnerDetails, VehicleMessageWithPaymentMethod, VehicleMessageWithStateChanged,
            VehicleMessageWithVendor {

        private static final long serialVersionUID = 1L;

        @SerializedName("qrCode")
        @Expose(deserialize = true)
        private String _qrCode;

        @SerializedName("riderId")
        @Expose(deserialize = true)
        private UUID _riderId;

        @SerializedName("jobId")
        @Expose(deserialize = true)
        private UUID _jobId;

        @Nullable
        private UUID _vehicleId;

        @Nullable
        private UUID _ownerId;

        @Nullable
        private String _deviceId;

        @Nullable
        private String _geoHash;

        @Nullable
        private byte _batteryPercentage;

        @Nullable
        private String _paymentMethodId;

        @Nullable
        private String _authTxn;

        @Nullable
        private String _authCurrency;

        @Nullable
        private BigDecimal _authAmount;

        @Nullable
        public boolean stateChanged;

        @Nullable
        public String vendor;

        @Override
        public String getVendor() {
            return vendor;
        }

        @Override
        public void setVendor(String vendorId) {
            this.vendor = vendorId;
        }

        public UnlockVehicleMessage() {

        }

        @Override
        public UUID getJobId() {
            return this._jobId;
        }

        public String getQrCode() {
            return this._qrCode;
        }

        public UUID getRiderId() {
            return this._riderId;
        }

        public UUID getVehicleId() {
            return this._vehicleId;
        }

        public UUID getOwnerId() {
            return this._ownerId;
        }

        public String getDeviceId() {
            return this._deviceId;
        }

        public String getGeoHash() {
            return this._geoHash;
        }

        public String getAuthTxn() {
            return this._authTxn;
        }

        public String getPaymentMethodId() {
            return this._paymentMethodId;
        }

        public byte getBatteryPercentage() {
            return this._batteryPercentage;
        }

        public void setVehicleId(UUID vehicleId) {
            this._vehicleId = vehicleId;
        }

        public void setOwnerId(UUID ownerId) {
            this._ownerId = ownerId;
        }

        public void setDeviceId(String deviceId) {
            this._deviceId = deviceId;
        }

        public void setGeoHash(String geoHash) {
            this._geoHash = geoHash;
        }

        public void setBatteryPercentage(byte batteryPercentage) {
            this._batteryPercentage = batteryPercentage;
        }

        public void setAuthTxn(String authTxn) {
            this._authTxn = authTxn;
        }

        public void setPaymentMethodId(String paymentMethodId) {
            this._paymentMethodId = paymentMethodId;
        }

        public String toString() {
            return "qrCode: " + this._qrCode + ", riderId: " + this._riderId + ", vehicleId: " + this._vehicleId
                    + ", geoHash: " + this._geoHash + ", batteryPercentage: " + (int) this._batteryPercentage;
        }

        public String getAuthCurrency() {
            return _authCurrency;
        }

        public void setAuthCurrency(String authCurrency) {
            _authCurrency = authCurrency;
        }

        public BigDecimal getAuthAmount() {
            return _authAmount;
        }

        public void setAuthAmount(BigDecimal authAmount) {
            _authAmount = authAmount;
        }

        @Override
        public void setFiatCurrencyCode(String fiatCurrencyCode) {
            // Ignored for this message type
        }

        @Override
        public void setPricingUsingFiat(boolean pricingUsingFiat) {
            // Ignored for this message type
        }

        @Override
        public void setCommissionFactor(BigDecimal commissionFactor) {
            // Ignored for this message type
        }

        @Override
        public void setPaymentDAVFactor(BigDecimal paymentDAVFactor) {
            // Ignored for this message type
        }

        @Override
        public void setStateChanged(boolean stateChanged) {
            this.stateChanged = stateChanged;
        }

        @Override
        public void setCompanyName(String companyName) {
            // Ignored for this message type
        }

        public void setRewardFactor(BigDecimal rewardFactor) {
            // Ignored for this message type
        }
    }
}
