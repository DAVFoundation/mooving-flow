package org.dav.vehicle_rider;

import com.google.auth.Credentials;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.auth.GcpCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.common.serialization.StringSerializer;
import org.dav.Json;
import org.dav.config.Config;
import org.dav.vehicle_rider.BlueSnapRequests.GetPaymentInfo;
import org.dav.vehicle_rider.cassandra.RideSummary;
import org.dav.vehicle_rider.cassandra.RideSummaryEffectiveDate;
import org.dav.vehicle_rider.cassandra_helpers.GetOwnerDetailsDoFn;
import org.dav.vehicle_rider.cassandra_helpers.GetRideDetailsDoFn;
import org.dav.vehicle_rider.cassandra_helpers.GetRiderPaymentMethod;
import org.dav.vehicle_rider.cassandra_helpers.GetVehicleDetails;
import org.dav.vehicle_rider.messages.*;
import org.dav.vehicle_rider.rider_payment.InvoiceGenerator;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

public class EndRideFlow {

    private static final Logger LOG = LoggerFactory.getLogger(EndRideFlow.class);
    private static final String CASSANDRA_SCEHME_NAME = "vehicle_rider";

    public static void runEndRideFlow(String[] args) {
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.create(true, args);

        ParameterTool parameters = ParameterTool.fromArgs(args);
        String timezoneApiKey = parameters.get("timezone_api_key");

        Credentials gcpCredentials = new GcpCredentialFactory().getCredential();
        config.pipelineOptions().as(GcpOptions.class).setGcpCredential(gcpCredentials);
        Pipeline p = Pipeline.create(config.pipelineOptions());

        PCollection<EndRideMessage> endRideMessages = p
                .apply("ReadLines",
                        KafkaIO.readBytes().withBootstrapServers(config.kafkaSeed() + ":" + config.kafkaPort())
                                .withTopic("end-ride")
                                .updateConsumerProperties(ImmutableMap.of("group.id", "lock-vehicle"))
                                .commitOffsetsInFinalize().withoutMetadata())

                .apply(MapElements.into(TypeDescriptors.strings()).via(t -> {
                    return new String(t.getValue());
                }))

                .apply(MapElements.into(TypeDescriptor.of(EndRideMessage.class)).via(message -> {
                    try {
                        final GsonBuilder builder = new GsonBuilder();
                        builder.excludeFieldsWithoutExposeAnnotation();
                        final Gson gson = builder.create();
                        return gson.fromJson(message, EndRideMessage.class);
                    } catch (Exception e) {
                        LOG.info("Error deserializing lock message: " + e.toString());
                        throw e;
                    }
                }))

                .apply("get active ride details", ParDo.of(new GetRideDetailsDoFn<>(config.cassandraSeed(), LOG)))

                .apply("get ride local time", ParDo.of(new GetRideLocalTime<>(timezoneApiKey, LOG)))

                .apply("get vehicle owner and price details",
                        ParDo.of(new GetVehicleDetails(config.cassandraSeed(), LOG)))

                .apply("get owner payment details", ParDo.of(new GetOwnerDetailsDoFn<>(config.cassandraSeed(), LOG)))

                .apply("get conversion rates", ParDo.of(new GetConversionRateDoFn(config.cassandraSeed(), LOG)))

                .apply(ParDo.of(new DoFn<EndRideMessage, EndRideMessage>() {
                    private static final long serialVersionUID = 1L;

                    @ProcessElement
                    public void ProcessElement(ProcessContext c) {
                        try {
                            EndRideMessage message = c.element();
                            long rideTime = (message.getLastTime().getTime() - message.getStartTime().getTime()) / 60000;
                            BigDecimal timedRidePrice = message.getPricePerMinute().multiply(BigDecimal.valueOf(rideTime));
                            message.setRidePrice(message.getBasePrice().add(timedRidePrice));
                            c.output(message);
                        } catch (Exception e) {
                            LOG.error("Error: price calculation failed", e);
                        }
                    }
                }));

        PCollection<RideSummary> rideSummaries = endRideMessages
                .apply("remove active ride", ParDo.of(new RemoveActiveRideDoFn<>(config.cassandraSeed(), LOG)))

                .apply(MapElements.into(TypeDescriptor.of(RideSummary.class)).via(endRide -> {
                    try {
                        final BigDecimal ridePrice = endRide.getRidePrice();
                        final BigDecimal ridePriceInDav = Helpers.calculateDavPrice(ridePrice, endRide.getConversionRate());
                        final BigDecimal maxDavAwarded = ridePriceInDav.subtract(ridePriceInDav.multiply(endRide.getCommissionFactor()));
                        BigDecimal davAwarded = endRide.getRewardBase() != null ? endRide.getRewardBase() : BigDecimal.ZERO;
                        if (endRide.getRewardFactor() != null) {
                            davAwarded = davAwarded.add(ridePriceInDav.multiply(endRide.getRewardFactor()));
                        }
                        davAwarded = davAwarded.min(maxDavAwarded);
                        return new RideSummary(endRide.getOwnerId(), // ownerId
                                endRide.getRiderId(), endRide.getVehicleId(), endRide.getStartTime(),
                                endRide.getLastTime(), endRide.getEffectiveDate(),
                                endRide.getStartGeoHash(), endRide.getLastGeoHash(),
                                endRide.getParkingImageUrl(), (byte) 0, // rating
                                endRide.getRidePrice(), // price
                                endRide.getFiatCurrencyCode(), // currencyCode
                                endRide.getConversionRate(), // conversionRate
                                endRide.getCommissionFactor(), // commissionFactor
                                endRide.getRidePrice().multiply(endRide.getCommissionFactor()), // commission
                                endRide.getPaymentDAVFactor(), // paymentDavFactor
                                endRide.getAuthTransactionId(),
                                endRide.getRideDistance(),
                                davAwarded);
                    } catch (Exception e) {
                        LOG.error("Error creating RideSummary", e);
                        return null;
                    }
                }))

                .apply("Filter NULLs", Filter.by(obj -> obj != null));

        rideSummaries.apply("write ride summaries to cassandra",
                CassandraIO.<RideSummary>write().withHosts(Arrays.asList(config.cassandraSeed()))
                        .withPort(config.cassandraPort()).withKeyspace(CASSANDRA_SCEHME_NAME)
                        .withEntity(RideSummary.class));

        rideSummaries.apply(new LastParkingPhotoUpdate(config, LOG));

        rideSummaries.apply("map ride summaries to ride summaries effective date",
                MapElements.into(TypeDescriptor.of(RideSummaryEffectiveDate.class))
                        .via(rideSummary -> new RideSummaryEffectiveDate(rideSummary)))

                .apply("write ride summaries effective date to cassandra",
                        CassandraIO.<RideSummaryEffectiveDate>write().withHosts(Arrays.asList(config.cassandraSeed()))
                                .withPort(config.cassandraPort()).withKeyspace(CASSANDRA_SCEHME_NAME)
                                .withEntity(RideSummaryEffectiveDate.class));

        endRideMessages.apply("Json Serialize", MapElements.into(TypeDescriptors.strings()).via(rideMessage -> {
            LockVehicleFlow.LockVehicleMessage lockVehicleMessage = new LockVehicleFlow.LockVehicleMessage();
            lockVehicleMessage.setVehicleId(rideMessage.getVehicleId());
            lockVehicleMessage.setDeviceId(rideMessage.getDeviceId());
            lockVehicleMessage.setVendor(rideMessage.getVendor());
            return Json.format(lockVehicleMessage, false);
        })).apply("Send message to lock vehicle",
                KafkaIO.<Void, String>write().withBootstrapServers(config.kafkaSeed() + ":" + config.kafkaPort())
                        .withTopic("lock-vehicle").withValueSerializer(StringSerializer.class).values());

                        
        PCollection<EndRideMessage> endRideMessagesCompleted = endRideMessages
                .apply("check if DAV payment allowed", ParDo.of(new IsDavPaymentAllowed(config.cassandraSeed(), LOG)))
                .apply("get payment method", ParDo.of(new GetPaymentMethod(config.cassandraSeed(), LOG)));

        endRideMessagesCompleted.apply("Json Serialize", MapElements.into(TypeDescriptors.strings()).via(rideMessage -> {
            UpdateDavBalance.UpdateDavBalanceMessage updateDavBalanceMessage = new UpdateDavBalance.UpdateDavBalanceMessage();
            updateDavBalanceMessage.paymentMethodDav = rideMessage.isPaymentMethodDAV();
            updateDavBalanceMessage.riderId = rideMessage.getRiderId();
            updateDavBalanceMessage.vehicleId = rideMessage.getVehicleId();
            updateDavBalanceMessage.startTime = rideMessage.getStartTime();
            return Json.format(updateDavBalanceMessage, false);
        }))
        .apply("Send message to update DAV balance",
                KafkaIO.<Void, String>write().withBootstrapServers(config.kafkaSeed() + ":" + config.kafkaPort())
                        .withTopic("update-dav-balance").withValueSerializer(StringSerializer.class).values());

        generateInvoiceFlow(endRideMessagesCompleted, config, LOG, parameters);

        p.run();
    }

    public static void generateInvoiceFlow(PCollection<EndRideMessage> endRideMessages, Config config, Logger LOG, ParameterTool parameters) {

        String bluesnapApiUser = parameters.get("bluesnap_api_user");
        String bluesnapApiPass = parameters.get("bluesnap_api_pass");
        String bluesnapUrl = parameters.get("bluesnap_url");
        String gcsBucket = parameters.get("gcs_bucket");

        LOG.info("starting invoice flow");

        endRideMessages.apply(ParDo.of(new DoFn<EndRideMessage, InvoiceGenerator>() {
                private static final long serialVersionUID = 1L;
                @ProcessElement
                public void ProcessElement(ProcessContext c) {
                    try {
                        EndRideMessage message = c.element();
                        InvoiceGenerator invoiceData = new InvoiceGenerator(message, LOG, gcsBucket);
                        c.output(invoiceData);
                    } catch (Exception e) {
                        LOG.error("Error (invoice creation failed): failed to get endRideMessages data", e);
                    }
                }
            }))
            .apply("Get rider payment method for invoice",
                    ParDo.of(new GetRiderPaymentMethod<>(config.cassandraSeed(), LOG)))
            .apply("get payment info for invoice",
                    ParDo.of(new GetPaymentInfo(LOG, bluesnapApiUser, bluesnapApiPass, bluesnapUrl)))
            .apply("Filter NULLs", Filter.by(obj -> obj != null))
            .apply(ParDo.of(new DoFn<InvoiceGenerator, InvoiceGenerator>() {
                private static final long serialVersionUID = 1L;
                @ProcessElement
                public void ProcessElement(ProcessContext c) {
                    try {
                        InvoiceGenerator invoiceData = c.element();
                        invoiceData.generateInvoice();
                    } catch (Exception e) {
                        LOG.error("Error (invoice creation failed): failed to get endRideMessages data", e);
                    }
                }
            }));
    }

    public static class EndRideMessage implements JobMessage, Serializable, VehicleMessageWithId,
            VehicleMessageWithDeviceId, VehicleMessageWithOwnerDetails, VehicleMessageWithStateChanged,
            VehicleMessageConversionRatesDetails, VehicleMessageWithRiderId, VehicleMessageWithOwnerId,
            VehicleMessageWithPrice, VehicleMessageWithRideDetails {

        private static final long serialVersionUID = 1L;

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
        private String _authTransactionId;

        @Nullable
        private String _startGeoHash;

        @Nullable
        private String _lastGeoHash;

        @Nullable
        private byte _startBatteryPercentage;

        @Nullable
        private byte _lastBatteryPercentage;

        @Nullable
        private Date _startTime;

        @Nullable
        private Date _lastTime;

        @Nullable
        private LocalDate _effectiveDate;

        @Nullable
        private boolean _removed = false;

        @Nullable
        @SerializedName("parkingImageUrl")
        @Expose(deserialize = true)
        private String _parkingImageUrl;

        @Nullable
        @SerializedName("paymentMethodDAV")
        @Expose(deserialize = true)
        private boolean _paymentMethodDAV;

        @Nullable
        private String _fiatCurrencyCode;

        @Nullable
        private BigDecimal _conversionRate;

        @Nullable
        private boolean _pricingUsingFiat;

        @Nullable
        private BigDecimal _basePrice;

        @Nullable
        private BigDecimal _pricePerMinute;

        @Nullable
        private BigDecimal _ridePrice;

        @Nullable
        private BigDecimal _commissionFactor;

        @Nullable
        private BigDecimal _paymentDAVFactor;

        @Nullable
        private String _companyName;

        @Nullable
        private BigDecimal _rewardBase;
        
        @Nullable
        private BigDecimal _rewardFactor;

        @Nullable
        private BigDecimal _rideDistance;

        public boolean stateChanged;

        @Nullable
        public String vendor;

        public String getVendor() {
            return vendor;
        }

        public void setVendor(String vendor) {
            this.vendor = vendor;
        }

        public EndRideMessage() {

        }

        public void setCompanyName(String _companyName) {
            this._companyName = _companyName;
        }

        public String getCompanyName() {
            return _companyName;
        }

        @Override
        public UUID getJobId() {
            return this._jobId;
        }

        @Override
        public UUID getRiderId() {
            return this._riderId;
        }

        @Override
        public void setRiderId(UUID riderId) {
            _riderId = riderId;
        }

        @Override
        public UUID getVehicleId() {
            return this._vehicleId;
        }

        @Override
        public void setVehicleId(UUID _vehicleId) {
            this._vehicleId = _vehicleId;
        }

        @Override
        public UUID getOwnerId() {
            return _ownerId;
        }

        @Override
        public void setOwnerId(UUID _ownerId) {
            this._ownerId = _ownerId;
        }

        public String getDeviceId() {
            return this._deviceId;
        }

        public void setDeviceId(String deviceId) {
            this._deviceId = deviceId;
        }

        public String getAuthTransactionId() {
            return this._authTransactionId;
        }

        @Override
        public void setAuthTransactionId(String authTransactionId) {
            this._authTransactionId = authTransactionId;
        }

        public String getStartGeoHash() {
            return this._startGeoHash;
        }

        public void setStartGeoHash(String geoHash) {
            this._startGeoHash = geoHash;
        }

        public byte getStartBatteryPercentage() {
            return this._startBatteryPercentage;
        }

        @Override
        public void setStartBatteryPercentage(byte batteryPercentage) {
            this._startBatteryPercentage = batteryPercentage;
        }

        public byte getLastBatteryPercentage() {
            return _lastBatteryPercentage;
        }

        public void setLastBatteryPercentage(byte lastBatteryPercentage) {
            this._lastBatteryPercentage = lastBatteryPercentage;
        }

        public Date getStartTime() {
            return _startTime;
        }

        @Override
        public void setStartTime(Date startTime) {
            this._startTime = startTime;
        }

        public Date getLastTime() {
            return _lastTime;
        }

        public void setLastTime(Date lastTime) {
            this._lastTime = lastTime;
        }

        public LocalDate getEffectiveDate() {
            return _effectiveDate;
        }

        public void setEffectiveDate(LocalDate effectiveDate) {
            this._effectiveDate = effectiveDate;
        }

        public String getLastGeoHash() {
            return _lastGeoHash;
        }

        public void setLastGeoHash(String _lastGeoHash) {
            this._lastGeoHash = _lastGeoHash;
        }

        public boolean isRemoved() {
            return _removed;
        }

        public void setRemoved(boolean removed) {
            this._removed = removed;
        }

        public String getParkingImageUrl() {
            return _parkingImageUrl;
        }

        public void setParkingImageUrl(String parkingImageUrl) {
            this._parkingImageUrl = parkingImageUrl;
        }

        public boolean isPaymentMethodDAV() {
            return _paymentMethodDAV;
        }

        public void setPaymentMethodDAV(boolean paymentMethodDAV) {
            this._paymentMethodDAV = paymentMethodDAV;
        }

        public String getFiatCurrencyCode() {
            return _fiatCurrencyCode;
        }

        public void setFiatCurrencyCode(String fiatCurrencyCode) {
            this._fiatCurrencyCode = fiatCurrencyCode;
        }

        public boolean isPricingUsingFiat() {
            return _pricingUsingFiat;
        }

        public void setPricingUsingFiat(boolean pricingUsingFiat) {
            this._pricingUsingFiat = pricingUsingFiat;
        }

        public BigDecimal getBasePrice() {
            return _basePrice;
        }

        public void setBasePrice(BigDecimal basePrice) {
            this._basePrice = basePrice;
        }

        public BigDecimal getPricePerMinute() {
            return _pricePerMinute;
        }

        public void setPricePerMinute(BigDecimal pricePerMinute) {
            this._pricePerMinute = pricePerMinute;
        }

        public BigDecimal getRidePrice() {
            return _ridePrice;
        }

        public void setRidePrice(BigDecimal ridePrice) {
            this._ridePrice = ridePrice;
        }

        public BigDecimal getRewardBase() {
            return this._rewardBase;
        }

        public void setRewardBase(BigDecimal rewardBase) {
            this._rewardBase = rewardBase;
        }

        public BigDecimal getRewardFactor() {
            return this._rewardFactor;
        }

        public void setRewardFactor(BigDecimal rewardFactor) {
            this._rewardFactor = rewardFactor;
        }

        public BigDecimal getConversionRate() {
            return _conversionRate;
        }

        public void setConversionRate(BigDecimal conversionRate) {
            this._conversionRate = conversionRate;
        }

        public BigDecimal getRideDistance() {
            return _rideDistance;
        }

        public void setRideDistance(BigDecimal rideDistance) {
            this._rideDistance = rideDistance;
        }

        public BigDecimal getCommissionFactor() {
            return _commissionFactor;
        }

        public void setCommissionFactor(BigDecimal commissionFactor) {
            this._commissionFactor = commissionFactor;
        }

        public BigDecimal getPaymentDAVFactor() {
            return _paymentDAVFactor;
        }

        public void setPaymentDAVFactor(BigDecimal paymentDAVFactor) {
            this._paymentDAVFactor = paymentDAVFactor;
        }

        @Override
        public void setAuthCurrency(String authCurrency) {
            // Ignored for this message type
        }

        @Override
        public void setStateChanged(boolean stateChanged) {
            this.stateChanged = stateChanged;
        }
    }
}
