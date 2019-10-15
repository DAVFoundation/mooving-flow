
package org.dav.vehicle_rider;

import avro.shaded.com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.dav.Json;
import org.dav.config.Config;
import org.dav.vehicle_rider.cassandra_helpers.UpdateOwnerDavBalanceDelta;
import org.dav.vehicle_rider.messages.IDavBalanceDeltaUpdateMessage;
import org.dav.vehicle_rider.token_payment.TokenPaymentMessage;
import org.dav.vehicle_rider.token_payment.TransactionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;

public class UpdateDavBalance {
  private static final Logger LOG = LoggerFactory.getLogger(UpdateDavBalance.class);

  public static void main(String[] args) {
    Config config = Config.create(true, args);

    Pipeline p = Pipeline.create(config.pipelineOptions());
    PCollection<UpdateDavBalanceMessage> updateDavBalanceMessages = p
        .apply("Read from kafka", KafkaIO.<Long, String>read().withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withBootstrapServers(String.format("%s:%s", config.kafkaSeed(), config.kafkaPort()))
            .withTopic("update-dav-balance").updateConsumerProperties(ImmutableMap.of("group.id", "DavBalanceUpdate"))
            .withReadCommitted().commitOffsetsInFinalize().withoutMetadata())
        .apply(Values.create())

        .apply(MapElements.into(TypeDescriptor.of(UpdateDavBalanceMessage.class)).via(message -> {
          try {
            return Json.parse(message, UpdateDavBalanceMessage.class, false);
          } catch (Exception e) {
            LOG.error("Parse JSON", e);
            return null;
          }
        })).apply("get ride data", ParDo.of(new GetRideData(config.cassandraSeed(), LOG)))
        .apply("Filter NULLs", Filter.by(obj -> obj != null));

    updateDavBalanceMessages
        .apply("Filter rides with payment method dav", Filter.by(message -> message.paymentMethodDav))
        .apply("update RideSummary", ParDo.of(new UpdateRideSummaryPaymentMethodDav(config.cassandraSeed(), LOG)));
    updateDavBalanceMessages
        .apply("Filter rides with payment method dav", Filter.by(message -> !message.paymentMethodDav))
        .apply("update RideSummary", ParDo.of(new UpdateRideSummaryPaymentMethodFiat(config.cassandraSeed(), LOG)));
    updateDavBalanceMessages.apply("update RideSummaryEffectiveDate",
        ParDo.of(new UpdateRideSummaryEffectiveDate(config.cassandraSeed(), LOG)));

    PCollection<UpdateDavBalanceMessage> riderUpdateDavBalanceMessages = updateDavBalanceMessages
        .apply("get rider DAV balance", ParDo.of(new GetRiderDavBalance(config.cassandraSeed(), LOG)));

    riderUpdateDavBalanceMessages.apply("update rider dav balance",
        ParDo.of(new UpdateRiderDavBalance(config.cassandraSeed(), LOG)));

    PCollection<UpdateDavBalanceMessage> riderPayedWithDavMessage = riderUpdateDavBalanceMessages.apply(
        "Filter rides with paymentMethodDav",
        Filter.by(riderUpdateDavBalanceMessage -> riderUpdateDavBalanceMessage.paymentMethodDav));

    riderPayedWithDavMessage.apply("update owner dav balance",
        ParDo.of(new UpdateOwnerDavBalanceDelta<UpdateDavBalanceMessage>(config.cassandraSeed(), LOG)));

    riderPayedWithDavMessage
        .apply("Create token payment message",
            MapElements.into(TypeDescriptor.of(TokenPaymentMessage.class))
                .via(updateDavBalanceMessage -> new TokenPaymentMessage(TransactionType.RiderToOwner,
                    updateDavBalanceMessage.getDavBalanceDelta(), updateDavBalanceMessage.ownerId)))
        .apply("Json Serialize",
            MapElements.into(TypeDescriptors.strings())
                .via(tokenPaymentMessage -> Json.format(tokenPaymentMessage, false)))
        .apply("Send token payment message",
            KafkaIO.<Void, String>write().withBootstrapServers(config.kafkaSeed() + ":" + config.kafkaPort())
                .withTopic("token-payment").withValueSerializer(StringSerializer.class).values());

    p.run();
  }

  static class UpdateDavBalanceMessage implements IDavBalanceDeltaUpdateMessage, Serializable {

    private static final long serialVersionUID = 1L;
    public UUID riderId;
    public UUID vehicleId;
    public UUID ownerId;
    public Date startTime;
    public boolean paymentMethodDav;
    public BigDecimal price;
    public BigDecimal davRate;
    public BigDecimal davAwarded;
    public BigDecimal riderDavBalance;
    public org.joda.time.LocalDate effectiveDate;

    @Override
    public UUID getOwnerId() {
      return ownerId;
    }

    @Override
    public BigDecimal getDavBalanceDelta() {
      return Helpers.calculateDavPrice(price, davRate);
    }
  }
}
