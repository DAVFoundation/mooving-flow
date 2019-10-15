package org.dav.vehicle_rider;

import org.dav.Json;
import org.dav.config.Config;
import org.dav.vehicle_rider.cassandra.Vehicle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.github.davidmoten.geo.GeoHash;

public class VehicleUpdatesFlow {
  private static final Logger LOG = LoggerFactory.getLogger(VehicleUpdatesFlow.class);

  public static void main(String[] args) {
    Config config = Config.create(true, args);
    Pipeline p = Pipeline.create(config.pipelineOptions());

    p.apply("Read Kafka",
        KafkaIO.<Long, String>read()
            .withBootstrapServers(config.kafkaSeed() + ":" + config.kafkaPort())
            .withTopic("iot-updates").withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .updateConsumerProperties(ImmutableMap.of("group.id", "my_beam_app_1"))
            .withReadCommitted().commitOffsetsInFinalize().withoutMetadata())

        .apply("Take Values", Values.<String>create())

        .apply("Parse JSON", ParDo.of(new DoFn<String, IotUpdateMessage>() {
          private static final long serialVersionUID = 1L;

          @ProcessElement
          public void processElement(@Element String iotUpdateMessage,
              OutputReceiver<IotUpdateMessage> output) {
            try {
              IotUpdateMessage iotUpdate =
                  Json.parse(iotUpdateMessage, IotUpdateMessage.class, false);
              output.output(iotUpdate);
            } catch (Exception e) {
              LOG.error("Parse JSON", e);
            }
          }
        }))
        .apply("Query Vehicle from DB",
            new CassandraQuery<IotUpdateMessage, Vehicle>(
                "SELECT * FROM vehicle_rider.vehicles WHERE device_id=? ALLOW FILTERING;",
                (i) -> new Object[] {i.deviceId}, Vehicle.class,
                (i, v) -> VehicleUpdatesFlow.updateVehicle(i, v), config.cassandraSeed(),
                ConsistencyLevel.ONE))
        .apply("Update DB", new CassandraUpdate<Vehicle>("vehicle_rider", "vehicles",
            (v) -> new HashMap<String, Object>() {
              private static final long serialVersionUID = 1L;
              {
                put("id", v.id);
                put("battery_percentage", v.batteryPercentage);
                put("geo_hash", v.geoHash);
                put("device_state", v.deviceState);
              }
            }, config.cassandraSeed(), ConsistencyLevel.ONE));
    p.run();
  }

  public static Vehicle updateVehicle(IotUpdateMessage iotUpdate, Vehicle vehicle) {
    try {
      if (iotUpdate.batteryPercentage != null) {
        vehicle.batteryPercentage = iotUpdate.batteryPercentage.byteValue();
      } else if (iotUpdate.batteryVoltage != null) {
        vehicle.batteryPercentage = (byte) Math.floor(Math.min(100, Math.max(0,
            (vehicle.batteryVoltageFactorAlpha.floatValue() * iotUpdate.batteryVoltage.floatValue())
                + vehicle.batteryVoltageFactorBeta.floatValue())));
      }

      if (iotUpdate.geohash != null) {
        vehicle.geoHash = iotUpdate.geohash;
      } else if (iotUpdate.latitude != null && iotUpdate.longitude != null) {
        vehicle.geoHash = GeoHash.encodeHash(iotUpdate.latitude, iotUpdate.longitude);
      }

      if (iotUpdate.state != null) {
        vehicle.deviceState = iotUpdate.state;
      }
    } catch (Exception e) {
      LOG.error("updateVehicle Error: ", e);
    }
    return vehicle;
  }
}
