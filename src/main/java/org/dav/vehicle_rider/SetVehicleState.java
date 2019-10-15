package org.dav.vehicle_rider;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.dav.config.Config;
import org.dav.vehicle_rider.messages.VehicleMessageWithDeviceId;
import org.dav.vehicle_rider.messages.VehicleMessageWithId;
import org.dav.vehicle_rider.messages.VehicleMessageWithStateChanged;
import org.dav.vehicle_rider.messages.VehicleMessageWithVendor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.UUID;

public class SetVehicleState<T extends VehicleMessageWithId & VehicleMessageWithDeviceId & VehicleMessageWithStateChanged & VehicleMessageWithVendor>
        extends DoFn<T, T> {

    private static final long serialVersionUID = 1L;

    private String cassandraHost;
    private Cluster cluster;
    private String kafkaHost;
    private ConsistencyLevel consistencyLevel;
    private Session session;
    private KafkaProducer<String,String> kafkaProducer;
    private SendCommandToVehicleController sendCommandToVehicleController;
    private State state;
    private static final Logger LOG = LoggerFactory.getLogger(SendCommandToVehicleController.class);

    public enum State {
        Locked, Unlocked
    }

    public SetVehicleState(State state, Config config) {
        this.state = state;
        this.cassandraHost = config.cassandraSeed();
        this.consistencyLevel = ConsistencyLevel.ONE;
        this.kafkaHost = config.kafkaSeed() + ":" + config.kafkaPort();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        T message = context.element();
        String deviceId = message.getDeviceId();
        String vendor = message.getVendor();
        UUID vehicleId = message.getVehicleId();
        if (deviceId != null && deviceId.length() > 0) {
            try {
                boolean success = sendCommandToVehicleController.sendCommand(vehicleId, deviceId,
                        vendor, state.name().toLowerCase());
                message.setStateChanged(success);
                LOG.info(String.format("setStateChanged: %s",success?"true":"false"));
            } catch (Exception e) {
                LOG.error("Failed to set vehicle state", e);
                LOG.info("setStateChanged: false");
                message.setStateChanged(false);
            }
        } else {
            LOG.info("Empty device ID");
            message.setStateChanged(true); // No real device. Assume changed.
            LOG.info("setStateChanged: true");
        }
        context.output(message);
    }

    @Setup
    public void setup() {
        try {
            cluster = Cluster.builder().withLoadBalancingPolicy(new RoundRobinPolicy())
                    .addContactPoint(cassandraHost)
                    .withQueryOptions(new QueryOptions().setConsistencyLevel(consistencyLevel))
                    .build();
            session = cluster.connect();
            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaHost);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProducer = new KafkaProducer<String,String>(props);
            sendCommandToVehicleController =
                    new SendCommandToVehicleController(session, kafkaProducer);
        } catch (Exception ex) {
            LOG.error("Setup Error",ex);
        }
    }

    @Teardown
    public void teardown() throws Exception {
        if (session != null) {
            session.close();
        }
        if (cluster != null) {
            cluster.close();
        }
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
    }

}
