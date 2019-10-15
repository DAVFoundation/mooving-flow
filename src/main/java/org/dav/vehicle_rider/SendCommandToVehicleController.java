package org.dav.vehicle_rider;

import com.datastax.driver.core.*;
import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SendCommandToVehicleController implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Command {
        Lock, Unlock
    }

    private static final int MAX_RETRIES = 6;
    private static final int MAX_POLLING_RETRIES = 60;
    private static final int POLLING_INTERVAL = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(SendCommandToVehicleController.class);

    private final Session cassandraSession;
    private final KafkaProducer<String, String> kafkaProducer;
    private PreparedStatement prepared;

    public SendCommandToVehicleController(Session cassandraSession,
            KafkaProducer<String, String> kafkaProducer) {
        this.cassandraSession = cassandraSession;
        this.kafkaProducer = kafkaProducer;

        String query = "SELECT device_state FROM vehicle_rider.vehicles WHERE id=?;";
        prepared = cassandraSession.prepare(query);
    }

    public boolean sendCommand(UUID vehicleId, String deviceId, String vendor, String command)
            throws InterruptedException {
        BoundStatement bound = prepared.bind(vehicleId);
        String message =
                String.format("{\"deviceId\": \"%s\", \"vendor\": \"%s\", \"command\": \"%s\"}",
                        deviceId, vendor, command);
        int retry = 0;
        while (retry < MAX_RETRIES) {
            String topicName = String.format("iot-commands-%s", vendor);
            kafkaProducer.send(new ProducerRecord<String, String>(topicName, message));

            int pollingRetry = 0;
            while (pollingRetry < MAX_POLLING_RETRIES) {
                ResultSet resultSet = cassandraSession.execute(bound);
                Row row = resultSet.one();
                String deviceState = row.getString("device_state");
                if (deviceState.equals(command)) {
                    return true;
                }
                pollingRetry++;
                Thread.sleep(POLLING_INTERVAL);
            }
            retry++;
        }

        return false;
    }
}
