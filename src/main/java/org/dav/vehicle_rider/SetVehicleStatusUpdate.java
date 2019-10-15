package org.dav.vehicle_rider;

import java.util.UUID;
import com.datastax.driver.core.BoundStatement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.dav.config.Config;
import org.dav.vehicle_rider.messages.VehicleMessageWithId;
import org.slf4j.Logger;

public class SetVehicleStatusUpdate<T extends VehicleMessageWithId>
        extends PTransform<PCollection<T>, PCollection<T>> {
    private static final long serialVersionUID = 1L;
    private SetVehicleStatusUpdateDoFn<T> unlockVehicleStatusUpdateDoFn;

    public SetVehicleStatusUpdate(Object unlockStatus, Config config, Logger log) {
        this.unlockVehicleStatusUpdateDoFn =
                new SetVehicleStatusUpdateDoFn<>(unlockStatus, config, log);
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        return input.apply(
                String.format("Update state to '%s'",
                        this.unlockVehicleStatusUpdateDoFn.status),
                ParDo.of(this.unlockVehicleStatusUpdateDoFn));
    }
}


class SetVehicleStatusUpdateDoFn<T extends VehicleMessageWithId> extends CassandraDoFnBase<T, T> {
    private static final long serialVersionUID = 1L;
    private static final String QUERY = "UPDATE vehicle_rider.vehicles SET status=? WHERE id=?";

    public Object status;

    public SetVehicleStatusUpdateDoFn(Object status, Config config, Logger log) {
        super(config.cassandraSeed(), QUERY, log);
        this.status = status;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        try {
            T message = context.element();
            UUID vehicleUUID = message.getVehicleId();
            BoundStatement bound = _prepared.bind(this.status, vehicleUUID);
            this.executeBoundStatement(bound);
        } catch (Exception ex) {
            _log.error(String.format("error while trying to update vehicle status to %s", status.toString()),
                    ex);
        }
    }
}
