package org.dav.vehicle_rider;

import java.util.UUID;
import com.datastax.driver.core.BoundStatement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.dav.config.Config;
import org.dav.vehicle_rider.CassandraDoFnBase;
import org.dav.vehicle_rider.messages.JobMessage;
import org.dav.vehicle_rider.messages.VehicleMessageWithId;
import org.slf4j.Logger;

public class UnlockVehicleStatusUpdate<T extends VehicleMessageWithId & JobMessage>
        extends PTransform<PCollection<T>, PCollection<T>> {
    private static final long serialVersionUID = 1L;
    private UnlockVehicleStatusUpdateDoFn<T> unlockVehicleStatusUpdateDoFn;
    private UpdateUserJobState<T> updateUserJobState;

    public UnlockVehicleStatusUpdate(Object unlockStatus, Config config, Logger log) {
        this.unlockVehicleStatusUpdateDoFn = new UnlockVehicleStatusUpdateDoFn<>(unlockStatus, config, log);
        this.updateUserJobState = null;
    }

    public UnlockVehicleStatusUpdate(Object unlockStatus, Config config, Logger log, boolean isJobFailed) {
        this(unlockStatus, config, log);
        this.updateUserJobState = new UpdateUserJobState<>(UserJobState.failed, config.cassandraSeed(), log);
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        PCollection<T> results = input.apply(
                String.format("Unlock state and update to '%s'",
                        this.unlockVehicleStatusUpdateDoFn.unlockStatus),
                ParDo.of(this.unlockVehicleStatusUpdateDoFn));
        if (this.updateUserJobState != null) {
            results = results.apply("Update job to failed", ParDo.of(this.updateUserJobState));
        }
        return results;
    }
}

class UnlockVehicleStatusUpdateDoFn<T extends VehicleMessageWithId> extends CassandraDoFnBase<T, T> {
    private static final long serialVersionUID = 1L;
    private static final String QUERY = "UPDATE vehicle_rider.vehicles SET status=?, in_transition=false WHERE id=?";

    public Object unlockStatus;

    public UnlockVehicleStatusUpdateDoFn(Object unlockStatus, Config config, Logger log) {
        super(config.cassandraSeed(), QUERY, log);
        this.unlockStatus = unlockStatus;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        try {
            T message = context.element();
            UUID vehicleUUID = message.getVehicleId();
            BoundStatement bound = _prepared.bind(this.unlockStatus, vehicleUUID);
            this.executeBoundStatement(bound);
            context.output(message);
        } catch (Exception ex) {
            _log.error(String.format("error while trying to update vehicle status to %s", unlockStatus.toString()), ex);
        }
    }
}
