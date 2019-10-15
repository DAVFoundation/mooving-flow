package org.dav.vehicle_rider;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.dav.config.Config;
import org.dav.vehicle_rider.messages.JobMessage;
import org.dav.vehicle_rider.messages.VehicleMessageWithId;
import org.slf4j.Logger;

import java.util.UUID;

public class LockVehicleStatusUpdate<T extends VehicleMessageWithId & JobMessage>
        extends PTransform<PCollection<T>, PCollection<T>> {
    private static final long serialVersionUID = 1L;
    private LockVehicleStatusUpdateDoFn<T> lockVehicleStatusUpdateDoFn;
    private UpdateUserJobState<T> updateUserJobState;
    private TupleTag<T> successTag;
    private TupleTag<T> failTag;

    public LockVehicleStatusUpdate(Object checkStatus, Object lockStatus, TupleTag<T> successTag, TupleTag<T> failTag,
            Config config, Logger log) {
        this.lockVehicleStatusUpdateDoFn = new LockVehicleStatusUpdateDoFn<>(checkStatus, lockStatus, successTag,
                failTag, config, log);
        this.updateUserJobState = new UpdateUserJobState<>(UserJobState.failed, config.cassandraSeed(), log);
        this.successTag = successTag;
        this.failTag = failTag;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        PCollectionTuple stateChangeCollection = input.apply(
                String.format("Lock state if '%s' and update to '%s'", this.lockVehicleStatusUpdateDoFn.checkStatus,
                        this.lockVehicleStatusUpdateDoFn.lockStatus),
                ParDo.of(this.lockVehicleStatusUpdateDoFn).withOutputTags(this.successTag,
                        TupleTagList.of(this.failTag)));
        stateChangeCollection.get(this.failTag).apply("update user job to failed", ParDo.of(this.updateUserJobState));
        return stateChangeCollection.get(this.successTag);
    }
}

class LockVehicleStatusUpdateDoFn<T extends VehicleMessageWithId> extends CassandraDoFnBase<T, T> {
    private static final long serialVersionUID = 1L;
    private static final String QUERY = "UPDATE vehicle_rider.vehicles SET status=?, in_transition=true WHERE id=? IF in_transition=false and status=?";

    private TupleTag<T> successTag;
    private TupleTag<T> failTag;

    public Object lockStatus;
    public Object checkStatus;

    public LockVehicleStatusUpdateDoFn(Object checkStatus, Object lockStatus, TupleTag<T> successTag,
            TupleTag<T> failTag, Config config, Logger log) {
        super(config.cassandraSeed(), QUERY, log);
        this.lockStatus = lockStatus;
        this.checkStatus = checkStatus;
        this.successTag = successTag;
        this.failTag = failTag;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        try {
            T message = context.element();
            UUID vehicleUUID = message.getVehicleId();
            BoundStatement bound = _prepared.bind(this.lockStatus, vehicleUUID, this.checkStatus);
            ResultSet results = this.executeBoundStatement(bound);
            Row row = results.one();
            if (row != null && row.getBool(0)) {
                context.output(this.successTag, message);
            } else {
                this._log.info("Failed to acquire lock");
                context.output(this.failTag, message);
            }
        } catch (Exception ex) {
            _log.error(String.format("error while trying to update vehicle status to %s", lockStatus.toString()), ex);
        }
    }
}
