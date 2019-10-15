package org.dav.vehicle_rider;

import java.util.UUID;

import com.datastax.driver.core.BoundStatement;

import org.dav.vehicle_rider.messages.JobMessage;
import org.slf4j.Logger;

public class UpdateUserJobState<T extends JobMessage> extends CassandraDoFnBase<T, T> {

    private static final long serialVersionUID = 1L;

    private static final String QUERY = "UPDATE vehicle_rider.user_jobs USING TTL 300 SET state=? WHERE id=?";
    private UserJobState _jobState;

    public UpdateUserJobState(UserJobState jobState, String cassandraHost, Logger log) {
        super(cassandraHost, QUERY, log);
        this._jobState = jobState;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        T message = context.element();
        UUID jobId = message.getJobId();
        try {
            BoundStatement bound = _prepared.bind(this._jobState.toString(), jobId);
            this.executeBoundStatement(bound);
            context.output(message);
        } catch (Exception ex) {
            _log.error(String.format("error while trying to update job: %s status to %s", jobId, this._jobState),
                    ex);
        }
    }
}