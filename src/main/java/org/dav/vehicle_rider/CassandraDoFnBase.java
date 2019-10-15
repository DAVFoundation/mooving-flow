package org.dav.vehicle_rider;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;

public abstract class CassandraDoFnBase<T, U> extends DoFn<T, U> {

    private static final long serialVersionUID = 1L;
    private static final long DEALY_BETWEEN_RECONNERCTS = 10000;

    private String _cassandraHost;
    private Cluster _cluster;
    private String _cassandraQuery;
    private ConsistencyLevel _consistencyLevel;

    protected final Logger _log;
    protected Session _session;
    protected PreparedStatement _prepared;

    protected CassandraDoFnBase(String cassandraHost, String cassandraQuery, Logger log) {
        _cassandraHost = cassandraHost;
        _cassandraQuery = cassandraQuery;
        _log = log;
        _consistencyLevel = ConsistencyLevel.ONE;
    }

    protected CassandraDoFnBase(String cassandraHost, String cassandraQuery, Logger log, ConsistencyLevel consistencyLevel) {
        this(cassandraHost, cassandraQuery, log);
        _consistencyLevel = consistencyLevel;
    }

    protected ResultSet executeBoundStatement(BoundStatement boundStatement) throws InterruptedException {
        try {
            // _log.info("try to query cassandra");
            return this._session.execute(boundStatement);
        } catch(NoHostAvailableException ex) {
            this._log.error("disconnected from cassandra, restarting connection...", ex);
            setup();
            Thread.sleep(DEALY_BETWEEN_RECONNERCTS);
            return this.executeBoundStatement(boundStatement);
        }
    }

    @Setup
    public void setup() {
        try {
            _cluster = Cluster.builder().withLoadBalancingPolicy(new RoundRobinPolicy())
                    .addContactPoint(_cassandraHost)
                    .withQueryOptions(new QueryOptions().setConsistencyLevel(_consistencyLevel))
                    .build();
            _session = _cluster.connect();
            _prepared = _session.prepare(_cassandraQuery);
        } catch (Exception ex) {
            _log.error("error while trying to initialize cassandra client", ex);
        }
    }

    @Teardown
    public void teardown() throws Exception {
        if (_session != null) {
            _session.close();
        }
        if (_cluster != null) {
            _cluster.close();
        }
    }

}
