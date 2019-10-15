package org.dav.vehicle_rider;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.datastax.driver.core.querybuilder.Update.Where;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraUpdate<I> extends PTransform<PCollection<I>, PDone> {
    private static final long serialVersionUID = 1L;
    private CassandraQueryDoFn<I> cassandraQueryDoFn;
    private String table;

    public CassandraUpdate(String keyspace, String table, ParamsFunc<I> paramsFunc, String cassandraHost,
            ConsistencyLevel consistencyLevel) {
        this.table = table;
        cassandraQueryDoFn =
                new CassandraQueryDoFn<>(keyspace, table, paramsFunc, cassandraHost, consistencyLevel);
    }

    @FunctionalInterface
    public static interface ParamsFunc<I> extends Serializable {
        Map<String, Object> get(I input);
    }

    @Override
    public PDone expand(PCollection<I> input) {
        input.apply("Update Cassandra: " + this.table, ParDo.of(this.cassandraQueryDoFn));
        return PDone.in(input.getPipeline());
    }

    private static class CassandraQueryDoFn<I> extends DoFn<I, Void> {
        private static final Logger LOG = LoggerFactory.getLogger(VehicleUpdatesFlow.class);
        private static final long serialVersionUID = 1L;

        private static final long DELAY_BETWEEN_RECONNERCTS = 10000;

        private String hosts;
        private Cluster cluster;
        private ConsistencyLevel consistencyLevel;

        private Session session;
        private String keyspace;
        private String table;
        private ParamsFunc<I> paramsFunc;

        protected CassandraQueryDoFn(String keyspace, String table, ParamsFunc<I> paramsFunc, String cassandraHost,
                ConsistencyLevel consistencyLevel) {
            this.keyspace = keyspace;
            this.table = table;
            this.paramsFunc = paramsFunc;
            this.hosts = cassandraHost;
            this.consistencyLevel = consistencyLevel;
        }

        protected ResultSet executeBoundStatement(Statement statement) throws InterruptedException {
            try {
                return this.session.execute(statement);
            } catch (NoHostAvailableException ex) {
                LOG.error("Disconnected from cassandra, restarting connection...", ex);
                setup();
                Thread.sleep(DELAY_BETWEEN_RECONNERCTS);
                return this.executeBoundStatement(statement);
            }
        }

        @Setup
        public void setup() {
            try {
                cluster = Cluster.builder().withLoadBalancingPolicy(new RoundRobinPolicy())
                        .addContactPoint(hosts)
                        .withQueryOptions(new QueryOptions().setConsistencyLevel(consistencyLevel))
                        .build();
                session = cluster.connect();
            } catch (Exception ex) {
                LOG.error("Error while trying to initialize cassandra client", ex);
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
        }

        @ProcessElement
        public void processElement(@Element I input) {
            try {
                Map<String, Object> params = this.paramsFunc.get(input);

                Where where =
                        QueryBuilder.update(keyspace, table).where(QueryBuilder.eq("id", params.get("id")));
                Assignments assignments = null;
                for (Entry<String, Object> pair : params.entrySet()) {
                    if (pair.getKey().equals("id")) {
                        continue;
                    }
                    if (assignments == null) {
                        assignments = where.with(QueryBuilder.set(pair.getKey(), pair.getValue()));
                    } else {
                        assignments.and(QueryBuilder.set(pair.getKey(), pair.getValue()));
                    }
                }
                Statement statement = assignments.setConsistencyLevel(consistencyLevel);
                this.executeBoundStatement(statement.setConsistencyLevel(consistencyLevel));
            } catch (Exception ex) {
                LOG.error(String.format("Error while trying to update cassandra table %s", table),
                        ex);
            }
        }
    }
}
