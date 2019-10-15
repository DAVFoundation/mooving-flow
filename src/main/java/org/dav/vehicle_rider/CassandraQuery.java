package org.dav.vehicle_rider;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class CassandraQuery<I, O  extends Serializable> extends PTransform<PCollection<I>, PCollection<O>> {
    private static final long serialVersionUID = 1L;
    private CassandraQueryDoFn<I, O> cassandraQueryDoFn;
    private String query;
    private Class<O> klass;

    public CassandraQuery(String query, ParamsFunc<I> paramsFunc, Class<O> klass,
            MapResult<I, O> mapResult, String cassandraHost, ConsistencyLevel consistencyLevel) {
        this.query = query;
        this.klass = klass;
        cassandraQueryDoFn = new CassandraQueryDoFn<>(query, paramsFunc, klass, mapResult,
                cassandraHost, consistencyLevel);
    }

    @FunctionalInterface
    public static interface ParamsFunc<I> extends Serializable {
        Object[] get(I input);
    }

    @FunctionalInterface
    public static interface MapResult<I, O> extends Serializable {
        O map(I input, O result);
    }

    @Override
    public PCollection<O> expand(PCollection<I> input) {
        return input.apply("Query Cassandra: " + this.query, ParDo.of(this.cassandraQueryDoFn))
                .setCoder(SerializableCoder.of(klass));
    }

    private static class CassandraQueryDoFn<I, O> extends DoFn<I, O> {
        private static final Logger LOG = LoggerFactory.getLogger(VehicleUpdatesFlow.class);
        private static final long serialVersionUID = 1L;

        private static final long DELAY_BETWEEN_RECONNERCTS = 10000;

        private String hosts;
        private Cluster cluster;
        private ConsistencyLevel consistencyLevel;

        private Session session;
        private PreparedStatement prepared;
        private String query;
        private ParamsFunc<I> paramsFunc;
        private Class<O> klass;
        private MapResult<I, O> mapResult;

        protected CassandraQueryDoFn(String query, ParamsFunc<I> paramsFunc, Class<O> klass,
                MapResult<I, O> mapResult, String cassandraHost,
                ConsistencyLevel consistencyLevel) {
            this.query = query;
            this.paramsFunc = paramsFunc;
            this.klass = klass;
            this.mapResult = mapResult;
            this.hosts = cassandraHost;
            this.consistencyLevel = consistencyLevel;
        }

        protected ResultSet executeBoundStatement(BoundStatement boundStatement)
                throws InterruptedException {
            try {
                return this.session.execute(boundStatement);
            } catch (NoHostAvailableException ex) {
                LOG.error("Disconnected from cassandra, restarting connection...", ex);
                setup();
                Thread.sleep(DELAY_BETWEEN_RECONNERCTS);
                return this.executeBoundStatement(boundStatement);
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
                prepared = session.prepare(query);
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
        public void processElement(@Element I input, OutputReceiver<O> output) {
            try {
                Object[] params = this.paramsFunc.get(input);
                BoundStatement bound = prepared.bind(params);
                ResultSet resultSet = this.executeBoundStatement(bound);

                MappingManager mappingManager = new MappingManager(session);
                Mapper<O> mapper = mappingManager.mapper(klass);
                if (resultSet.iterator().hasNext()) {
                    Result<O> results = mapper.map(resultSet);
                    for (O result : results) {
                        try {
                            output.output(this.mapResult.map(input, result));
                        } catch (Exception e) {
                            LOG.error(String.format("Error mapping result: %s, %s", query,
                                    result.toString()), e);
                        }
                    }
                }
            } catch (Exception ex) {
                LOG.error(String.format("Error while trying to query cassandra: %s", query), ex);
            }
        }
    }
}
