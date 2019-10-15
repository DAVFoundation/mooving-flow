package org.dav.config;

import com.google.auto.value.AutoValue;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class Config {
        private static final Logger LOG = LoggerFactory.getLogger(Config.class);

        public static Config create(boolean streaming, String[] args) {
                Builder builder = new AutoValue_Config.Builder();

                ParameterTool parameters = ParameterTool.fromArgs(args);

                FlinkPipelineOptions options =
                                PipelineOptionsFactory.create().as(FlinkPipelineOptions.class);
                options.setRunner(FlinkRunner.class);
                options.setStreaming(streaming);
                options.setJobName(parameters.get("jobName", "Unknown-Name").replaceAll("\\s","").replace("(", "_").replace(")", ""));
                options.setParallelism(1);
                builder.setPipelineOptions(options);

                builder.setCassandraSeed(parameters.get("CASSANDRA_SEEDS", ""));
                builder.setCassandraPort(parameters.getInt("CASSANDRA_PORT", 0));
                builder.setKafkaSeed(parameters.get("KAFKA_SEEDS", ""));
                builder.setKafkaPort(parameters.getInt("KAFKA_PORT", 0));

                return builder.build();
        }

        public abstract PipelineOptions pipelineOptions();

        public abstract String cassandraSeed();

        public abstract int cassandraPort();

        public abstract String kafkaSeed();

        public abstract int kafkaPort();

        @AutoValue.Builder
        abstract static class Builder {
                abstract Builder setPipelineOptions(PipelineOptions pipelineOptions);

                abstract Builder setCassandraSeed(String cassandraHosts);

                abstract Builder setCassandraPort(int cassandraPort);

                abstract Builder setKafkaSeed(String kafkaSeed);

                abstract Builder setKafkaPort(int kafkaPort);

                abstract Config build();
        }
}
