package org.dav.vehicle_rider;

import com.github.davidmoten.geo.GeoHash;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.dav.config.Config;
import org.dav.vehicle_rider.cassandra.VehicleLocationSearchResults;
import org.dav.vehicle_rider.cassandra_helpers.GetAllAvailableVehicles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

public class SearchVehiclesFlow {
    private static final Logger LOG = LoggerFactory.getLogger(SearchVehiclesFlow.class);

    public static void main(String[] args) {
        Config config = Config.create(true, args);
        Pipeline p = Pipeline.create(config.pipelineOptions());
        p

                .apply("Read Kafka", KafkaIO.<Long, String>read()
                        .withBootstrapServers(config.kafkaSeed() + ":" + config.kafkaPort())
                        .withTopic("search-vehicles").withKeyDeserializer(LongDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .updateConsumerProperties(ImmutableMap.of("group.id", "my_beam_app_1"))
                        .withReadCommitted().commitOffsetsInFinalize().withoutMetadata())

                .apply("Take Values", Values.<String>create())

                .apply("Parse JSON", MapElements
                        .into(TypeDescriptor.of(SearchVehiclesMessage.class)).via(message -> {
                            try {
                                final GsonBuilder builder = new GsonBuilder();
                                builder.excludeFieldsWithoutExposeAnnotation();
                                final Gson gson = builder.create();
                                return gson.fromJson(message, SearchVehiclesMessage.class);
                            } catch (Exception e) {
                                LOG.error("Parse JSON", e);
                                return null;
                            }
                        }))

                .apply("Filter NULLs", Filter.by(obj -> obj != null))

                .apply(ParDo.of(new GetAllAvailableVehicles(config.cassandraSeed(), LOG)))

                .apply("Get Vehicles for search",
                        MapElements.into(TypeDescriptor.of(VehicleLocationSearchResults.class))
                                .via(vehicleList -> {
                                    try {
                                        return new VehicleLocationSearchResults(vehicleList);
                                    } catch (Exception e) {
                                        LOG.error(
                                                "Failed to create vehicle locations search results object",
                                                e);
                                        return null;
                                    }
                                }))
                .apply("Filter NULLs", Filter.by(obj -> obj != null))

                .apply(new SearchResultsUpdate(config, LOG));

        p.run();
    }

    public static class SearchVehiclesMessage implements Serializable{
        private static final long serialVersionUID = 1L;

        @SerializedName("locationHash")
        @Expose(deserialize=true)
        private String _geoHash;

        @SerializedName("searchPrefixLength")
        @Expose(deserialize=true)
        private short _searchPrefixLength;

        public List<String> getGeoHashes() {
            String searchPrefix = getSearchPrefix();
            List<String> geoHashes = GeoHash.neighbours(searchPrefix);
            geoHashes.add(searchPrefix);
            return geoHashes;
        }

        public String getSearchPrefix() {
            return _geoHash.substring(0, _searchPrefixLength);
        }

        public short getSearchPrefixLength() {
            return _searchPrefixLength;
        }

        public String getCashPrefix() {
            return _geoHash;
        }

    }
}
