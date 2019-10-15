package org.dav.vehicle_rider;

import com.datastax.driver.core.BoundStatement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.dav.config.Config;
import org.dav.vehicle_rider.cassandra.VehicleLocationSearchResults;
import org.slf4j.Logger;

public class SearchResultsUpdate extends PTransform<PCollection<VehicleLocationSearchResults>, PCollection<VehicleLocationSearchResults>> {
    private static final long serialVersionUID = 1L;
    private SearchResultsUpdateDoFn searchResultsUpdateDoFn;

    public SearchResultsUpdate(Config config, Logger log) {
        this.searchResultsUpdateDoFn =
                new SearchResultsUpdateDoFn(config, log);
    }

    @Override
    public PCollection<VehicleLocationSearchResults> expand(PCollection<VehicleLocationSearchResults> input) {
        return input.apply("expand Ride VehicleLocationSearchResults", ParDo.of(this.searchResultsUpdateDoFn));
    }
}

class SearchResultsUpdateDoFn extends CassandraDoFnBase<VehicleLocationSearchResults, VehicleLocationSearchResults> {
    private static final long serialVersionUID = 1L;
    private static final String QUERY =
            "INSERT INTO vehicle_rider.vehicle_location_search_results (" +
                    "  geo_hash_prefix, " +
                    "  search_prefix_length, " +
                    "  search_prefix, " +
                    "  search_results " +
                    ") " +
                    "VALUES (?,?,?,?) USING TTL 10";

    public SearchResultsUpdateDoFn(Config config, Logger log) {
        super(config.cassandraSeed(), QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        try {
            VehicleLocationSearchResults vehicleLocationSearchResults = context.element();
            String geoHashPrefix = vehicleLocationSearchResults.geoHashPrefix;
            byte searchPrefixLength = vehicleLocationSearchResults.searchPrefixLength;
            String searchPrefix = vehicleLocationSearchResults.searchPrefix;
            String searchResults = vehicleLocationSearchResults.searchResults;
            BoundStatement bound = _prepared.bind(geoHashPrefix, searchPrefixLength, searchPrefix, searchResults);
            this.executeBoundStatement(bound);
            context.output(vehicleLocationSearchResults);
        } catch (Exception ex) {
            _log.error("error while trying to update VehicleLocationSearchResults",  ex);
        }
    }
}
