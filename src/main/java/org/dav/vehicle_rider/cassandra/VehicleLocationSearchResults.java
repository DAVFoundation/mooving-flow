package org.dav.vehicle_rider.cassandra;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.api.services.bigquery.model.TableRow;
import org.dav.vehicle_rider.VehicleList;
import java.io.Serializable;

@Table(name = "vehicle_location_search_results", keyspace = "vehicle_rider")
public class VehicleLocationSearchResults implements Serializable {

    private static final long serialVersionUID = 1L;
    @Column(name = "geo_hash_prefix")
    public String geoHashPrefix;

    @PartitionKey
    @Column(name = "search_prefix_length")
    public byte searchPrefixLength;

    @Column(name = "search_prefix")
    public String searchPrefix;

    @Column(name = "search_results")
    public String searchResults;

    public VehicleLocationSearchResults() {

    }

    public VehicleLocationSearchResults(VehicleList vehicleList) {
        this.geoHashPrefix = vehicleList.getCashPrefix();
        this.searchPrefixLength = vehicleList.getSearchPrefixLength();
        this.searchPrefix = this.geoHashPrefix.substring(0, this.searchPrefixLength);
        this.searchResults = vehicleList.getVehicles();
    }

    public TableRow serializeToTableRow() {
        TableRow searchResultsTableRow = new TableRow();
        searchResultsTableRow.set("geo_hash_prefix", this.geoHashPrefix);
        searchResultsTableRow.set("search_prefix_length", this.searchPrefixLength);
        searchResultsTableRow.set("search_prefix", this.searchPrefix);
        searchResultsTableRow.set("search_results", this.searchResults);
        return searchResultsTableRow;
    }
}


