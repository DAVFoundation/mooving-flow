package org.dav.vehicle_rider.cassandra;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.api.services.bigquery.model.TableRow;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

@Table(name = "dav_conversion_rate", keyspace = "vehicle_rider")
public class DavConversionRate implements Serializable {
    
    private static final long serialVersionUID = 1L;

    @PartitionKey
    @Column(name = "currency_code")
    public String currencyCode;

    @Column(name = "updated_timestamp")
    public Date updateTimestamp;

    @Column(name= "price")
    public BigDecimal price;

    public DavConversionRate(String currencyCode, BigDecimal price) {
        this.currencyCode = currencyCode;
        this.price = price;
        this.updateTimestamp = new Date();
    }

    public TableRow serializeToTableRow() {
        TableRow conversionRateRow = new TableRow();
        conversionRateRow.set("currency_code", this.currencyCode);
        conversionRateRow.set("updated_timestamp", this.updateTimestamp);
        conversionRateRow.set("price", this.price);
        return conversionRateRow;
    }
}