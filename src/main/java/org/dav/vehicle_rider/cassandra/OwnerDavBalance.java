
package org.dav.vehicle_rider.cassandra;

import java.io.Serializable;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.api.services.bigquery.model.TableRow;

import org.apache.avro.reflect.Nullable;

@Table(name = "owner_dav_balance", keyspace = "vehicle_rider")
public class OwnerDavBalance implements Serializable {

    private static final long serialVersionUID = 1L;

    @PartitionKey
    @Column(name = "id")
    public UUID id;

    @Column(name = "dav_balance_delta")
    @Nullable
    public long davBalanceDelta;

    public OwnerDavBalance() {

    }

    public TableRow serializeToTableRow() {
        TableRow ownerRow = new TableRow();
        ownerRow.set("id", this.id);
        ownerRow.set("dav_balance_delta", this.davBalanceDelta);
        return ownerRow;
    }
}