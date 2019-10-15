package org.dav.vehicle_rider.cassandra;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.reflect.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;

@Table(name = "riders", keyspace = "vehicle_rider")
public class Rider implements Serializable {
    
    private static final long serialVersionUID = 1L;

    @PartitionKey
    @Column (name = "id")
    public UUID id;

    @Nullable
    @Column (name = "email")
    public String email;

    @Column (name = "phone_number")
    public String phoneNumber;

    @Column (name = "phone_confirmed")
    public boolean phoneConfirmed;

    @Nullable
    @Column (name = "first_name")
    public String firstName;

    @Nullable
    @Column (name = "last_name")
    public String lastName;

    @Nullable
    @Column (name = "profile_image_id")
    public String profileImageId;

    @Nullable
    @Column (name = "eth_addr")
    public String ethAddr;

    @Nullable
    @Column (name = "private_key")
    public ByteBuffer privateKey;

    @Nullable
    @Column (name = "payment_method_customer")
    public String paymentMethodCustomer;

    @Nullable
    @Column (name = "payment_method_id")
    public String paymentMethodId;

    @Nullable
    @Column (name = "dav_balance")
    public BigDecimal davBalance;

    public Rider() {

    }

    public TableRow serializeToTableRow() {
        TableRow ownerPaymentRow = new TableRow();
        ownerPaymentRow.set("id", this.id);
        ownerPaymentRow.set("email", this.email);
        ownerPaymentRow.set("phone_number", this.phoneNumber);
        ownerPaymentRow.set("phone_confirmed", this.phoneConfirmed);
        ownerPaymentRow.set("first_name", this.firstName);
        ownerPaymentRow.set("last_name", this.lastName);
        ownerPaymentRow.set("profile_image_id", this.profileImageId);
        ownerPaymentRow.set("eth_addr", this.ethAddr);
        ownerPaymentRow.set("private_key", this.privateKey);
        // for now we don't use this field, so it was better not to create it in big query
        // ownerPaymentRow.set("payment_method_customer", this.paymentMethodCustomer);
        ownerPaymentRow.set("payment_method_id", this.paymentMethodId);
        return ownerPaymentRow;
    }
}