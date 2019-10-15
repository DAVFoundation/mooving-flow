
package org.dav.vehicle_rider.cassandra;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.api.services.bigquery.model.TableRow;

import org.apache.avro.reflect.Nullable;

@Table(name = "owners", keyspace = "vehicle_rider")
public class Owner implements Serializable {

    private static final long serialVersionUID = 1L;

    @PartitionKey
    @Column(name = "id")
    public UUID id;

    @Column(name = "email")
    @Nullable
    public String email;

    @Column(name = "phone_number")
    @Nullable
    public String phoneNumber;

    @Column(name = "phone_confirmed")
    public boolean phoneConfirmed;

    @Column(name = "first_name")
    @Nullable
    public String firstName;

    @Column(name = "last_name")
    @Nullable
    public String lastName;

    @Column(name = "profile_image_id")
    @Nullable
    public String profileImageId;

    @Column(name = "eth_addr")
    @Nullable
    public String ethAddr;

    @Column(name = "private_key")
    @Nullable
    public ByteBuffer privateKey;

    @Column(name = "dav_balance_base")
    public BigDecimal davBalanceBase;

    @Column(name = "payment_dav_factor")
    public BigDecimal paymentDavFactor;

    @Column(name = "fiat_currency_code")
    public String fiatCurrencyCode;

    @Column(name = "pricing_using_fiat")
    public boolean pricingUsingFiat;

    @Column(name = "commission_factor")
    public BigDecimal commissionFactor;

    @Column(name = "vendor_id")
    public String vendor;

    @Column(name = "company_name")
    public String companyName;

    public Owner() {

    }

    public Owner(UUID id, BigDecimal paymentDavFactor, String fiatCurrencyCode, boolean pricingUsingFiat,
            BigDecimal commissionFactor, String vendor) {
        this();
        this.id = id;
        this.paymentDavFactor = paymentDavFactor;
        this.fiatCurrencyCode = fiatCurrencyCode;
        this.pricingUsingFiat = pricingUsingFiat;
        this.commissionFactor = commissionFactor;
        this.vendor = vendor;
    }

    public TableRow serializeToTableRow() {
        TableRow ownerRow = new TableRow();
        ownerRow.set("id", this.id);
        ownerRow.set("email", this.email);
        ownerRow.set("phone_number", this.phoneNumber);
        ownerRow.set("phone_confirmed", this.phoneConfirmed);
        ownerRow.set("first_name", this.firstName);
        ownerRow.set("last_name", this.lastName);
        ownerRow.set("profile_image_id", this.profileImageId);
        ownerRow.set("eth_addr", this.ethAddr);
        ownerRow.set("private_key", this.privateKey);
        ownerRow.set("dav_balance_base", this.davBalanceBase);
        ownerRow.set("payment_dav_factor", this.paymentDavFactor);
        ownerRow.set("fiat_currency_code", this.fiatCurrencyCode);
        ownerRow.set("pricing_using_fiat", this.pricingUsingFiat);
        ownerRow.set("commission_factor", this.commissionFactor);
        ownerRow.set("vendor_id", this.vendor);
        ownerRow.set("company_name", this.companyName);
        return ownerRow;
    }
}