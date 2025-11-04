package io.pipeline.connector.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import java.io.Serializable;
import java.util.Objects;

/**
 * Junction entity for many-to-many relationship between Connectors and Accounts.
 * <p>
 * A connector can be linked to multiple accounts, and an account can have
 * multiple connectors. This enables cross-account connector sharing.
 * <p>
 * Database Table: {@code connector_accounts}
 */
@Entity
@Table(name = "connector_accounts")
@IdClass(ConnectorAccount.ConnectorAccountId.class)
public class ConnectorAccount extends PanacheEntityBase {

    /**
     * Connector ID (composite key part 1).
     */
    @Id
    @Column(name = "connector_id", nullable = false)
    public String connectorId;

    /**
     * Account ID (composite key part 2).
     */
    @Id
    @Column(name = "account_id", nullable = false)
    public String accountId;

    /**
     * Many-to-one relationship to Connector entity.
     * Fetched eagerly for efficient access to connector details.
     */
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "connector_id", insertable = false, updatable = false)
    public Connector connector;

    /**
     * Default constructor for JPA.
     */
    public ConnectorAccount() {}

    /**
     * Create a connector-account link.
     *
     * @param connectorId Connector identifier
     * @param accountId Account identifier
     */
    public ConnectorAccount(String connectorId, String accountId) {
        this.connectorId = connectorId;
        this.accountId = accountId;
    }

    /**
     * Composite primary key class for ConnectorAccount.
     */
    public static class ConnectorAccountId implements Serializable {
        public String connectorId;
        public String accountId;

        public ConnectorAccountId() {}

        public ConnectorAccountId(String connectorId, String accountId) {
            this.connectorId = connectorId;
            this.accountId = accountId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConnectorAccountId that = (ConnectorAccountId) o;
            return Objects.equals(connectorId, that.connectorId) &&
                   Objects.equals(accountId, that.accountId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorId, accountId);
        }
    }
}
