package ai.pipestream.connector.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import java.io.Serializable;
import java.util.Objects;

/**
 * Junction entity establishing a many-to-many relationship between Connectors and Accounts.
 * <p>
 * This entity enables flexible connector-account associations where a single connector
 * can serve multiple accounts (shared connector model), and an account can utilize
 * multiple connectors for different data sources.
 *
 * <h2>Architecture Pattern</h2>
 * Implements the classic junction table pattern for many-to-many relationships:
 * <pre>
 * Account 1 --&gt; ConnectorAccount 1 --&gt; Connector A
 * Account 1 --&gt; ConnectorAccount 2 --&gt; Connector B
 * Account 2 --&gt; ConnectorAccount 3 --&gt; Connector A  (shared)
 * </pre>
 *
 * <h2>Database Schema</h2>
 * <pre>
 * Table: connector_accounts
 * Composite Primary Key: (connector_id, account_id)
 * Foreign Keys:
 *   - connector_id -&gt; connectors.connector_id (ON DELETE CASCADE)
 *   - account_id: No FK constraint (accounts managed by separate service)
 * </pre>
 *
 * <h2>Service Decoupling</h2>
 * Notably, this table does NOT define a foreign key to the accounts table because
 * accounts are managed by a separate microservice (account-manager). Account existence
 * is validated via gRPC before creating links, maintaining service independence.
 *
 * <h2>Query Patterns</h2>
 * <ul>
 *   <li><b>Find connectors for account</b>: {@code SELECT ca WHERE ca.accountId = ?}</li>
 *   <li><b>Find accounts for connector</b>: {@code SELECT ca WHERE ca.connectorId = ?}</li>
 *   <li><b>Check if linked</b>: {@code COUNT WHERE ca.connectorId = ? AND ca.accountId = ?}</li>
 * </ul>
 *
 * <h2>Example Usage</h2>
 * <pre>
 * // Link connector to account
 * ConnectorAccount link = new ConnectorAccount(connectorId, accountId);
 * link.persist();
 *
 * // Find all connectors for an account with eager-loaded connector details
 * List&lt;ConnectorAccount&gt; links = ConnectorAccount.find(
 *     "accountId = ?1", accountId
 * ).list();
 * </pre>
 *
 * @see ai.pipestream.connector.entity.Connector
 * @see ai.pipestream.connector.repository.ConnectorRepository#linkConnectorToAccount(String, String)
 * @see ai.pipestream.connector.repository.ConnectorRepository#unlinkConnectorFromAccount(String, String)
 * @since 1.0.0
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
