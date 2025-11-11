package ai.pipestream.connector.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import java.time.OffsetDateTime;

/**
 * Connector entity representing external data sources that ingest documents into the Pipestream platform.
 * <p>
 * A connector is a configured integration with an external system (e.g., filesystem, Confluence, SharePoint)
 * that crawls and ingests documents. Each connector authenticates using an API key and can be associated
 * with multiple accounts through a many-to-many relationship.
 *
 * <h2>Key Features</h2>
 * <ul>
 *   <li><b>API Key Authentication</b>: Secure authentication using Argon2id-hashed API keys</li>
 *   <li><b>Multi-Account Support</b>: Single connector can serve multiple accounts via junction table</li>
 *   <li><b>Soft Deletion</b>: Maintains audit trail through soft delete with reason tracking</li>
 *   <li><b>Status Management</b>: Active/inactive states with status reason for debugging</li>
 *   <li><b>Flexible Metadata</b>: JSON-based metadata for S3 configuration, limits, and defaults</li>
 * </ul>
 *
 * <h2>Database Schema</h2>
 * <pre>
 * Table: connectors
 * Primary Key: connector_id (UUID)
 * Unique Keys: connector_name
 * Indexes: active, connector_name, connector_type
 * </pre>
 *
 * <h2>Security Model</h2>
 * API keys are never stored in plaintext. The {@link #apiKeyHash} field contains an Argon2id hash
 * (64MB memory, 3 iterations, 4 threads) that is verified using constant-time comparison to prevent
 * timing attacks. The plaintext API key is only returned once during registration or rotation.
 *
 * <h2>Lifecycle States</h2>
 * <ul>
 *   <li><b>Active</b>: Connector can authenticate and ingest documents</li>
 *   <li><b>Inactive</b>: Authentication disabled, often due to account inactivation or manual disable</li>
 *   <li><b>Soft-Deleted</b>: Marked as deleted with reason and timestamp; can be recovered</li>
 * </ul>
 *
 * <h2>Example Usage</h2>
 * <pre>
 * // Creating a new connector (typically done via ConnectorRepository)
 * Connector connector = new Connector(
 *     UUID.randomUUID().toString(),
 *     "my-confluence-connector",
 *     "confluence",
 *     "Company wiki connector",
 *     apiKeyUtil.hashApiKey(apiKey)
 * );
 * connector.metadata = metadataJson;
 * connector.persist();
 * </pre>
 *
 * @see ai.pipestream.connector.entity.ConnectorAccount
 * @see ai.pipestream.connector.repository.ConnectorRepository
 * @see ai.pipestream.connector.util.ApiKeyUtil
 * @since 1.0.0
 */
@Entity
@Table(name = "connectors")
public class Connector extends PanacheEntityBase {

    /**
     * Unique connector identifier (primary key).
     * Generated as UUID.
     */
    @Id
    @Column(name = "connector_id", unique = true, nullable = false)
    public String connectorId;

    /**
     * Platform-unique connector name.
     * Used for identification and logging.
     */
    @Column(name = "connector_name", unique = true, nullable = false)
    public String connectorName;

    /**
     * Connector type identifier.
     * Examples: "filesystem", "confluence", "database", "api"
     */
    @Column(name = "connector_type", nullable = false)
    public String connectorType;

    /**
     * Optional description of the connector.
     */
    @Column(name = "description")
    public String description;

    /**
     * Hashed API key for authentication.
     * Never expose the plaintext key after initial generation.
     * Uses BCrypt or similar hashing algorithm.
     */
    @Column(name = "api_key_hash", nullable = false)
    public String apiKeyHash;

    /**
     * Whether the connector is active.
     * Inactive connectors cannot authenticate or ingest documents.
     * Default: true
     */
    @Column(name = "active", nullable = false)
    public Boolean active = true;

    /**
     * Status reason for connector state.
     * Examples: "account_inactive", "manual_disable", "api_key_rotated"
     * Null if connector is active without special status.
     */
    @Column(name = "status_reason")
    public String statusReason;

    /**
     * Timestamp when the connector was created.
     */
    @Column(name = "created_at")
    public OffsetDateTime createdAt;

    /**
     * Timestamp when the connector was last updated.
     */
    @Column(name = "updated_at")
    public OffsetDateTime updatedAt;

    /**
     * Timestamp when the API key was last rotated.
     * Null if key has never been rotated.
     */
    @Column(name = "last_rotated_at")
    public OffsetDateTime lastRotatedAt;

    /**
     * Additional connector metadata as JSON.
     * Stores S3 config, limits, default metadata, etc.
     */
    @Column(name = "metadata", columnDefinition = "JSON")
    public String metadata;

    /**
     * Reason for deletion (soft delete).
     * Null if not deleted.
     */
    @Column(name = "deleted_reason")
    public String deletedReason;

    /**
     * Timestamp when connector was deleted (soft delete).
     * Null if not deleted.
     */
    @Column(name = "deleted_at")
    public OffsetDateTime deletedAt;

    /**
     * Default constructor for JPA.
     */
    public Connector() {}

    /**
     * Create a new active connector with hashed API key.
     *
     * @param connectorId Unique identifier (UUID)
     * @param connectorName Platform-unique name
     * @param connectorType Type identifier
     * @param description Optional description
     * @param apiKeyHash Hashed API key
     */
    public Connector(String connectorId, String connectorName, String connectorType,
                     String description, String apiKeyHash) {
        this.connectorId = connectorId;
        this.connectorName = connectorName;
        this.connectorType = connectorType;
        this.description = description;
        this.apiKeyHash = apiKeyHash;
        this.active = true;
        this.createdAt = OffsetDateTime.now();
        this.updatedAt = OffsetDateTime.now();
        this.lastRotatedAt = null;
        this.metadata = "{}"; // Empty JSON object
    }
}
