package io.pipeline.connector.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import java.time.OffsetDateTime;

/**
 * Connector entity representing external data sources.
 * <p>
 * Connectors authenticate via API keys to ingest documents into the platform.
 * Each connector can be linked to multiple accounts via the connector_accounts
 * junction table.
 * <p>
 * Database Table: {@code connectors}
 * <p>
 * API keys are stored hashed for security. The plaintext key is only returned
 * once during registration or rotation.
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
