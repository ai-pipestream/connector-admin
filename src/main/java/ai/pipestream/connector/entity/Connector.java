package ai.pipestream.connector.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.OffsetDateTime;

/**
 * Connector entity representing a connector type/template.
 * <p>
 * Connectors are pre-seeded type definitions (e.g., "s3", "file-crawler") that
 * are reusable across accounts. Each account creates a {@link DataSource} binding
 * to use a connector type.
 * <p>
 * Database Table: {@code connectors}
 * <p>
 * Note: API keys are NOT stored on Connector. Each DataSource has its own API key.
 */
@Entity
@Table(name = "connectors")
public class Connector extends PanacheEntityBase {

    /**
     * Unique connector type identifier (primary key).
     * Deterministic: hash(connector_type)
     */
    @Id
    @Column(name = "connector_id", unique = true, nullable = false)
    public String connectorId;

    /**
     * Connector type name (e.g., "s3", "file-crawler", "confluence").
     * Must be unique across all connector types.
     */
    @Column(name = "connector_type", unique = true, nullable = false)
    public String connectorType;

    /**
     * Human-readable display name.
     */
    @Column(name = "name", nullable = false)
    public String name;

    /**
     * Description of the connector type.
     */
    @Column(name = "description", columnDefinition = "TEXT")
    public String description;

    /**
     * Management type: UNMANAGED or MANAGED.
     * UNMANAGED = external systems push via API key, no connector app.
     * MANAGED = platform-managed with health checks, scheduling, etc.
     */
    @Column(name = "management_type", nullable = false)
    public String managementType = "UNMANAGED";

    /**
     * Timestamp when the connector type was created.
     */
    @Column(name = "created_at", columnDefinition = "TIMESTAMP WITH TIME ZONE")
    public OffsetDateTime createdAt;

    /**
     * Timestamp when the connector type was last updated.
     */
    @Column(name = "updated_at", columnDefinition = "TIMESTAMP WITH TIME ZONE")
    public OffsetDateTime updatedAt;

    /**
     * Reference to connector configuration schema.
     * Used for JSON Schema validation of custom configuration.
     */
    @Column(name = "custom_config_schema_id")
    public String customConfigSchemaId;

    /**
     * Default value for persist_pipedoc configuration (Tier 1).
     * Applies to all DataSources using this connector unless overridden.
     */
    @Column(name = "default_persist_pipedoc")
    public Boolean defaultPersistPipedoc = true;

    /**
     * Default maximum inline size in bytes (Tier 1).
     * Documents larger than this will be persisted to S3.
     */
    @Column(name = "default_max_inline_size_bytes")
    public Integer defaultMaxInlineSizeBytes = 1048576; // 1MB default

    /**
     * Default custom configuration values (JSON Schema-validated).
     * Default values for connector-specific configuration fields.
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "default_custom_config", columnDefinition = "jsonb")
    public String defaultCustomConfig;

    /**
     * Display name for UI/admin interfaces.
     */
    @Column(name = "display_name")
    public String displayName;

    /**
     * Owner of the connector type.
     */
    @Column(name = "owner")
    public String owner;

    /**
     * Documentation URL for the connector type.
     */
    @Column(name = "documentation_url", columnDefinition = "TEXT")
    public String documentationUrl;

    /**
     * Tags for categorizing connector types.
     */
    @Column(name = "tags", columnDefinition = "TEXT[]")
    public String[] tags;

    /**
     * Many-to-one relationship to ConnectorConfigSchema entity.
     */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "custom_config_schema_id", insertable = false, updatable = false)
    public ConnectorConfigSchema configSchema;

    /**
     * Default constructor for JPA.
     */
    public Connector() {}

    /**
     * Create a new connector type.
     *
     * @param connectorId Unique identifier (deterministic hash of connectorType)
     * @param connectorType Type name (e.g., "s3", "file-crawler")
     * @param name Human-readable display name
     * @param description Description of the connector type
     * @param managementType Whether "MANAGED" or "UNMANAGED"
     */
    public Connector(String connectorId, String connectorType, String name,
                     String description, String managementType) {
        this.connectorId = connectorId;
        this.connectorType = connectorType;
        this.name = name;
        this.description = description;
        this.managementType = managementType;
        this.createdAt = OffsetDateTime.now();
        this.updatedAt = OffsetDateTime.now();
    }
}
