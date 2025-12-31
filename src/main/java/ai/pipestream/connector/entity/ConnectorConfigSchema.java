package ai.pipestream.connector.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.OffsetDateTime;

/**
 * Connector configuration schema entity.
 * <p>
 * Stores JSON Schema definitions for connector-specific custom configuration.
 * This enables JSON Forms UI generation and validation of custom config fields.
 * Strongly typed configuration fields (persistence, retention, encryption, hydration)
 * are defined in protobuf and NOT stored here - only custom/connector-specific config uses JSON Schema.
 * <p>
 * Database Table: {@code connector_config_schemas}
 */
@Entity
@Table(name = "connector_config_schemas", uniqueConstraints = {
    @UniqueConstraint(columnNames = {"connector_id", "schema_version"})
})
public class ConnectorConfigSchema extends PanacheEntityBase {

    /**
     * Unique schema identifier (primary key).
     */
    @Id
    @Column(name = "schema_id", unique = true, nullable = false)
    public String schemaId;

    /**
     * Connector this schema belongs to.
     * References connectors.connector_id
     */
    @Column(name = "connector_id", nullable = false)
    public String connectorId;

    /**
     * Schema version identifier.
     * Allows versioning of schemas for backward compatibility.
     */
    @Column(name = "schema_version", nullable = false)
    public String schemaVersion;

    /**
     * JSON Schema for Tier 1 custom configuration.
     * Validates connector-specific configuration at the datasource level.
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "custom_config_schema", columnDefinition = "jsonb", nullable = false)
    public String customConfigSchema;

    /**
     * JSON Schema for Tier 2 node custom configuration.
     * Validates connector-specific configuration at the node/graph level.
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "node_custom_config_schema", columnDefinition = "jsonb", nullable = false)
    public String nodeCustomConfigSchema;

    /**
     * Timestamp when the schema was created.
     */
    @Column(name = "created_at", columnDefinition = "TIMESTAMP WITH TIME ZONE", nullable = false)
    public OffsetDateTime createdAt;

    /**
     * User who created this schema.
     */
    @Column(name = "created_by")
    public String createdBy;

    /**
     * Apicurio Registry artifact ID.
     * Used for schema storage and versioning in Apicurio.
     */
    @Column(name = "apicurio_artifact_id")
    public String apicurioArtifactId;

    /**
     * Apicurio Registry global ID.
     * Used for schema lookups in Apicurio.
     */
    @Column(name = "apicurio_global_id")
    public Long apicurioGlobalId;

    /**
     * Sync status with Apicurio Registry.
     * Values: PENDING, SYNCED, FAILED
     */
    @Column(name = "sync_status", nullable = false)
    public String syncStatus = "PENDING";

    /**
     * Timestamp of last sync attempt with Apicurio.
     */
    @Column(name = "last_sync_attempt", columnDefinition = "TIMESTAMP WITH TIME ZONE")
    public OffsetDateTime lastSyncAttempt;

    /**
     * Error message from last sync attempt (if failed).
     */
    @Column(name = "sync_error")
    public String syncError;

    /**
     * Many-to-one relationship to Connector entity.
     */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "connector_id", insertable = false, updatable = false)
    public Connector connector;

    /**
     * Default constructor for JPA.
     */
    public ConnectorConfigSchema() {}

    /**
     * Create a new connector config schema.
     *
     * @param schemaId Unique schema identifier
     * @param connectorId Connector this schema belongs to
     * @param schemaVersion Schema version
     * @param customConfigSchema JSON Schema for Tier 1 custom config
     * @param nodeCustomConfigSchema JSON Schema for Tier 2 node custom config
     * @param createdBy User who created this schema
     */
    public ConnectorConfigSchema(String schemaId, String connectorId, String schemaVersion,
                                 String customConfigSchema, String nodeCustomConfigSchema,
                                 String createdBy) {
        this.schemaId = schemaId;
        this.connectorId = connectorId;
        this.schemaVersion = schemaVersion;
        this.customConfigSchema = customConfigSchema;
        this.nodeCustomConfigSchema = nodeCustomConfigSchema;
        this.createdBy = createdBy;
        this.syncStatus = "PENDING";
        this.createdAt = OffsetDateTime.now();
    }
}

