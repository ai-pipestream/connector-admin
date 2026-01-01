package ai.pipestream.connector.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.OffsetDateTime;

/**
 * DataSource entity representing an account's binding to a connector type.
 * <p>
 * Each account can have one DataSource per connector type. The DataSource has
 * its own API key and configuration, allowing per-account customization.
 * Storage configuration is handled via Drive references (FilesystemService).
 * <p>
 * Database Table: {@code datasources}
 * <p>
 * Deterministic ID: datasource_id = hash(account_id + connector_id)
 */
@Entity
@Table(name = "datasources", uniqueConstraints = {
    @UniqueConstraint(columnNames = {"account_id", "connector_id"})
})
public class DataSource extends PanacheEntityBase {

    /**
     * Unique datasource identifier (primary key).
     * Deterministic: hash(account_id + connector_id)
     */
    @Id
    @Column(name = "datasource_id", unique = true, nullable = false)
    public String datasourceId;

    /**
     * Account that owns this datasource.
     */
    @Column(name = "account_id", nullable = false)
    public String accountId;

    /**
     * Connector type this datasource uses.
     * References connectors.connector_id
     */
    @Column(name = "connector_id", nullable = false)
    public String connectorId;

    /**
     * Human-readable datasource name.
     */
    @Column(name = "name", nullable = false)
    public String name;

    /**
     * Hashed API key for authentication.
     * Never expose the plaintext key after initial generation.
     * Uses Argon2id hashing via Password4j.
     */
    @Column(name = "api_key_hash", nullable = false)
    public String apiKeyHash;

    /**
     * Drive name for document storage.
     * References Drive entity in FilesystemService.
     * The Drive contains S3 bucket, KMS keys, and credentials via Infisical.
     */
    @Column(name = "drive_name", nullable = false)
    public String driveName;

    /**
     * Additional datasource metadata as JSONB.
     * Stores default metadata, custom config, etc.
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "metadata", columnDefinition = "jsonb")
    public String metadata;

    /**
     * Maximum file size allowed in bytes.
     * 0 = no limit.
     */
    @Column(name = "max_file_size")
    public Long maxFileSize = 0L;

    /**
     * Rate limit for uploads per minute.
     * 0 = no limit.
     */
    @Column(name = "rate_limit_per_minute")
    public Long rateLimitPerMinute = 0L;

    /**
     * Whether the datasource is active.
     * Inactive datasources cannot authenticate or ingest documents.
     */
    @Column(name = "active", nullable = false)
    public Boolean active = true;

    /**
     * Status reason for datasource state.
     * Examples: "account_inactive", "manual_disable", "api_key_rotated"
     */
    @Column(name = "status_reason")
    public String statusReason;

    /**
     * Timestamp when the datasource was created.
     */
    @Column(name = "created_at", columnDefinition = "TIMESTAMP WITH TIME ZONE")
    public OffsetDateTime createdAt;

    /**
     * Timestamp when the datasource was last updated.
     */
    @Column(name = "updated_at", columnDefinition = "TIMESTAMP WITH TIME ZONE")
    public OffsetDateTime updatedAt;

    /**
     * Timestamp when the API key was last rotated.
     */
    @Column(name = "last_rotated_at", columnDefinition = "TIMESTAMP WITH TIME ZONE")
    public OffsetDateTime lastRotatedAt;

    /**
     * Many-to-one relationship to Connector entity.
     */
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "connector_id", insertable = false, updatable = false)
    public Connector connector;

    /**
     * Tier 1 configuration overrides (serialized protobuf).
     * Serialized ConnectorGlobalConfig protobuf message containing:
     * PersistenceConfig, RetentionConfig, EncryptionConfig, HydrationConfig.
     * Overrides connector defaults for this specific datasource instance.
     */
    @Column(name = "global_config_proto", columnDefinition = "BYTEA")
    public byte[] globalConfigProto;

    /**
     * Custom configuration overrides (JSON Schema-validated).
     * Instance-level custom configuration that overrides connector defaults.
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "custom_config", columnDefinition = "jsonb")
    public String customConfig;

    /**
     * Schema version used for validation of custom_config.
     * References connector_config_schemas.schema_id.
     */
    @Column(name = "custom_config_schema_id")
    public String customConfigSchemaId;

    /**
     * Many-to-one relationship to ConnectorConfigSchema entity.
     */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "custom_config_schema_id", insertable = false, updatable = false)
    public ConnectorConfigSchema configSchema;

    /**
     * Default constructor for JPA.
     */
    public DataSource() {}

    /**
     * Create a new datasource.
     *
     * @param datasourceId Unique identifier (deterministic hash)
     * @param accountId Account that owns this datasource
     * @param connectorId Connector type to bind
     * @param name Human-readable name
     * @param apiKeyHash Hashed API key
     * @param driveName Drive name for storage
     */
    public DataSource(String datasourceId, String accountId, String connectorId,
                      String name, String apiKeyHash, String driveName) {
        this.datasourceId = datasourceId;
        this.accountId = accountId;
        this.connectorId = connectorId;
        this.name = name;
        this.apiKeyHash = apiKeyHash;
        this.driveName = driveName;
        this.active = true;
        this.metadata = "{}";
        this.createdAt = OffsetDateTime.now();
        this.updatedAt = OffsetDateTime.now();
    }
}
