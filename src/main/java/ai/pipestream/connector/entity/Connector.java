package ai.pipestream.connector.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
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
