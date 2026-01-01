package ai.pipestream.connector.repository;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.DataSource;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Repository for DataSource entity CRUD operations with fully reactive database access.
 * <p>
 * Provides transactional access to the datasources table with support for:
 * <ul>
 *   <li>DataSource creation with deterministic ID generation</li>
 *   <li>API key hashing and rotation</li>
 *   <li>Status management (activate/disable with reason)</li>
 *   <li>Soft deletion</li>
 *   <li>Pagination and counts for listings</li>
 *   <li>Connector type lookups (pre-seeded templates)</li>
 * </ul>
 * <p>
 * <strong>Reactive Implementation:</strong>
 * All methods return {@link Uni} for fully non-blocking database operations using
 * Hibernate Reactive Panache. Transactions are handled via {@link Panache#withTransaction(java.util.function.Supplier)}.
 */
@ApplicationScoped
public class DataSourceRepository {

    private static final Logger LOG = Logger.getLogger(DataSourceRepository.class);

    // ========================================================================
    // DataSource CRUD Operations
    // ========================================================================

    /**
     * Generate a deterministic datasource ID from account and connector.
     * <p>
     * datasourceId = UUID.nameUUIDFromBytes((accountId + connectorId).getBytes(UTF_8))
     * <p>
     * This is a pure function (no database access) so it's synchronous.
     *
     * @param accountId Account identifier
     * @param connectorId Connector type identifier
     * @return Deterministic UUID string
     */
    public String generateDatasourceId(String accountId, String connectorId) {
        String input = accountId + connectorId;
        return UUID.nameUUIDFromBytes(input.getBytes(StandardCharsets.UTF_8)).toString();
    }

    /**
     * Create a new datasource with a pre-computed API key hash (reactive).
     * <p>
     * The datasource ID is generated deterministically from accountId + connectorId.
     *
     * @param accountId Account that owns this datasource
     * @param connectorId Connector type to bind
     * @param name Human-readable name
     * @param apiKeyHash Argon2id hash of the API key
     * @param driveName Drive name for storage (references FilesystemService)
     * @param metadata Optional metadata JSON
     * @return Uni that completes with the created DataSource entity, or null if it already exists
     */
    public Uni<DataSource> createDataSource(String accountId, String connectorId, String name,
                                            String apiKeyHash, String driveName, String metadata) {
        return Panache.withTransaction(() -> {
            String datasourceId = generateDatasourceId(accountId, connectorId);

            // Check if datasource already exists
            return DataSource.<DataSource>findById(datasourceId)
                .flatMap(existing -> {
                    if (existing != null) {
                        LOG.warnf("DataSource already exists for account %s and connector %s", accountId, connectorId);
                        return Uni.createFrom().nullItem();
                    }

                    DataSource datasource = new DataSource(datasourceId, accountId, connectorId,
                                                          name, apiKeyHash, driveName);
                    if (metadata != null && !metadata.isEmpty()) {
                        datasource.metadata = metadata;
                    }

                    LOG.infof("Creating datasource: %s (account: %s, connector: %s)", datasourceId, accountId, connectorId);
                    return datasource.<DataSource>persist()
                        .invoke(persisted -> LOG.infof("Created datasource: %s (account: %s, connector: %s)",
                            persisted.datasourceId, persisted.accountId, persisted.connectorId));
                });
        });
    }

    /**
     * Find datasource by ID (reactive).
     *
     * @param datasourceId Unique datasource identifier
     * @return Uni that completes with DataSource entity or null if not found
     */
    public Uni<DataSource> findByDatasourceId(String datasourceId) {
        return Panache.withSession(() -> DataSource.<DataSource>findById(datasourceId));
    }

    /**
     * Find datasource by account and connector (reactive).
     * <p>
     * Uses deterministic ID lookup.
     *
     * @param accountId Account identifier
     * @param connectorId Connector type identifier
     * @return Uni that completes with DataSource entity or null if not found
     */
    public Uni<DataSource> findByAccountAndConnector(String accountId, String connectorId) {
        String datasourceId = generateDatasourceId(accountId, connectorId);
        return findByDatasourceId(datasourceId);
    }

    /**
     * List datasources for an account (reactive).
     *
     * @param accountId Account identifier
     * @param includeInactive If false, only return active datasources
     * @param limit Maximum number of results (0 = no limit)
     * @param offset Number of results to skip
     * @return Uni that completes with List of matching datasources
     */
    public Uni<List<DataSource>> listByAccount(String accountId, boolean includeInactive,
                                               int limit, int offset) {
        return Panache.withSession(() -> {
            var query = includeInactive
                ? DataSource.<DataSource>find("accountId = ?1 ORDER BY createdAt DESC", accountId)
                : DataSource.<DataSource>find("accountId = ?1 AND active = true ORDER BY createdAt DESC", accountId);

            if (offset > 0 || limit > 0) {
                int pageIndex = offset > 0 ? offset / Math.max(limit, 1) : 0;
                int pageSize = limit > 0 ? limit : 50;
                query = query.page(pageIndex, pageSize);
            }

            return query.list();
        });
    }

    /**
     * List all datasources with pagination (reactive).
     *
     * @param includeInactive If false, only return active datasources
     * @param limit Maximum number of results (0 = no limit)
     * @param offset Number of results to skip
     * @return Uni that completes with List of matching datasources
     */
    public Uni<List<DataSource>> listAll(boolean includeInactive, int limit, int offset) {
        return Panache.withSession(() -> {
            var query = includeInactive
                ? DataSource.<DataSource>findAll()
                : DataSource.<DataSource>find("active = true");

            if (offset > 0 || limit > 0) {
                int pageIndex = offset > 0 ? offset / Math.max(limit, 1) : 0;
                int pageSize = limit > 0 ? limit : 50;
                query = query.page(pageIndex, pageSize);
            }

            return query.list();
        });
    }

    /**
     * Count datasources for an account (reactive).
     *
     * @param accountId Account identifier (optional, null means count all)
     * @param includeInactive If false, only count active datasources
     * @return Uni that completes with the total count
     */
    public Uni<Long> countDataSources(String accountId, boolean includeInactive) {
        return Panache.withSession(() -> {
            if (accountId != null && !accountId.isEmpty()) {
                if (includeInactive) {
                    return DataSource.count("accountId", accountId);
                } else {
                    return DataSource.count("accountId = ?1 AND active = true", accountId);
                }
            } else {
                if (includeInactive) {
                    return DataSource.count();
                } else {
                    return DataSource.count("active = true");
                }
            }
        });
    }

    /**
     * Update datasource fields (reactive).
     *
     * @param datasourceId DataSource identifier
     * @param newName New name (optional)
     * @param newMetadata New metadata JSON (optional)
     * @param newDriveName New drive name (optional)
     * @return Uni that completes with updated datasource or null if not found
     */
    public Uni<DataSource> updateDataSource(String datasourceId, String newName,
                                           String newMetadata, String newDriveName) {
        return Panache.withTransaction(() ->
            findByDatasourceId(datasourceId)
                .flatMap(datasource -> {
                    if (datasource == null) {
                        LOG.warnf("Cannot update datasource - not found: %s", datasourceId);
                        return Uni.createFrom().nullItem();
                    }

                    boolean changed = false;

                    if (newName != null && !newName.isEmpty()) {
                        datasource.name = newName;
                        changed = true;
                    }

                    if (newMetadata != null && !newMetadata.isEmpty()) {
                        datasource.metadata = newMetadata;
                        changed = true;
                    }

                    if (newDriveName != null && !newDriveName.isEmpty()) {
                        datasource.driveName = newDriveName;
                        changed = true;
                    }

                    if (changed) {
                        datasource.updatedAt = OffsetDateTime.now();
                        LOG.infof("Updating datasource %s", datasourceId);
                        return datasource.<DataSource>persist()
                            .invoke(persisted -> LOG.infof("Updated datasource %s", persisted.datasourceId));
                    } else {
                        return Uni.createFrom().item(datasource);
                    }
                })
        );
    }

    /**
     * Set datasource active status (reactive).
     *
     * @param datasourceId DataSource identifier
     * @param active New status (true=active, false=inactive)
     * @param reason Reason for status change
     * @return Uni that completes with true if status was updated, false if not found
     */
    public Uni<Boolean> setDataSourceStatus(String datasourceId, boolean active, String reason) {
        return Panache.withTransaction(() ->
            findByDatasourceId(datasourceId)
                .flatMap(datasource -> {
                    if (datasource == null) {
                        LOG.warnf("Cannot set status - datasource not found: %s", datasourceId);
                        return Uni.createFrom().item(false);
                    }

                    if (datasource.active == active) {
                        LOG.debugf("DataSource %s already has status active=%s", datasourceId, active);
                        return Uni.createFrom().item(true); // Idempotent
                    }

                    datasource.active = active;
                    datasource.statusReason = reason;
                    datasource.updatedAt = OffsetDateTime.now();
                    LOG.infof("Setting datasource %s status to active=%s, reason: %s", datasourceId, active, reason);
                    return datasource.<DataSource>persist()
                        .map(persisted -> true)
                        .invoke(v -> LOG.infof("Set datasource %s status to active=%s, reason: %s",
                            datasourceId, active, reason));
                })
        );
    }

    /**
     * Delete a datasource (soft delete) (reactive).
     *
     * @param datasourceId DataSource identifier
     * @param reason Reason for deletion
     * @return Uni that completes with true if deleted, false if not found
     */
    public Uni<Boolean> deleteDataSource(String datasourceId, String reason) {
        return Panache.withTransaction(() ->
            findByDatasourceId(datasourceId)
                .flatMap(datasource -> {
                    if (datasource == null) {
                        LOG.warnf("Cannot delete datasource - not found: %s", datasourceId);
                        return Uni.createFrom().item(false);
                    }

                    datasource.active = false;
                    datasource.statusReason = reason;
                    datasource.updatedAt = OffsetDateTime.now();
                    LOG.infof("Deleting datasource %s, reason: %s", datasourceId, reason);
                    return datasource.<DataSource>persist()
                        .map(persisted -> true)
                        .invoke(v -> LOG.infof("Deleted datasource %s, reason: %s", datasourceId, reason));
                })
        );
    }

    /**
     * Rotate the API key for a datasource (reactive).
     *
     * @param datasourceId DataSource identifier
     * @param newApiKeyHash New hashed API key
     * @return Uni that completes with true if rotated, false if not found
     */
    public Uni<Boolean> rotateApiKey(String datasourceId, String newApiKeyHash) {
        return Panache.withTransaction(() ->
            findByDatasourceId(datasourceId)
                .flatMap(datasource -> {
                    if (datasource == null) {
                        LOG.warnf("Cannot rotate API key - datasource not found: %s", datasourceId);
                        return Uni.createFrom().item(false);
                    }

                    datasource.apiKeyHash = newApiKeyHash;
                    datasource.lastRotatedAt = OffsetDateTime.now();
                    datasource.updatedAt = OffsetDateTime.now();
                    LOG.infof("Rotating API key for datasource: %s", datasourceId);
                    return datasource.<DataSource>persist()
                        .map(persisted -> true)
                        .invoke(v -> LOG.infof("Rotated API key for datasource: %s", datasourceId));
                })
        );
    }

    // ========================================================================
    // Connector Type Operations (Pre-seeded Templates)
    // ========================================================================

    /**
     * Find connector type by ID (reactive).
     *
     * @param connectorId Connector type identifier
     * @return Uni that completes with Connector entity or null if not found
     */
    public Uni<Connector> findConnectorById(String connectorId) {
        return Panache.withSession(() -> Connector.<Connector>findById(connectorId));
    }

    /**
     * Find connector type by type name (reactive).
     *
     * @param connectorType Type name (e.g., "s3", "file-crawler")
     * @return Uni that completes with Connector entity or null if not found
     */
    public Uni<Connector> findConnectorByType(String connectorType) {
        return Panache.withSession(() -> 
            Connector.<Connector>find("connectorType", connectorType).firstResult()
        );
    }

    /**
     * List all connector types (reactive).
     *
     * @return Uni that completes with List of all connector types
     */
    public Uni<List<Connector>> listConnectorTypes() {
        return Panache.withSession(() -> Connector.listAll());
    }

    /**
     * Count connector types (reactive).
     *
     * @return Uni that completes with the total count
     */
    public Uni<Long> countConnectorTypes() {
        return Panache.withSession(() -> Connector.count());
    }
}
