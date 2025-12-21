package ai.pipestream.connector.repository;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.DataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Repository for DataSource entity CRUD operations.
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
 */
@ApplicationScoped
public class DataSourceRepository {

    private static final Logger LOG = Logger.getLogger(DataSourceRepository.class);

    @Inject
    EntityManager entityManager;

    // ========================================================================
    // DataSource CRUD Operations
    // ========================================================================

    /**
     * Generate a deterministic datasource ID from account and connector.
     * <p>
     * datasourceId = UUID.nameUUIDFromBytes((accountId + connectorId).getBytes(UTF_8))
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
     * Create a new datasource with a pre-computed API key hash.
     * <p>
     * The datasource ID is generated deterministically from accountId + connectorId.
     *
     * @param accountId Account that owns this datasource
     * @param connectorId Connector type to bind
     * @param name Human-readable name
     * @param apiKeyHash Argon2id hash of the API key
     * @param driveName Drive name for storage (references FilesystemService)
     * @param metadata Optional metadata JSON
     * @return The created DataSource entity
     */
    @Transactional
    public DataSource createDataSource(String accountId, String connectorId, String name,
                                        String apiKeyHash, String driveName, String metadata) {
        String datasourceId = generateDatasourceId(accountId, connectorId);

        // Check if datasource already exists
        DataSource existing = findByDatasourceId(datasourceId);
        if (existing != null) {
            LOG.warnf("DataSource already exists for account %s and connector %s", accountId, connectorId);
            return null;
        }

        DataSource datasource = new DataSource(datasourceId, accountId, connectorId,
                                                name, apiKeyHash, driveName);
        if (metadata != null && !metadata.isEmpty()) {
            datasource.metadata = metadata;
        }
        datasource.persist();

        LOG.infof("Created datasource: %s (account: %s, connector: %s)",
                  datasourceId, accountId, connectorId);
        return datasource;
    }

    /**
     * Find datasource by ID.
     *
     * @param datasourceId Unique datasource identifier
     * @return DataSource entity or null if not found
     */
    @Transactional
    public DataSource findByDatasourceId(String datasourceId) {
        return DataSource.findById(datasourceId);
    }

    /**
     * Find datasource by account and connector.
     * <p>
     * Uses deterministic ID lookup.
     *
     * @param accountId Account identifier
     * @param connectorId Connector type identifier
     * @return DataSource entity or null if not found
     */
    @Transactional
    public DataSource findByAccountAndConnector(String accountId, String connectorId) {
        String datasourceId = generateDatasourceId(accountId, connectorId);
        return findByDatasourceId(datasourceId);
    }

    /**
     * List datasources for an account.
     *
     * @param accountId Account identifier
     * @param includeInactive If false, only return active datasources
     * @param limit Maximum number of results (0 = no limit)
     * @param offset Number of results to skip
     * @return List of matching datasources
     */
    @Transactional
    public List<DataSource> listByAccount(String accountId, boolean includeInactive,
                                          int limit, int offset) {
        String query = "FROM DataSource d WHERE d.accountId = :accountId";
        if (!includeInactive) {
            query += " AND d.active = true";
        }
        query += " ORDER BY d.createdAt DESC";

        var typedQuery = entityManager.createQuery(query, DataSource.class)
                            .setParameter("accountId", accountId);

        if (offset > 0) {
            typedQuery.setFirstResult(offset);
        }
        if (limit > 0) {
            typedQuery.setMaxResults(limit);
        }

        return typedQuery.getResultList();
    }

    /**
     * List all datasources with pagination.
     *
     * @param includeInactive If false, only return active datasources
     * @param limit Maximum number of results (0 = no limit)
     * @param offset Number of results to skip
     * @return List of matching datasources
     */
    @Transactional
    public List<DataSource> listAll(boolean includeInactive, int limit, int offset) {
        if (includeInactive) {
            if (limit > 0 || offset > 0) {
                return DataSource.findAll()
                    .page(offset / Math.max(limit, 1), limit > 0 ? limit : 50)
                    .list();
            }
            return DataSource.listAll();
        } else {
            if (limit > 0 || offset > 0) {
                return DataSource.find("active", true)
                    .page(offset / Math.max(limit, 1), limit > 0 ? limit : 50)
                    .list();
            }
            return DataSource.list("active", true);
        }
    }

    /**
     * Count datasources for an account.
     *
     * @param accountId Account identifier (optional)
     * @param includeInactive If false, only count active datasources
     * @return Total count
     */
    @Transactional
    public long countDataSources(String accountId, boolean includeInactive) {
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
                return DataSource.count("active", true);
            }
        }
    }

    /**
     * Update datasource fields.
     *
     * @param datasourceId DataSource identifier
     * @param newName New name (optional)
     * @param newMetadata New metadata JSON (optional)
     * @param newDriveName New drive name (optional)
     * @return Updated datasource or null if not found
     */
    @Transactional
    public DataSource updateDataSource(String datasourceId, String newName,
                                       String newMetadata, String newDriveName) {
        DataSource datasource = findByDatasourceId(datasourceId);
        if (datasource == null) {
            LOG.warnf("Cannot update datasource - not found: %s", datasourceId);
            return null;
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
            datasource.persist();
            LOG.infof("Updated datasource %s", datasourceId);
        }

        return datasource;
    }

    /**
     * Set datasource active status.
     *
     * @param datasourceId DataSource identifier
     * @param active New status (true=active, false=inactive)
     * @param reason Reason for status change
     * @return true if status was updated, false if not found
     */
    @Transactional
    public boolean setDataSourceStatus(String datasourceId, boolean active, String reason) {
        DataSource datasource = findByDatasourceId(datasourceId);
        if (datasource == null) {
            LOG.warnf("Cannot set status - datasource not found: %s", datasourceId);
            return false;
        }

        if (datasource.active == active) {
            LOG.debugf("DataSource %s already has status active=%s", datasourceId, active);
            return true; // Idempotent
        }

        datasource.active = active;
        datasource.statusReason = reason;
        datasource.updatedAt = OffsetDateTime.now();
        datasource.persist();

        LOG.infof("Set datasource %s status to active=%s, reason: %s", datasourceId, active, reason);
        return true;
    }

    /**
     * Delete a datasource (soft delete).
     *
     * @param datasourceId DataSource identifier
     * @param reason Reason for deletion
     * @return true if deleted, false if not found
     */
    @Transactional
    public boolean deleteDataSource(String datasourceId, String reason) {
        DataSource datasource = findByDatasourceId(datasourceId);
        if (datasource == null) {
            LOG.warnf("Cannot delete datasource - not found: %s", datasourceId);
            return false;
        }

        datasource.active = false;
        datasource.statusReason = reason;
        datasource.updatedAt = OffsetDateTime.now();
        datasource.persist();

        LOG.infof("Deleted datasource %s, reason: %s", datasourceId, reason);
        return true;
    }

    /**
     * Rotate the API key for a datasource.
     *
     * @param datasourceId DataSource identifier
     * @param newApiKeyHash New hashed API key
     * @return true if rotated, false if not found
     */
    @Transactional
    public boolean rotateApiKey(String datasourceId, String newApiKeyHash) {
        DataSource datasource = findByDatasourceId(datasourceId);
        if (datasource == null) {
            LOG.warnf("Cannot rotate API key - datasource not found: %s", datasourceId);
            return false;
        }

        datasource.apiKeyHash = newApiKeyHash;
        datasource.lastRotatedAt = OffsetDateTime.now();
        datasource.updatedAt = OffsetDateTime.now();
        datasource.persist();

        LOG.infof("Rotated API key for datasource: %s", datasourceId);
        return true;
    }

    // ========================================================================
    // Connector Type Operations (Pre-seeded Templates)
    // ========================================================================

    /**
     * Find connector type by ID.
     *
     * @param connectorId Connector type identifier
     * @return Connector entity or null if not found
     */
    @Transactional
    public Connector findConnectorById(String connectorId) {
        return Connector.findById(connectorId);
    }

    /**
     * Find connector type by type name.
     *
     * @param connectorType Type name (e.g., "s3", "file-crawler")
     * @return Connector entity or null if not found
     */
    @Transactional
    public Connector findConnectorByType(String connectorType) {
        return Connector.find("connectorType", connectorType).firstResult();
    }

    /**
     * List all connector types.
     *
     * @return List of all connector types
     */
    @Transactional
    public List<Connector> listConnectorTypes() {
        return Connector.listAll();
    }

    /**
     * Count connector types.
     *
     * @return Total count
     */
    @Transactional
    public long countConnectorTypes() {
        return Connector.count();
    }
}
