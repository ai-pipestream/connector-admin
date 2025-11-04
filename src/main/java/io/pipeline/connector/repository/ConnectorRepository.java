package io.pipeline.connector.repository;

import io.pipeline.connector.entity.Connector;
import io.pipeline.connector.entity.ConnectorAccount;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Repository for `Connector` entity CRUD and linkage operations.
 * <p>
 * Provides transactional access to the `connectors` table and related junctions with support for:
 * <ul>
 *   <li>Connector creation with metadata and API key hashing</li>
 *   <li>Account linkage (many-to-many via `connector_accounts`)</li>
 *   <li>API key rotation</li>
 *   <li>Status management (activate/disable with reason)</li>
 *   <li>Soft deletion</li>
 *   <li>Pagination and counts for listings</li>
 * </ul>
 *
 * Reactive semantics:
 * <ul>
 *   <li>Methods are invoked from service-layer `Uni` pipelines and run on worker threads.</li>
 *   <li>All methods are annotated {@code @Transactional}; operations may open DB transactions.</li>
 * </ul>
 *
 * Side effects:
 * <ul>
 *   <li>Reads and writes to the database via JPA/Hibernate Panache APIs.</li>
 *   <li>INFO/DEBUG logs for auditing and diagnostics.</li>
 * </ul>
 *
 * Thread safety:
 * <ul>
 *   <li>Methods are transaction-scoped and safe for concurrent access; entity state is not cached in fields.</li>
 * </ul>
 */
@ApplicationScoped
public class ConnectorRepository {

    private static final Logger LOG = Logger.getLogger(ConnectorRepository.class);

    @Inject
    EntityManager entityManager;

    /**
     * Creates a new connector using a precomputed API key hash.
     * <p>
     * Generates a unique connector ID (UUID) and persists the connector with the provided Argon2id hash.
     * The plaintext API key is never stored and should only be returned by the service layer.
     *
     * Reactive semantics:
     * - Runs inside a transaction on a worker thread; typically called from a service-layer {@code Uni} pipeline.
     *
     * Side effects:
     * - Inserts a new row into the {@code connectors} table; writes INFO logs for auditing.
     *
     * @param connectorName Platform-unique name for the connector.
     * @param connectorType Type identifier (filesystem, confluence, etc.).
     * @param description Optional description.
     * @param apiKeyHash Argon2id hash of the API key (caller must hash before passing).
     * @return The created {@code Connector} entity.
     */
    @Transactional
    public Connector createConnector(String connectorName, String connectorType,
                                    String description, String apiKeyHash) {
        return createConnectorWithMetadata(connectorName, connectorType, description, apiKeyHash, "{}");
    }

    /**
     * Create a new connector with metadata.
     *
     * @param connectorName Platform-unique name
     * @param connectorType Type identifier
     * @param description Optional description
     * @param apiKeyHash Hashed API key
     * @param metadataJson Metadata as JSON string
     * @return The created Connector entity
     */
    @Transactional
    public Connector createConnectorWithMetadata(String connectorName, String connectorType,
                                                String description, String apiKeyHash, String metadataJson) {
        String connectorId = UUID.randomUUID().toString();

        Connector connector = new Connector(connectorId, connectorName, connectorType,
                                           description, apiKeyHash);
        connector.metadata = metadataJson;
        connector.persist();

        LOG.infof("Created connector: %s (type: %s)", connectorName, connectorType);
        return connector;
    }

    /**
     * Find connector by ID.
     *
     * @param connectorId Unique connector identifier
     * @return Connector entity or null if not found
     */
    @Transactional
    public Connector findByConnectorId(String connectorId) {
        return Connector.findById(connectorId);
    }

    /**
     * Find connector by name.
     * <p>
     * Connector names are unique across the platform.
     *
     * @param connectorName Connector name
     * @return Connector entity or null if not found
     */
    @Transactional
    public Connector findByConnectorName(String connectorName) {
        return Connector.find("connectorName", connectorName).firstResult();
    }

    /**
     * List all connectors, optionally filtered by account and/or active status.
     *
     * @param accountId Optional account ID to filter by
     * @param includeInactive If false, only return active connectors
     * @param limit Maximum number of results (0 = no limit)
     * @param offset Number of results to skip
     * @return List of matching connectors
     */
    @Transactional
    public List<Connector> listConnectors(String accountId, boolean includeInactive, int limit, int offset) {
        if (accountId != null && !accountId.isEmpty()) {
            // Find connectors linked to this account via junction table
            String query = "SELECT c FROM Connector c " +
                          "JOIN ConnectorAccount ca ON c.connectorId = ca.connectorId " +
                          "WHERE ca.accountId = :accountId";

            if (!includeInactive) {
                query += " AND c.active = true";
            }

            var typedQuery = entityManager.createQuery(query, Connector.class)
                               .setParameter("accountId", accountId);

            if (offset > 0) {
                typedQuery.setFirstResult(offset);
            }
            if (limit > 0) {
                typedQuery.setMaxResults(limit);
            }

            return typedQuery.getResultList();
        } else {
            // List all connectors
            if (includeInactive) {
                if (limit > 0 || offset > 0) {
                    return Connector.findAll().page(offset / Math.max(limit, 1), limit > 0 ? limit : 50).list();
                }
                return Connector.listAll();
            } else {
                if (limit > 0 || offset > 0) {
                    return Connector.find("active", true).page(offset / Math.max(limit, 1), limit > 0 ? limit : 50).list();
                }
                return Connector.list("active", true);
            }
        }
    }

    /**
     * Set connector active status.
     * <p>
     * Inactive connectors cannot authenticate or ingest documents.
     * This method is idempotent.
     *
     * @param connectorId Connector identifier
     * @param active New status (true=active, false=inactive)
     * @param reason Reason for status change (logged)
     * @return true if status was updated, false if connector not found
     */
    @Transactional
    public boolean setConnectorStatus(String connectorId, boolean active, String reason) {
        Connector connector = findByConnectorId(connectorId);
        if (connector == null) {
            LOG.warnf("Cannot set status - connector not found: %s", connectorId);
            return false;
        }

        if (connector.active == active) {
            LOG.debugf("Connector %s already has status active=%s", connectorId, active);
            return true; // Idempotent
        }

        connector.active = active;
        connector.updatedAt = OffsetDateTime.now();
        connector.persist();

        LOG.infof("Set connector %s status to active=%s, reason: %s", connectorId, active, reason);
        return true;
    }

    /**
     * Disable a connector with a specific status reason.
     * <p>
     * Used for tracking why a connector was disabled (e.g., account_inactive).
     *
     * @param connectorId Connector identifier
     * @param statusReason Reason for disablement
     * @return true if disabled, false if connector not found
     */
    @Transactional
    public boolean disableConnector(String connectorId, String statusReason) {
        Connector connector = findByConnectorId(connectorId);
        if (connector == null) {
            LOG.warnf("Cannot disable connector - not found: %s", connectorId);
            return false;
        }

        if (!connector.active) {
            LOG.debugf("Connector %s already inactive", connectorId);
            return true; // Idempotent
        }

        connector.active = false;
        connector.statusReason = statusReason;
        connector.updatedAt = OffsetDateTime.now();
        connector.persist();

        LOG.infof("Disabled connector %s, reason: %s", connectorId, statusReason);
        return true;
    }

    /**
     * Enable a connector and clear status reason.
     * <p>
     * Used to re-enable connectors that were disabled due to account inactivation.
     *
     * @param connectorId Connector identifier
     * @return true if enabled, false if connector not found
     */
    @Transactional
    public boolean enableConnector(String connectorId) {
        Connector connector = findByConnectorId(connectorId);
        if (connector == null) {
            LOG.warnf("Cannot enable connector - not found: %s", connectorId);
            return false;
        }

        if (connector.active) {
            LOG.debugf("Connector %s already active", connectorId);
            return true; // Idempotent
        }

        connector.active = true;
        connector.statusReason = null; // Clear status reason
        connector.updatedAt = OffsetDateTime.now();
        connector.persist();

        LOG.infof("Enabled connector %s", connectorId);
        return true;
    }

    /**
     * Find all connectors linked to an account.
     * <p>
     * Returns ConnectorAccount entities with full connector details.
     *
     * @param accountId Account identifier
     * @return List of ConnectorAccount entities
     */
    @Transactional
    public List<ConnectorAccount> findByAccountId(String accountId) {
        return entityManager.createQuery(
            "SELECT ca FROM ConnectorAccount ca JOIN FETCH ca.connector WHERE ca.accountId = :accountId",
            ConnectorAccount.class)
            .setParameter("accountId", accountId)
            .getResultList();
    }

    /**
     * Rotate the API key for a connector.
     * <p>
     * Updates the api_key_hash and last_rotated_at timestamp.
     *
     * @param connectorId Connector identifier
     * @param newApiKeyHash New hashed API key
     * @return true if rotated, false if connector not found
     */
    @Transactional
    public boolean rotateApiKey(String connectorId, String newApiKeyHash) {
        Connector connector = findByConnectorId(connectorId);
        if (connector == null) {
            LOG.warnf("Cannot rotate API key - connector not found: %s", connectorId);
            return false;
        }

        connector.apiKeyHash = newApiKeyHash;
        connector.lastRotatedAt = OffsetDateTime.now();
        connector.updatedAt = OffsetDateTime.now();
        connector.persist();

        LOG.infof("Rotated API key for connector: %s", connectorId);
        return true;
    }

    /**
     * Link a connector to an account.
     * <p>
     * Creates an entry in the connector_accounts junction table.
     * Idempotent - does nothing if link already exists.
     *
     * @param connectorId Connector identifier
     * @param accountId Account identifier
     */
    @Transactional
    public void linkConnectorToAccount(String connectorId, String accountId) {
        // Check if link already exists
        long existing = ConnectorAccount.count("connectorId = ?1 and accountId = ?2",
                                               connectorId, accountId);
        if (existing > 0) {
            LOG.debugf("Connector %s already linked to account %s", connectorId, accountId);
            return;
        }

        ConnectorAccount link = new ConnectorAccount(connectorId, accountId);
        link.persist();

        LOG.infof("Linked connector %s to account %s", connectorId, accountId);
    }

    /**
     * Unlink a connector from an account.
     *
     * @param connectorId Connector identifier
     * @param accountId Account identifier
     * @return true if link was removed, false if it didn't exist
     */
    @Transactional
    public boolean unlinkConnectorFromAccount(String connectorId, String accountId) {
        long deleted = ConnectorAccount.delete("connectorId = ?1 and accountId = ?2",
                                               connectorId, accountId);

        if (deleted > 0) {
            LOG.infof("Unlinked connector %s from account %s", connectorId, accountId);
            return true;
        }

        LOG.debugf("No link found between connector %s and account %s", connectorId, accountId);
        return false;
    }

    /**
     * Get list of account IDs linked to a connector.
     *
     * @param connectorId Connector identifier
     * @return List of account IDs
     */
    @Transactional
    public List<String> getLinkedAccounts(String connectorId) {
        return entityManager.createQuery(
            "SELECT ca.accountId FROM ConnectorAccount ca WHERE ca.connectorId = :connectorId",
            String.class)
            .setParameter("connectorId", connectorId)
            .getResultList();
    }

    /**
     * Count total connectors matching filters (for pagination metadata).
     *
     * @param accountId Optional account ID filter
     * @param includeInactive If false, only count active connectors
     * @return Total count
     */
    @Transactional
    public long countConnectors(String accountId, boolean includeInactive) {
        if (accountId != null && !accountId.isEmpty()) {
            String query = "SELECT COUNT(c) FROM Connector c " +
                          "JOIN ConnectorAccount ca ON c.connectorId = ca.connectorId " +
                          "WHERE ca.accountId = :accountId";

            if (!includeInactive) {
                query += " AND c.active = true";
            }

            return entityManager.createQuery(query, Long.class)
                               .setParameter("accountId", accountId)
                               .getSingleResult();
        } else {
            if (includeInactive) {
                return Connector.count();
            } else {
                return Connector.count("active", true);
            }
        }
    }

    /**
     * Delete a connector (soft delete).
     * <p>
     * Sets active=false and records deletion reason and timestamp.
     *
     * @param connectorId Connector identifier
     * @param reason Reason for deletion
     * @return true if deleted, false if not found
     */
    @Transactional
    public boolean deleteConnector(String connectorId, String reason) {
        Connector connector = findByConnectorId(connectorId);
        if (connector == null) {
            LOG.warnf("Cannot delete connector - not found: %s", connectorId);
            return false;
        }

        connector.active = false;
        connector.deletedReason = reason;
        connector.deletedAt = OffsetDateTime.now();
        connector.updatedAt = OffsetDateTime.now();
        connector.persist();

        LOG.infof("Deleted connector %s, reason: %s", connectorId, reason);
        return true;
    }

    /**
     * Update connector fields.
     *
     * @param connectorId Connector identifier
     * @param newName New connector name (optional)
     * @param newMetadataJson New metadata JSON (optional)
     * @return Updated connector or null if not found
     */
    @Transactional
    public Connector updateConnector(String connectorId, String newName, String newMetadataJson) {
        Connector connector = findByConnectorId(connectorId);
        if (connector == null) {
            LOG.warnf("Cannot update connector - not found: %s", connectorId);
            return null;
        }

        boolean changed = false;

        if (newName != null && !newName.isEmpty()) {
            connector.connectorName = newName;
            changed = true;
        }

        if (newMetadataJson != null && !newMetadataJson.isEmpty()) {
            connector.metadata = newMetadataJson;
            changed = true;
        }

        if (changed) {
            connector.updatedAt = java.time.OffsetDateTime.now();
            connector.persist();
            LOG.infof("Updated connector %s", connectorId);
        }

        return connector;
    }
}
