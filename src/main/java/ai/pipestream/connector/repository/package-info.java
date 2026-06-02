/**
 * Repository classes for database access in the Connector Admin service.
 *
 * <p>Repositories use Hibernate ORM Panache with synchronous methods. Transactional
 * write methods are marked with {@link jakarta.transaction.Transactional}.
 *
 * <ul>
 *   <li>{@link ai.pipestream.connector.repository.DataSourceRepository} — CRUD for the
 *       {@code datasources} table.  Includes API-key rotation, status management, and
 *       pagination helpers.</li>
 *   <li>{@link ai.pipestream.connector.repository.ConnectorRegistrationRepository} — CRUD
 *       for the {@code connectors} and {@code connector_config_schemas} tables.</li>
 * </ul>
 */
package ai.pipestream.connector.repository;
