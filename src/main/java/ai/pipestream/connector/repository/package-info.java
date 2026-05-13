/**
 * Reactive repository classes for database access in the Connector Admin service.
 *
 * <p>All repositories use Hibernate Reactive Panache and return
 * {@link io.smallrye.mutiny.Uni} for non-blocking I/O.  Transactions are managed
 * explicitly via {@link io.quarkus.hibernate.reactive.panache.Panache#withTransaction}.
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
