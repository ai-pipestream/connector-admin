/**
 * JPA entity classes for the Connector Admin service.
 *
 * <ul>
 *   <li>{@link ai.pipestream.connector.entity.Connector} — connector type template
 *       (e.g., "s3", "file-crawler").  Pre-seeded and immutable at runtime except for
 *       default configuration fields updated via
 *       {@code ConnectorRegistrationService.UpdateConnectorTypeDefaults}.</li>
 *   <li>{@link ai.pipestream.connector.entity.DataSource} — an account's binding to a
 *       connector type.  Carries its own hashed API key and optional per-instance
 *       configuration overrides.</li>
 *   <li>{@link ai.pipestream.connector.entity.ConnectorConfigSchema} — a versioned JSON
 *       Schema definition attached to a connector type.  Used to validate custom
 *       configuration at both Tier 1 (connector-admin) and Tier 2 (pipestream-engine)
 *       levels.</li>
 * </ul>
 *
 * <p>All entities extend {@link io.quarkus.hibernate.orm.panache.PanacheEntityBase}
 * and rely on Flyway for schema management (no DDL auto-generation).
 */
package ai.pipestream.connector.entity;
