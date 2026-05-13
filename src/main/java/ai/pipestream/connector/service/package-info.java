/**
 * gRPC service implementations for the Connector Admin service.
 *
 * <h2>Services</h2>
 * <dl>
 *   <dt>{@link ai.pipestream.connector.service.DataSourceAdminServiceImpl}</dt>
 *   <dd>Full datasource lifecycle — create, read, update, delete, status control, and
 *       API-key management (generate / validate / rotate).  Connector type discovery
 *       ({@code ListConnectorTypes}, {@code GetConnectorType}) is also served here so
 *       that clients can enumerate available connector templates without needing a second
 *       service stub.</dd>
 *
 *   <dt>{@link ai.pipestream.connector.service.ConnectorRegistrationServiceImpl}</dt>
 *   <dd>Connector type registration and JSON Schema management.  Allows creating new
 *       connector templates, attaching versioned config schemas, and updating per-connector
 *       default configuration values.</dd>
 * </dl>
 *
 * <h2>Supporting beans</h2>
 * <ul>
 *   <li>{@link ai.pipestream.connector.service.AccountValidationService} — calls
 *       account-manager over gRPC to verify that an account exists and is active before
 *       a datasource is created.</li>
 *   <li>{@link ai.pipestream.connector.service.ConfigMergingService} — resolves the merged
 *       Tier 1 configuration returned in {@code ValidateApiKey} responses.</li>
 *   <li>{@link ai.pipestream.connector.service.ConnectorTypeSeedLoader} — seeds connector
 *       types from {@code connectors-seed.json} at application startup.</li>
 *   <li>{@link ai.pipestream.connector.service.AccountEventListener} — Kafka consumer
 *       that reacts to account lifecycle events (e.g., account deactivated).</li>
 * </ul>
 */
package ai.pipestream.connector.service;
