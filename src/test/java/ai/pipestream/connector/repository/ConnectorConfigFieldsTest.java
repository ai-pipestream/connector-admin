package ai.pipestream.connector.repository;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.ConnectorConfigSchema;
import ai.pipestream.connector.entity.DataSource;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for new configuration fields in Connector and DataSource entities with Hibernate Reactive.
 */
@QuarkusTest
public class ConnectorConfigFieldsTest {

    @Inject
    DataSourceRepository repository;

    private static final String TEST_CONNECTOR_ID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"; // Pre-seeded S3 connector

    @BeforeEach
    @RunOnVertxContext
    void setUp(UniAsserter asserter) {
        // Clean up test datasources (reactive)
        asserter.execute(() -> Panache.withTransaction(() -> DataSource.deleteAll()));
        
        // Clear any test-created schema references from connectors (reactive)
        asserter.execute(() -> Panache.withTransaction(() ->
            Connector.<Connector>listAll()
                .flatMap(connectors -> {
                    io.smallrye.mutiny.Uni<?> lastUni = io.smallrye.mutiny.Uni.createFrom().voidItem();
                    for (Connector connector : connectors) {
                        if (connector.customConfigSchemaId != null) {
                            connector.customConfigSchemaId = null;
                            lastUni = connector.<Connector>persist();
                        }
                    }
                    return lastUni;
                })
        ));
    }
    
    @AfterEach
    @RunOnVertxContext
    void tearDown(UniAsserter asserter) {
        // Clean up test-created schemas (reactive)
        asserter.execute(() -> Panache.withTransaction(() ->
            ConnectorConfigSchema.<ConnectorConfigSchema>listAll()
                .flatMap(schemas -> {
                    io.smallrye.mutiny.Uni<?> lastUni = io.smallrye.mutiny.Uni.createFrom().voidItem();
                    for (ConnectorConfigSchema schema : schemas) {
                        lastUni = Connector.count("customConfigSchemaId", schema.schemaId)
                            .flatMap(connectorRefs ->
                                DataSource.count("customConfigSchemaId", schema.schemaId)
                                    .flatMap(datasourceRefs -> {
                                        if (connectorRefs == 0 && datasourceRefs == 0) {
                                            return schema.delete();
                                        }
                                        return io.smallrye.mutiny.Uni.createFrom().voidItem();
                                    })
                            );
                    }
                    return lastUni;
                })
        ));
    }

    // ========================================================================
    // Connector Configuration Fields Tests
    // ========================================================================

    @Test
    @RunOnVertxContext
    void testConnector_DefaultConfigurationFields(UniAsserter asserter) {
        asserter.assertThat(() -> Panache.withSession(() -> repository.findConnectorById(TEST_CONNECTOR_ID)), connector -> {
            assertNotNull(connector);

            // Update fields
            asserter.assertThat(() -> Panache.withTransaction(() -> {
                connector.defaultPersistPipedoc = false;
                connector.defaultMaxInlineSizeBytes = 2097152; // 2MB
                connector.defaultCustomConfig = "{\"test\":\"value\"}";
                connector.displayName = "S3 Connector";
                connector.owner = "pipestream-team";
                connector.documentationUrl = "https://docs.example.com/s3";
                connector.tags = java.util.List.of("storage", "s3", "aws");
                return connector.<Connector>persist();
            }), updated -> {
                assertNotNull(updated);
            });

            // Verify
            asserter.assertThat(() -> Panache.withSession(() -> repository.findConnectorById(TEST_CONNECTOR_ID)), verified -> {
                assertNotNull(verified);
                assertFalse(verified.defaultPersistPipedoc);
                assertEquals(2097152, verified.defaultMaxInlineSizeBytes);
                assertEquals("{\"test\":\"value\"}", verified.defaultCustomConfig);
                assertEquals("S3 Connector", verified.displayName);
                assertEquals("pipestream-team", verified.owner);
                assertEquals("https://docs.example.com/s3", verified.documentationUrl);
                assertIterableEquals(java.util.List.of("storage", "s3", "aws"), verified.tags);
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testConnector_ConfigSchemaRelationship(UniAsserter asserter) {
        // Create a config schema (reactive)
        String schemaId = UUID.randomUUID().toString();
        asserter.execute(() -> Panache.withTransaction(() -> {
            ConnectorConfigSchema s = new ConnectorConfigSchema(
                schemaId, TEST_CONNECTOR_ID, "1.0.0",
                "{}", "{}", "test-user"
            );
            return s.<ConnectorConfigSchema>persist().replaceWithVoid();
        }));

        // Link connector to schema (reactive) - use sequential calls
        asserter.assertThat(() -> Panache.withSession(() -> repository.findConnectorById(TEST_CONNECTOR_ID)), connector -> {
            connector.customConfigSchemaId = schemaId;
        });
        
        // Update connector
        asserter.execute(() -> Panache.withTransaction(() ->
            repository.findConnectorById(TEST_CONNECTOR_ID)
                .flatMap(connector -> {
                    connector.customConfigSchemaId = schemaId;
                    return connector.<Connector>persist();
                })
                .replaceWithVoid()
        ));

        // Verify relationship
        asserter.assertThat(() -> Panache.withSession(() -> repository.findConnectorById(TEST_CONNECTOR_ID)), verified -> {
            assertNotNull(verified);
            assertEquals(schemaId, verified.customConfigSchemaId);
        });
    }

    // ========================================================================
    // DataSource Configuration Fields Tests
    // ========================================================================

    @Test
    @RunOnVertxContext
    void testDataSource_GlobalConfigProto(UniAsserter asserter) {
        String accountId = "test-account-" + System.currentTimeMillis();
        
        asserter.assertThat(() -> Panache.withTransaction(() -> repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        )), ds -> {
            byte[] configProto = "test-proto-bytes".getBytes();
            
            // Set global config proto (simulated serialized protobuf) (reactive)
            asserter.assertThat(() -> Panache.withTransaction(() ->
                DataSource.<DataSource>findById(ds.datasourceId)
                    .flatMap(dataSource -> {
                        dataSource.globalConfigProto = configProto;
                        return dataSource.<DataSource>persist();
                    })
            ), updated -> {
                assertNotNull(updated);
            });

            // Verify
            asserter.assertThat(() -> Panache.withSession(() -> repository.findByDatasourceId(ds.datasourceId)), verified -> {
                assertNotNull(verified);
                assertNotNull(verified.globalConfigProto);
                assertArrayEquals(configProto, verified.globalConfigProto);
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testDataSource_CustomConfig(UniAsserter asserter) {
        String accountId = "test-account-" + System.currentTimeMillis();
        
        asserter.assertThat(() -> Panache.withTransaction(() -> repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        )), ds -> {
            // Set custom config (JSON Schema-validated) (reactive)
            String customConfig = "{\"parse_images\":true,\"include_history\":false}";
            
            asserter.assertThat(() -> Panache.withTransaction(() ->
                DataSource.<DataSource>findById(ds.datasourceId)
                    .flatMap(dataSource -> {
                        dataSource.customConfig = customConfig;
                        return dataSource.<DataSource>persist();
                    })
            ), updated -> {
                assertNotNull(updated);
            });

            // Verify
            asserter.assertThat(() -> Panache.withSession(() -> repository.findByDatasourceId(ds.datasourceId)), verified -> {
                assertNotNull(verified);
                assertEquals(customConfig, verified.customConfig);
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testDataSource_ConfigSchemaReference(UniAsserter asserter) {
        // Create a config schema (reactive)
        String schemaId = UUID.randomUUID().toString();
        asserter.execute(() -> Panache.withTransaction(() -> {
            ConnectorConfigSchema s = new ConnectorConfigSchema(
                schemaId, TEST_CONNECTOR_ID, "1.0.0",
                "{}", "{}", "test-user"
            );
            return s.<ConnectorConfigSchema>persist().replaceWithVoid();
        }));

        // Create datasource with schema reference (reactive)
        String accountId = "test-account-" + System.currentTimeMillis();
        asserter.assertThat(() -> Panache.withTransaction(() -> repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        )), ds -> {
            asserter.assertThat(() -> Panache.withTransaction(() ->
                DataSource.<DataSource>findById(ds.datasourceId)
                    .flatMap(dataSource -> {
                        dataSource.customConfigSchemaId = schemaId;
                        return dataSource.<DataSource>persist();
                    })
            ), updated -> {
                assertNotNull(updated);
            });

            // Verify
            asserter.assertThat(() -> Panache.withSession(() -> repository.findByDatasourceId(ds.datasourceId)), verified -> {
                assertNotNull(verified);
                assertEquals(schemaId, verified.customConfigSchemaId);
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testDataSource_AllConfigFieldsTogether(UniAsserter asserter) {
        // Create schema (reactive)
        String schemaId = UUID.randomUUID().toString();
        asserter.execute(() -> Panache.withTransaction(() -> {
            ConnectorConfigSchema s = new ConnectorConfigSchema(
                schemaId, TEST_CONNECTOR_ID, "1.0.0",
                "{\"type\":\"object\"}", "{\"type\":\"object\"}", "test-user"
            );
            return s.<ConnectorConfigSchema>persist().replaceWithVoid();
        }));

        // Create datasource with all config fields (reactive)
        String accountId = "test-account-" + System.currentTimeMillis();
        asserter.assertThat(() -> Panache.withTransaction(() -> repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        )), ds -> {
            byte[] configProto = "global-config-proto".getBytes();
            String customConfig = "{\"connector_specific\":\"value\"}";

            asserter.assertThat(() -> Panache.withTransaction(() ->
                DataSource.<DataSource>findById(ds.datasourceId)
                    .flatMap(dataSource -> {
                        dataSource.globalConfigProto = configProto;
                        dataSource.customConfig = customConfig;
                        dataSource.customConfigSchemaId = schemaId;
                        return dataSource.<DataSource>persist();
                    })
            ), updated -> {
                assertNotNull(updated);
            });

            // Verify
            asserter.assertThat(() -> Panache.withSession(() -> repository.findByDatasourceId(ds.datasourceId)), verified -> {
                assertNotNull(verified);
                assertArrayEquals(configProto, verified.globalConfigProto);
                assertEquals(customConfig, verified.customConfig);
                assertEquals(schemaId, verified.customConfigSchemaId);
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testDataSource_ConfigFieldsNullable(UniAsserter asserter) {
        // Verify that config fields can be null (for backward compatibility)
        String accountId = "test-account-" + System.currentTimeMillis();
        
        asserter.assertThat(() -> Panache.withTransaction(() -> repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        )), ds -> {
            // All new config fields should be null by default
            assertNull(ds.globalConfigProto);
            assertNull(ds.customConfig);
            assertNull(ds.customConfigSchemaId);

            // Verify it persists correctly with null values (reactive)
            asserter.execute(() -> Panache.withTransaction(() ->
                DataSource.<DataSource>findById(ds.datasourceId)
                    .flatMap(dataSource -> dataSource.<DataSource>persist())
                    .replaceWithVoid()
            ));
            
            // Verify after reload
            asserter.assertThat(() -> Panache.withSession(() -> repository.findByDatasourceId(ds.datasourceId)), verified -> {
                assertNotNull(verified);
                assertNull(verified.globalConfigProto);
                assertNull(verified.customConfig);
                assertNull(verified.customConfigSchemaId);
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testConnector_DefaultValues(UniAsserter asserter) {
        asserter.assertThat(() -> Panache.withSession(() -> repository.findConnectorById(TEST_CONNECTOR_ID)), connector -> {
            assertNotNull(connector);

            // Verify default values when explicitly set (reactive)
            asserter.assertThat(() -> Panache.withTransaction(() -> {
                connector.defaultPersistPipedoc = null; // Clear existing
                connector.defaultMaxInlineSizeBytes = null;
                return connector.<Connector>persist();
            }), updated -> {
                assertNotNull(updated);
            });

            // Verify
            asserter.assertThat(() -> Panache.withSession(() -> repository.findConnectorById(TEST_CONNECTOR_ID)), verified -> {
                assertNotNull(verified);
                // Defaults are set at application level, not DB level
                // This test verifies fields can be null
                assertNull(verified.defaultPersistPipedoc);
                assertNull(verified.defaultMaxInlineSizeBytes);
            });
        });
    }
}
