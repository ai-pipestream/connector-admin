package ai.pipestream.connector.repository;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.ConnectorConfigSchema;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ConnectorConfigSchema entity CRUD operations with Hibernate Reactive.
 */
@QuarkusTest
public class ConnectorConfigSchemaTest {

    @Inject
    DataSourceRepository repository;

    private static final String TEST_CONNECTOR_ID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"; // Pre-seeded S3 connector

    @BeforeEach
    @RunOnVertxContext
    void setUp(UniAsserter asserter) {
        // Clean up test-created schema references from connectors (reactive)
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
        // Clean up test-created schemas (delete all for simplicity in tests)
        asserter.execute(() -> Panache.withTransaction(() -> ConnectorConfigSchema.deleteAll()));
    }

    @Test
    @RunOnVertxContext
    void testCreateConnectorConfigSchema_Success(UniAsserter asserter) {
        String schemaId = UUID.randomUUID().toString();
        String schemaVersion = "1.0.0";
        String customConfigSchema = "{\"type\":\"object\",\"properties\":{\"parse_images\":{\"type\":\"boolean\"}}}";
        String nodeCustomConfigSchema = "{\"type\":\"object\",\"properties\":{\"node_specific\":{\"type\":\"string\"}}}";
        String createdBy = "test-user";

        asserter.assertThat(() -> Panache.withTransaction(() -> {
            ConnectorConfigSchema s = new ConnectorConfigSchema(
                schemaId, TEST_CONNECTOR_ID, schemaVersion,
                customConfigSchema, nodeCustomConfigSchema, createdBy
            );
            return s.<ConnectorConfigSchema>persist();
        }), schema -> {
            assertNotNull(schema);
            assertEquals(schemaId, schema.schemaId);
            assertEquals(TEST_CONNECTOR_ID, schema.connectorId);
            assertEquals(schemaVersion, schema.schemaVersion);
            assertEquals(customConfigSchema, schema.customConfigSchema);
            assertEquals(nodeCustomConfigSchema, schema.nodeCustomConfigSchema);
            assertEquals(createdBy, schema.createdBy);
            assertEquals("PENDING", schema.syncStatus);
            assertNotNull(schema.createdAt);
        });
    }

    @Test
    @RunOnVertxContext
    void testCreateConnectorConfigSchema_UniqueConstraint(UniAsserter asserter) {
        String schemaVersion = "1.0.0";
        String customConfigSchema = "{\"type\":\"object\"}";
        String nodeCustomConfigSchema = "{\"type\":\"object\"}";

        String schemaId1 = UUID.randomUUID().toString();
        asserter.assertThat(() -> Panache.withTransaction(() -> {
            ConnectorConfigSchema s = new ConnectorConfigSchema(
                schemaId1, TEST_CONNECTOR_ID, schemaVersion,
                customConfigSchema, nodeCustomConfigSchema, "user1"
            );
            return s.<ConnectorConfigSchema>persist();
        }), schema1 -> {
            assertNotNull(schema1);
        });

        // Attempt to create duplicate (same connector + version) - should fail
        String schemaId2 = UUID.randomUUID().toString();
        asserter.assertThat(() -> Panache.withTransaction(() -> {
            ConnectorConfigSchema s2 = new ConnectorConfigSchema(
                schemaId2, TEST_CONNECTOR_ID, schemaVersion,
                customConfigSchema, nodeCustomConfigSchema, "user2"
            );
            return s2.<ConnectorConfigSchema>persist();
        }).onFailure().recoverWithUni(failure -> {
            // Expected ConstraintViolationException for unique constraint violation
            if (failure instanceof org.hibernate.exception.ConstraintViolationException) {
                return io.smallrye.mutiny.Uni.createFrom().nullItem();
            }
            // Re-throw unexpected exceptions
            return io.smallrye.mutiny.Uni.createFrom().failure(failure);
        }), schema2 -> {
            assertNull(schema2, "Should have failed due to unique constraint");
        });
    }

    @Test
    @RunOnVertxContext
    void testFindConnectorConfigSchemaById(UniAsserter asserter) {
        String schemaId = UUID.randomUUID().toString();
        String schemaVersion = "1.0.0";

        asserter.execute(() -> Panache.withTransaction(() -> {
            ConnectorConfigSchema s = new ConnectorConfigSchema(
                schemaId, TEST_CONNECTOR_ID, schemaVersion,
                "{}", "{}", "test-user"
            );
            return s.<ConnectorConfigSchema>persist().replaceWithVoid();
        }));

        asserter.assertThat(() -> Panache.withSession(() ->
            ConnectorConfigSchema.<ConnectorConfigSchema>findById(schemaId)
        ), found -> {
            assertNotNull(found);
            assertEquals(schemaId, found.schemaId);
            assertEquals(TEST_CONNECTOR_ID, found.connectorId);
            assertEquals(schemaVersion, found.schemaVersion);
        });
    }

    @Test
    @RunOnVertxContext
    void testFindConnectorConfigSchemaByConnectorId(UniAsserter asserter) {
        String schemaVersion = "1.0.0";
        String secondConnectorId = "b1ffc0aa-0d1c-5f09-cc7e-7cc0ce491b22"; // Pre-seeded file-crawler

        asserter.execute(() -> Panache.withTransaction(() -> {
            ConnectorConfigSchema s1 = new ConnectorConfigSchema(
                UUID.randomUUID().toString(), TEST_CONNECTOR_ID, schemaVersion,
                "{}", "{}", "user"
            );
            return s1.<ConnectorConfigSchema>persist().replaceWithVoid();
        }));

        asserter.execute(() -> Panache.withTransaction(() -> {
            ConnectorConfigSchema s2 = new ConnectorConfigSchema(
                UUID.randomUUID().toString(), secondConnectorId, schemaVersion,
                "{}", "{}", "user"
            );
            return s2.<ConnectorConfigSchema>persist().replaceWithVoid();
        }));

        // Find by connector ID
        asserter.assertThat(() -> Panache.withSession(() ->
            ConnectorConfigSchema.<ConnectorConfigSchema>find("connectorId", TEST_CONNECTOR_ID).list()
        ), found -> {
            assertEquals(1, found.size());
            ConnectorConfigSchema foundSchema = found.get(0);
            assertEquals(TEST_CONNECTOR_ID, foundSchema.connectorId);
        });
    }

    @Test
    @RunOnVertxContext
    void testUpdateConnectorConfigSchema(UniAsserter asserter) {
        String schemaId = UUID.randomUUID().toString();
        String originalSchema = "{\"type\":\"object\",\"properties\":{\"old\":{\"type\":\"string\"}}}";
        String updatedSchema = "{\"type\":\"object\",\"properties\":{\"new\":{\"type\":\"boolean\"}}}";

        asserter.execute(() -> Panache.withTransaction(() -> {
            ConnectorConfigSchema s = new ConnectorConfigSchema(
                schemaId, TEST_CONNECTOR_ID, "1.0.0",
                originalSchema, "{}", "test-user"
            );
            return s.<ConnectorConfigSchema>persist().replaceWithVoid();
        }));

        // Update schema
        asserter.assertThat(() -> Panache.withTransaction(() ->
            ConnectorConfigSchema.<ConnectorConfigSchema>findById(schemaId)
                .flatMap(schema -> {
                    schema.customConfigSchema = updatedSchema;
                    schema.syncStatus = "SYNCED";
                    schema.apicurioArtifactId = "test-artifact-123";
                    return schema.<ConnectorConfigSchema>persist();
                })
        ), updated -> {
            assertNotNull(updated);
            assertEquals(updatedSchema, updated.customConfigSchema);
            assertEquals("SYNCED", updated.syncStatus);
            assertEquals("test-artifact-123", updated.apicurioArtifactId);
        });
    }

    @Test
    @RunOnVertxContext
    void testDeleteConnectorConfigSchema(UniAsserter asserter) {
        String schemaId = UUID.randomUUID().toString();

        asserter.execute(() -> Panache.withTransaction(() -> {
            ConnectorConfigSchema s = new ConnectorConfigSchema(
                schemaId, TEST_CONNECTOR_ID, "1.0.0",
                "{}", "{}", "test-user"
            );
            return s.<ConnectorConfigSchema>persist().replaceWithVoid();
        }));

        asserter.assertThat(() -> Panache.withSession(() ->
            ConnectorConfigSchema.<ConnectorConfigSchema>findById(schemaId)
        ), found -> {
            assertNotNull(found);
        });

        // Explicit deletion should work when no connector references this schema
        asserter.execute(() -> Panache.withTransaction(() ->
            ConnectorConfigSchema.<ConnectorConfigSchema>findById(schemaId)
                .flatMap(schema -> schema.delete())
        ));

        asserter.assertThat(() -> Panache.withSession(() ->
            ConnectorConfigSchema.<ConnectorConfigSchema>findById(schemaId)
        ), found -> {
            assertNull(found);
        });
    }

    @Test
    @RunOnVertxContext
    void testConnector_CustomConfigSchemaId_FkConstraintRequiresSchemaExists(UniAsserter asserter) {
        asserter.assertThat(() -> Panache.withSession(() ->
            repository.findConnectorById(TEST_CONNECTOR_ID)
        ), connector -> {
            assertNotNull(connector);

            String nonexistentSchemaId = UUID.randomUUID().toString();
            asserter.assertThat(() -> Panache.withTransaction(() -> {
                connector.customConfigSchemaId = nonexistentSchemaId;
                return connector.<Connector>persist().onFailure().recoverWithUni(failure -> {
                    // Expected to fail due to FK constraint, return null
                    return io.smallrye.mutiny.Uni.createFrom().nullItem();
                });
            }), updated -> {
                assertNull(updated, "Should have failed due to FK constraint violation");
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testConnectorConfigSchema_RelationshipToConnector(UniAsserter asserter) {
        String schemaId = UUID.randomUUID().toString();

        asserter.execute(() -> Panache.withTransaction(() -> {
            ConnectorConfigSchema s = new ConnectorConfigSchema(
                schemaId, TEST_CONNECTOR_ID, "1.0.0",
                "{}", "{}", "test-user"
            );
            return s.<ConnectorConfigSchema>persist().replaceWithVoid();
        }));

        // Verify connector relationship exists
        asserter.assertThat(() -> Panache.withSession(() ->
            repository.findConnectorById(TEST_CONNECTOR_ID)
        ), connector -> {
            assertNotNull(connector);
            assertEquals(TEST_CONNECTOR_ID, connector.connectorId);
        });
        
        // Verify schema can be loaded
        asserter.assertThat(() -> Panache.withSession(() ->
            ConnectorConfigSchema.<ConnectorConfigSchema>findById(schemaId)
        ), loadedSchema -> {
            assertNotNull(loadedSchema);
            assertEquals(TEST_CONNECTOR_ID, loadedSchema.connectorId);
        });
    }

    @Test
    @RunOnVertxContext
    void testConnectorConfigSchema_ApicurioSyncFields(UniAsserter asserter) {
        String schemaId = UUID.randomUUID().toString();

        asserter.execute(() -> Panache.withTransaction(() -> {
            ConnectorConfigSchema s = new ConnectorConfigSchema(
                schemaId, TEST_CONNECTOR_ID, "1.0.0",
                "{}", "{}", "test-user"
            );
            return s.<ConnectorConfigSchema>persist().replaceWithVoid();
        }));

        // Update Apicurio sync fields
        asserter.assertThat(() -> Panache.withTransaction(() ->
            ConnectorConfigSchema.<ConnectorConfigSchema>findById(schemaId)
                .flatMap(schema -> {
                    schema.apicurioArtifactId = "my-artifact-id";
                    schema.apicurioGlobalId = 12345L;
                    schema.syncStatus = "SYNCED";
                    schema.lastSyncAttempt = OffsetDateTime.now();
                    return schema.<ConnectorConfigSchema>persist();
                })
        ), updated -> {
            assertNotNull(updated);
            assertEquals("my-artifact-id", updated.apicurioArtifactId);
            assertEquals(12345L, updated.apicurioGlobalId);
            assertEquals("SYNCED", updated.syncStatus);
            assertNotNull(updated.lastSyncAttempt);
        });
    }

    @Test
    @RunOnVertxContext
    void testConnectorConfigSchema_UpdateSyncError(UniAsserter asserter) {
        String schemaId = UUID.randomUUID().toString();

        asserter.execute(() -> Panache.withTransaction(() -> {
            ConnectorConfigSchema s = new ConnectorConfigSchema(
                schemaId, TEST_CONNECTOR_ID, "1.0.0",
                "{}", "{}", "test-user"
            );
            return s.<ConnectorConfigSchema>persist().replaceWithVoid();
        }));

        // Simulate sync failure
        asserter.assertThat(() -> Panache.withTransaction(() ->
            ConnectorConfigSchema.<ConnectorConfigSchema>findById(schemaId)
                .flatMap(schema -> {
                    schema.syncStatus = "FAILED";
                    schema.syncError = "Connection timeout";
                    schema.lastSyncAttempt = OffsetDateTime.now();
                    return schema.<ConnectorConfigSchema>persist();
                })
        ), updated -> {
            assertNotNull(updated);
            assertEquals("FAILED", updated.syncStatus);
            assertEquals("Connection timeout", updated.syncError);
            assertNotNull(updated.lastSyncAttempt);
        });
    }

    @Test
    @RunOnVertxContext
    void testConnectorConfigSchema_MultipleVersions(UniAsserter asserter) {
        String schemaId1 = UUID.randomUUID().toString();
        String schemaId2 = UUID.randomUUID().toString();

        // Create version 1.0.0
        asserter.execute(() -> Panache.withTransaction(() -> {
            ConnectorConfigSchema s1 = new ConnectorConfigSchema(
                schemaId1, TEST_CONNECTOR_ID, "1.0.0",
                "{}", "{}", "user"
            );
            return s1.<ConnectorConfigSchema>persist().replaceWithVoid();
        }));

        // Create version 2.0.0 for same connector
        asserter.execute(() -> Panache.withTransaction(() -> {
            ConnectorConfigSchema s2 = new ConnectorConfigSchema(
                schemaId2, TEST_CONNECTOR_ID, "2.0.0",
                "{}", "{}", "user"
            );
            return s2.<ConnectorConfigSchema>persist().replaceWithVoid();
        }));

        // Both should exist
        asserter.assertThat(() -> Panache.withSession(() ->
            ConnectorConfigSchema.<ConnectorConfigSchema>findById(schemaId1)
        ), found -> {
            assertNotNull(found);
        });
        
        asserter.assertThat(() -> Panache.withSession(() ->
            ConnectorConfigSchema.<ConnectorConfigSchema>findById(schemaId2)
        ), found -> {
            assertNotNull(found);
        });

        // Find all versions for connector
        asserter.assertThat(() -> Panache.withSession(() ->
            ConnectorConfigSchema.<ConnectorConfigSchema>find("connectorId", TEST_CONNECTOR_ID).list()
        ), allVersions -> {
            assertEquals(2, allVersions.size());
        });
    }
}
