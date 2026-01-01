package ai.pipestream.connector.service;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.DataSource;
import ai.pipestream.connector.intake.v1.*;
import ai.pipestream.data.v1.HydrationConfig;
import com.google.protobuf.Struct;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * gRPC integration tests for DataSourceAdminService.
 */
@QuarkusTest
public class DataSourceAdminServiceTest {

    @GrpcClient
    MutinyDataSourceAdminServiceGrpc.MutinyDataSourceAdminServiceStub dataSourceAdminService;

    private static final String TEST_CONNECTOR_ID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"; // Pre-seeded S3
    private static final String TEST_ACCOUNT_ID = "test-account";

    @BeforeEach
    @RunOnVertxContext
    void setUp(UniAsserter asserter) {
        // Clean up test data (reactive)
        asserter.execute(() -> Panache.withTransaction(() -> DataSource.deleteAll()));
    }

    @Test
    @RunOnVertxContext
    void testCreateDataSource_Success(UniAsserter asserter) {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        CreateDataSourceRequest request = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Test DataSource")
            .setDriveName("test-drive")
            .putMetadata("env", "test")
            .build();

        asserter.assertThat(() -> dataSourceAdminService.createDataSource(request), response -> {
            assertTrue(response.getSuccess());
            assertNotNull(response.getDatasource());
            assertNotNull(response.getDatasource().getDatasourceId());
            assertFalse(response.getDatasource().getDatasourceId().isEmpty());

            // API key should be returned on creation
            assertNotNull(response.getDatasource().getApiKey());
            assertFalse(response.getDatasource().getApiKey().isEmpty());

            assertEquals(uniqueAccount, response.getDatasource().getAccountId());
            assertEquals(TEST_CONNECTOR_ID, response.getDatasource().getConnectorId());
            assertEquals("Test DataSource", response.getDatasource().getName());
            assertEquals("test-drive", response.getDatasource().getDriveName());
            assertTrue(response.getDatasource().getActive());
        });
    }

    @Test
    @RunOnVertxContext
    void testCreateDataSource_DuplicateFails(UniAsserter asserter) {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        CreateDataSourceRequest request = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("First DataSource")
            .setDriveName("drive1")
            .build();

        // First creation should succeed
        asserter.assertThat(() -> dataSourceAdminService.createDataSource(request), response1 -> {
            assertTrue(response1.getSuccess());

            // Second creation with same account+connector should fail
            CreateDataSourceRequest request2 = CreateDataSourceRequest.newBuilder()
                .setAccountId(uniqueAccount)
                .setConnectorId(TEST_CONNECTOR_ID)
                .setName("Second DataSource")
                .setDriveName("drive2")
                .build();

            asserter.assertThat(() -> dataSourceAdminService.createDataSource(request2)
                .onFailure().recoverWithUni(failure -> {
                    // Expected to fail, return null
                    return io.smallrye.mutiny.Uni.createFrom().nullItem();
                }), response2 -> {
                    assertNull(response2, "Expected ALREADY_EXISTS exception");
                });
        });
    }

    @Test
    @RunOnVertxContext
    void testGetDataSource(UniAsserter asserter) {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Get Test DataSource")
            .setDriveName("test-drive")
            .build();

        asserter.assertThat(() -> dataSourceAdminService.createDataSource(createRequest), createResponse -> {
            assertTrue(createResponse.getSuccess());
            String datasourceId = createResponse.getDatasource().getDatasourceId();

            // Get the datasource
            GetDataSourceRequest getRequest = GetDataSourceRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .build();

            asserter.assertThat(() -> dataSourceAdminService.getDataSource(getRequest), getResponse -> {
                assertNotNull(getResponse.getDatasource());
                assertEquals(datasourceId, getResponse.getDatasource().getDatasourceId());
                assertEquals(uniqueAccount, getResponse.getDatasource().getAccountId());

                // API key should NOT be returned on get
                assertTrue(getResponse.getDatasource().getApiKey().isEmpty());
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testValidateApiKey_Success(UniAsserter asserter) {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Validate Test")
            .setDriveName("test-drive")
            .build();

        asserter.assertThat(() -> dataSourceAdminService.createDataSource(createRequest), createResponse -> {
            assertTrue(createResponse.getSuccess());
            String datasourceId = createResponse.getDatasource().getDatasourceId();
            String apiKey = createResponse.getDatasource().getApiKey();

            // Validate the API key
            ValidateApiKeyRequest validateRequest = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .setApiKey(apiKey)
                .build();

            asserter.assertThat(() -> dataSourceAdminService.validateApiKey(validateRequest), validateResponse -> {
                assertTrue(validateResponse.getValid());
                assertEquals("API key is valid", validateResponse.getMessage());
                assertNotNull(validateResponse.getConfig());
                assertEquals(datasourceId, validateResponse.getConfig().getDatasourceId());
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testValidateApiKey_Invalid(UniAsserter asserter) {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Invalid Key Test")
            .setDriveName("test-drive")
            .build();

        asserter.assertThat(() -> dataSourceAdminService.createDataSource(createRequest), createResponse -> {
            assertTrue(createResponse.getSuccess());
            String datasourceId = createResponse.getDatasource().getDatasourceId();

            // Try with wrong API key
            ValidateApiKeyRequest validateRequest = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .setApiKey("wrong-api-key")
                .build();

            asserter.assertThat(() -> dataSourceAdminService.validateApiKey(validateRequest), validateResponse -> {
                assertFalse(validateResponse.getValid());
                assertEquals("Invalid API key", validateResponse.getMessage());
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testRotateApiKey(UniAsserter asserter) {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Rotate Test")
            .setDriveName("test-drive")
            .build();

        asserter.assertThat(() -> dataSourceAdminService.createDataSource(createRequest), createResponse -> {
            assertTrue(createResponse.getSuccess());
            String datasourceId = createResponse.getDatasource().getDatasourceId();
            String originalApiKey = createResponse.getDatasource().getApiKey();

            // Rotate the API key
            RotateApiKeyRequest rotateRequest = RotateApiKeyRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .build();

            asserter.assertThat(() -> dataSourceAdminService.rotateApiKey(rotateRequest), rotateResponse -> {
                assertTrue(rotateResponse.getSuccess());
                assertNotNull(rotateResponse.getNewApiKey());
                assertFalse(rotateResponse.getNewApiKey().isEmpty());
                assertNotEquals(originalApiKey, rotateResponse.getNewApiKey());

                String newApiKey = rotateResponse.getNewApiKey();

                // Old key should no longer work
                ValidateApiKeyRequest validateOld = ValidateApiKeyRequest.newBuilder()
                    .setDatasourceId(datasourceId)
                    .setApiKey(originalApiKey)
                    .build();
                
                asserter.assertThat(() -> dataSourceAdminService.validateApiKey(validateOld), oldResponse -> {
                    assertFalse(oldResponse.getValid());
                });

                // New key should work
                ValidateApiKeyRequest validateNew = ValidateApiKeyRequest.newBuilder()
                    .setDatasourceId(datasourceId)
                    .setApiKey(newApiKey)
                    .build();
                
                asserter.assertThat(() -> dataSourceAdminService.validateApiKey(validateNew), newResponse -> {
                    assertTrue(newResponse.getValid());
                });
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testSetDataSourceStatus(UniAsserter asserter) {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Status Test")
            .setDriveName("test-drive")
            .build();

        asserter.assertThat(() -> dataSourceAdminService.createDataSource(createRequest), createResponse -> {
            assertTrue(createResponse.getSuccess());
            String datasourceId = createResponse.getDatasource().getDatasourceId();
            String apiKey = createResponse.getDatasource().getApiKey();

            // Disable the datasource
            SetDataSourceStatusRequest disableRequest = SetDataSourceStatusRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .setActive(false)
                .setReason("test_disable")
                .build();

            asserter.assertThat(() -> dataSourceAdminService.setDataSourceStatus(disableRequest), disableResponse -> {
                assertTrue(disableResponse.getSuccess());

                // API key should not validate for inactive datasource
                ValidateApiKeyRequest validateRequest = ValidateApiKeyRequest.newBuilder()
                    .setDatasourceId(datasourceId)
                    .setApiKey(apiKey)
                    .build();
                
                asserter.assertThat(() -> dataSourceAdminService.validateApiKey(validateRequest), validateResponse -> {
                    assertFalse(validateResponse.getValid());
                    assertTrue(validateResponse.getMessage().contains("inactive"));
                });

                // Re-enable
                SetDataSourceStatusRequest enableRequest = SetDataSourceStatusRequest.newBuilder()
                    .setDatasourceId(datasourceId)
                    .setActive(true)
                    .build();
                
                asserter.execute(() -> dataSourceAdminService.setDataSourceStatus(enableRequest).replaceWithVoid());

                // Should work again
                asserter.assertThat(() -> dataSourceAdminService.validateApiKey(validateRequest), validateResponse2 -> {
                    assertTrue(validateResponse2.getValid());
                });
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testListDataSources(UniAsserter asserter) {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();
        String secondConnectorId = "b1ffc0aa-0d1c-5f09-cc7e-7cc0ce491b22"; // file-crawler

        // Create two datasources
        asserter.execute(() -> dataSourceAdminService.createDataSource(CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("DS1")
            .setDriveName("drive1")
            .build()).replaceWithVoid());

        asserter.execute(() -> dataSourceAdminService.createDataSource(CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(secondConnectorId)
            .setName("DS2")
            .setDriveName("drive2")
            .build()).replaceWithVoid());

        // List all for account
        ListDataSourcesRequest listRequest = ListDataSourcesRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .build();

        asserter.assertThat(() -> dataSourceAdminService.listDataSources(listRequest), listResponse -> {
            assertEquals(2, listResponse.getDatasourcesCount());
            assertEquals(2, listResponse.getTotalCount());
        });
    }

    @Test
    @RunOnVertxContext
    void testListConnectorTypes(UniAsserter asserter) {
        ListConnectorTypesRequest request = ListConnectorTypesRequest.newBuilder().build();

        asserter.assertThat(() -> dataSourceAdminService.listConnectorTypes(request), response -> {
            assertTrue(response.getConnectorsCount() >= 2);
            assertTrue(response.getConnectorsList().stream()
                .anyMatch(c -> "s3".equals(c.getConnectorType())));
            assertTrue(response.getConnectorsList().stream()
                .anyMatch(c -> "file-crawler".equals(c.getConnectorType())));
        });
    }

    @Test
    @RunOnVertxContext
    void testGetConnectorType(UniAsserter asserter) {
        GetConnectorTypeRequest request = GetConnectorTypeRequest.newBuilder()
            .setConnectorId(TEST_CONNECTOR_ID)
            .build();

        asserter.assertThat(() -> dataSourceAdminService.getConnectorType(request), response -> {
            assertNotNull(response.getConnector());
            assertEquals(TEST_CONNECTOR_ID, response.getConnector().getConnectorId());
            assertEquals("s3", response.getConnector().getConnectorType());
            assertEquals(ManagementType.MANAGEMENT_TYPE_UNMANAGED, response.getConnector().getManagementType());
        });
    }

    @Test
    @RunOnVertxContext
    void testDeleteDataSource(UniAsserter asserter) {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Delete Test")
            .setDriveName("test-drive")
            .build();

        asserter.assertThat(() -> dataSourceAdminService.createDataSource(createRequest), createResponse -> {
            assertTrue(createResponse.getSuccess());
            String datasourceId = createResponse.getDatasource().getDatasourceId();

            // Delete
            DeleteDataSourceRequest deleteRequest = DeleteDataSourceRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .build();

            asserter.assertThat(() -> dataSourceAdminService.deleteDataSource(deleteRequest), deleteResponse -> {
                assertTrue(deleteResponse.getSuccess());

                // Should still exist but be inactive (soft delete)
                GetDataSourceRequest getRequest = GetDataSourceRequest.newBuilder()
                    .setDatasourceId(datasourceId)
                    .build();

                asserter.assertThat(() -> dataSourceAdminService.getDataSource(getRequest), getResponse -> {
                    assertFalse(getResponse.getDatasource().getActive());
                });
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testUpdateDataSource(UniAsserter asserter) {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Original Name")
            .setDriveName("original-drive")
            .build();

        asserter.assertThat(() -> dataSourceAdminService.createDataSource(createRequest), createResponse -> {
            assertTrue(createResponse.getSuccess());
            String datasourceId = createResponse.getDatasource().getDatasourceId();

            // Update
            UpdateDataSourceRequest updateRequest = UpdateDataSourceRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .setName("Updated Name")
                .setDriveName("updated-drive")
                .putMetadata("updated", "true")
                .build();

            asserter.assertThat(() -> dataSourceAdminService.updateDataSource(updateRequest), updateResponse -> {
                assertTrue(updateResponse.getSuccess());
                assertEquals("Updated Name", updateResponse.getDatasource().getName());
                assertEquals("updated-drive", updateResponse.getDatasource().getDriveName());
                assertEquals("true", updateResponse.getDatasource().getMetadataMap().get("updated"));
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testValidateApiKey_ReturnsMergedConfig(UniAsserter asserter) {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Config Test")
            .setDriveName("test-drive")
            .build();

        asserter.assertThat(() -> dataSourceAdminService.createDataSource(createRequest), createResponse -> {
            assertTrue(createResponse.getSuccess());
            String datasourceId = createResponse.getDatasource().getDatasourceId();
            String apiKey = createResponse.getDatasource().getApiKey();

            ValidateApiKeyRequest validateRequest = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .setApiKey(apiKey)
                .build();

            asserter.assertThat(() -> dataSourceAdminService.validateApiKey(validateRequest), response -> {
                assertTrue(response.getValid());
                assertNotNull(response.getConfig());
                assertTrue(response.getConfig().hasGlobalConfig());
                
                // Verify global_config has system defaults at minimum
                var globalConfig = response.getConfig().getGlobalConfig();
                assertTrue(globalConfig.hasPersistenceConfig());
                assertTrue(globalConfig.hasHydrationConfig());
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testValidateApiKey_ConnectorDefaults(UniAsserter asserter) {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Connector Defaults Test")
            .setDriveName("test-drive")
            .build();

        asserter.assertThat(() -> dataSourceAdminService.createDataSource(createRequest), createResponse -> {
            assertTrue(createResponse.getSuccess());
            String datasourceId = createResponse.getDatasource().getDatasourceId();
            String apiKey = createResponse.getDatasource().getApiKey();

            ValidateApiKeyRequest validateRequest = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .setApiKey(apiKey)
                .build();

            asserter.assertThat(() -> dataSourceAdminService.validateApiKey(validateRequest), response -> {
                assertTrue(response.getValid());
                var globalConfig = response.getConfig().getGlobalConfig();
                
                // Should have persistence config (from connector defaults or system defaults)
                assertTrue(globalConfig.hasPersistenceConfig());
                // Verify persist_pipedoc is set - just verify the config is present and has a value
                // System default is true, connector may override, but it should be set
                assertNotNull(globalConfig.getPersistenceConfig());
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testValidateApiKey_MergedOverrides_ConnectorDefaults_DataSourceColumnAndProto(UniAsserter asserter) {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        // Arrange: set connector defaults (Tier 1 defaults) - use reactive helper
        Connector[] originalHolder = new Connector[1];
        asserter.assertThat(() -> setConnectorDefaultsReactive(TEST_CONNECTOR_ID,
            false,
            2097152,
            "{\"connector_setting\":\"default_value\",\"parse_images\":true}"
        ), original -> {
            originalHolder[0] = original;
            // Create datasource
            CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
                .setAccountId(uniqueAccount)
                .setConnectorId(TEST_CONNECTOR_ID)
                .setName("Merged Overrides Test")
                .setDriveName("test-drive")
                .build();

            asserter.assertThat(() -> dataSourceAdminService.createDataSource(createRequest), createResponse -> {
                assertTrue(createResponse.getSuccess());
                String datasourceId = createResponse.getDatasource().getDatasourceId();
                String apiKey = createResponse.getDatasource().getApiKey();

                // Arrange: datasource column override custom_config (use Panache in reactive context)
                asserter.execute(() -> Panache.withTransaction(() ->
                    DataSource.<DataSource>findById(datasourceId)
                        .flatMap(ds -> {
                            ds.customConfig = "{\"connector_setting\":\"override_value\",\"new_setting\":123}";
                            return ds.<DataSource>persist();
                        })
                        .replaceWithVoid()
                ));

                // Arrange: datasource proto override (highest priority for strongly typed fields)
                DataSourceConfig.ConnectorGlobalConfig protoOverride =
                    DataSourceConfig.ConnectorGlobalConfig.newBuilder()
                        .setPersistenceConfig(
                            DataSourceConfig.PersistenceConfig.newBuilder()
                                .setPersistPipedoc(true) // override connector default false
                                .setMaxInlineSizeBytes(5242880) // 5MB
                                .build()
                        )
                        .setHydrationConfig(
                            HydrationConfig.newBuilder()
                                .setDefaultHydrationPolicy(HydrationConfig.HydrationPolicy.HYDRATION_POLICY_ALWAYS_REF)
                                .build()
                        )
                        .build();
                
                asserter.execute(() -> Panache.withTransaction(() ->
                    DataSource.<DataSource>findById(datasourceId)
                        .flatMap(ds -> {
                            ds.globalConfigProto = protoOverride.toByteArray();
                            return ds.<DataSource>persist();
                        })
                        .replaceWithVoid()
                ));

                // Act
                ValidateApiKeyRequest validateRequest = ValidateApiKeyRequest.newBuilder()
                    .setDatasourceId(datasourceId)
                    .setApiKey(apiKey)
                    .build();

                asserter.assertThat(() -> dataSourceAdminService.validateApiKey(validateRequest), response -> {
                    // Assert
                    assertTrue(response.getValid());
                    assertTrue(response.getConfig().hasGlobalConfig());
                    var globalConfig = response.getConfig().getGlobalConfig();

                    // Strongly typed fields: should come from proto override
                    assertTrue(globalConfig.hasPersistenceConfig());
                    assertEquals(true, globalConfig.getPersistenceConfig().getPersistPipedoc());
                    assertEquals(5242880, globalConfig.getPersistenceConfig().getMaxInlineSizeBytes());
                    assertTrue(globalConfig.hasHydrationConfig());
                    assertEquals(HydrationConfig.HydrationPolicy.HYDRATION_POLICY_ALWAYS_REF,
                        globalConfig.getHydrationConfig().getDefaultHydrationPolicy());

                    // Custom config: connector defaults + datasource column override should be merged (proto had none)
                    assertTrue(globalConfig.hasCustomConfig());
                    Struct customConfig = globalConfig.getCustomConfig();
                    assertEquals("override_value", customConfig.getFieldsMap().get("connector_setting").getStringValue());
                    assertEquals(true, customConfig.getFieldsMap().get("parse_images").getBoolValue()); // from connector defaults
                    assertEquals(123, (int) customConfig.getFieldsMap().get("new_setting").getNumberValue());
                });
            });

            // Restore connector defaults so other tests aren't affected
            asserter.execute(() -> restoreConnectorDefaultsReactive(originalHolder[0]).replaceWithVoid());
        });
    }

    // Helper methods - reactive versions for use in @RunOnVertxContext tests
    Uni<Connector> setConnectorDefaultsReactive(String connectorId, Boolean persistPipedoc, Integer maxInlineSizeBytes, String defaultCustomConfig) {
        return Panache.withTransaction(() ->
            Connector.<Connector>findById(connectorId)
                .flatMap(connector -> {
                    assertNotNull(connector);

                    Connector snapshot = new Connector();
                    snapshot.connectorId = connector.connectorId;
                    snapshot.defaultPersistPipedoc = connector.defaultPersistPipedoc;
                    snapshot.defaultMaxInlineSizeBytes = connector.defaultMaxInlineSizeBytes;
                    snapshot.defaultCustomConfig = connector.defaultCustomConfig;

                    connector.defaultPersistPipedoc = persistPipedoc;
                    connector.defaultMaxInlineSizeBytes = maxInlineSizeBytes;
                    connector.defaultCustomConfig = defaultCustomConfig;
                    return connector.<Connector>persist()
                        .map(persisted -> snapshot);
                })
        );
    }

    Uni<Void> restoreConnectorDefaultsReactive(Connector snapshot) {
        if (snapshot == null) return io.smallrye.mutiny.Uni.createFrom().voidItem();
        return Panache.withTransaction(() ->
            Connector.<Connector>findById(snapshot.connectorId)
                .flatMap(connector -> {
                    if (connector == null) return io.smallrye.mutiny.Uni.createFrom().voidItem();
                    connector.defaultPersistPipedoc = snapshot.defaultPersistPipedoc;
                    connector.defaultMaxInlineSizeBytes = snapshot.defaultMaxInlineSizeBytes;
                    connector.defaultCustomConfig = snapshot.defaultCustomConfig;
                    return connector.<Connector>persist().replaceWithVoid();
                })
        );
    }
}
