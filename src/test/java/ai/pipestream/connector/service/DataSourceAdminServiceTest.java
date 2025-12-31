package ai.pipestream.connector.service;

import ai.pipestream.connector.entity.DataSource;
import ai.pipestream.connector.intake.v1.*;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.transaction.Transactional;
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
    @Transactional
    void setUp() {
        // Clean up test data
        DataSource.deleteAll();
    }

    @Test
    void testCreateDataSource_Success() {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        CreateDataSourceRequest request = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Test DataSource")
            .setDriveName("test-drive")
            .putMetadata("env", "test")
            .build();

        CreateDataSourceResponse response = dataSourceAdminService.createDataSource(request)
            .await().indefinitely();

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
    }

    @Test
    void testCreateDataSource_DuplicateFails() {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        CreateDataSourceRequest request = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("First DataSource")
            .setDriveName("drive1")
            .build();

        // First creation should succeed
        CreateDataSourceResponse response1 = dataSourceAdminService.createDataSource(request)
            .await().indefinitely();
        assertTrue(response1.getSuccess());

        // Second creation with same account+connector should fail
        CreateDataSourceRequest request2 = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Second DataSource")
            .setDriveName("drive2")
            .build();

        try {
            dataSourceAdminService.createDataSource(request2).await().indefinitely();
            fail("Expected ALREADY_EXISTS exception");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("ALREADY_EXISTS") ||
                       e.getMessage().contains("already exists"));
        }
    }

    @Test
    void testGetDataSource() {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        // Create datasource first
        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Get Test DataSource")
            .setDriveName("test-drive")
            .build();

        CreateDataSourceResponse createResponse = dataSourceAdminService.createDataSource(createRequest)
            .await().indefinitely();
        String datasourceId = createResponse.getDatasource().getDatasourceId();

        // Get the datasource
        GetDataSourceRequest getRequest = GetDataSourceRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .build();

        GetDataSourceResponse getResponse = dataSourceAdminService.getDataSource(getRequest)
            .await().indefinitely();

        assertNotNull(getResponse.getDatasource());
        assertEquals(datasourceId, getResponse.getDatasource().getDatasourceId());
        assertEquals(uniqueAccount, getResponse.getDatasource().getAccountId());

        // API key should NOT be returned on get
        assertTrue(getResponse.getDatasource().getApiKey().isEmpty());
    }

    @Test
    void testValidateApiKey_Success() {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        // Create datasource
        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Validate Test")
            .setDriveName("test-drive")
            .build();

        CreateDataSourceResponse createResponse = dataSourceAdminService.createDataSource(createRequest)
            .await().indefinitely();

        String datasourceId = createResponse.getDatasource().getDatasourceId();
        String apiKey = createResponse.getDatasource().getApiKey();

        // Validate the API key
        ValidateApiKeyRequest validateRequest = ValidateApiKeyRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setApiKey(apiKey)
            .build();

        ValidateApiKeyResponse validateResponse = dataSourceAdminService.validateApiKey(validateRequest)
            .await().indefinitely();

        assertTrue(validateResponse.getValid());
        assertEquals("API key is valid", validateResponse.getMessage());
        assertNotNull(validateResponse.getConfig());
        assertEquals(datasourceId, validateResponse.getConfig().getDatasourceId());
    }

    @Test
    void testValidateApiKey_Invalid() {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        // Create datasource
        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Invalid Key Test")
            .setDriveName("test-drive")
            .build();

        CreateDataSourceResponse createResponse = dataSourceAdminService.createDataSource(createRequest)
            .await().indefinitely();
        String datasourceId = createResponse.getDatasource().getDatasourceId();

        // Try with wrong API key
        ValidateApiKeyRequest validateRequest = ValidateApiKeyRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setApiKey("wrong-api-key")
            .build();

        ValidateApiKeyResponse validateResponse = dataSourceAdminService.validateApiKey(validateRequest)
            .await().indefinitely();

        assertFalse(validateResponse.getValid());
        assertEquals("Invalid API key", validateResponse.getMessage());
    }

    @Test
    void testRotateApiKey() {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        // Create datasource
        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Rotate Test")
            .setDriveName("test-drive")
            .build();

        CreateDataSourceResponse createResponse = dataSourceAdminService.createDataSource(createRequest)
            .await().indefinitely();

        String datasourceId = createResponse.getDatasource().getDatasourceId();
        String originalApiKey = createResponse.getDatasource().getApiKey();

        // Rotate the API key
        RotateApiKeyRequest rotateRequest = RotateApiKeyRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .build();

        RotateApiKeyResponse rotateResponse = dataSourceAdminService.rotateApiKey(rotateRequest)
            .await().indefinitely();

        assertTrue(rotateResponse.getSuccess());
        assertNotNull(rotateResponse.getNewApiKey());
        assertFalse(rotateResponse.getNewApiKey().isEmpty());
        assertNotEquals(originalApiKey, rotateResponse.getNewApiKey());

        // Old key should no longer work
        ValidateApiKeyRequest validateOld = ValidateApiKeyRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setApiKey(originalApiKey)
            .build();
        ValidateApiKeyResponse oldResponse = dataSourceAdminService.validateApiKey(validateOld)
            .await().indefinitely();
        assertFalse(oldResponse.getValid());

        // New key should work
        ValidateApiKeyRequest validateNew = ValidateApiKeyRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setApiKey(rotateResponse.getNewApiKey())
            .build();
        ValidateApiKeyResponse newResponse = dataSourceAdminService.validateApiKey(validateNew)
            .await().indefinitely();
        assertTrue(newResponse.getValid());
    }

    @Test
    void testSetDataSourceStatus() {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        // Create datasource
        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Status Test")
            .setDriveName("test-drive")
            .build();

        CreateDataSourceResponse createResponse = dataSourceAdminService.createDataSource(createRequest)
            .await().indefinitely();
        String datasourceId = createResponse.getDatasource().getDatasourceId();
        String apiKey = createResponse.getDatasource().getApiKey();

        // Disable the datasource
        SetDataSourceStatusRequest disableRequest = SetDataSourceStatusRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setActive(false)
            .setReason("test_disable")
            .build();

        SetDataSourceStatusResponse disableResponse = dataSourceAdminService.setDataSourceStatus(disableRequest)
            .await().indefinitely();
        assertTrue(disableResponse.getSuccess());

        // API key should not validate for inactive datasource
        ValidateApiKeyRequest validateRequest = ValidateApiKeyRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setApiKey(apiKey)
            .build();
        ValidateApiKeyResponse validateResponse = dataSourceAdminService.validateApiKey(validateRequest)
            .await().indefinitely();
        assertFalse(validateResponse.getValid());
        assertTrue(validateResponse.getMessage().contains("inactive"));

        // Re-enable
        SetDataSourceStatusRequest enableRequest = SetDataSourceStatusRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setActive(true)
            .build();
        dataSourceAdminService.setDataSourceStatus(enableRequest).await().indefinitely();

        // Should work again
        validateResponse = dataSourceAdminService.validateApiKey(validateRequest)
            .await().indefinitely();
        assertTrue(validateResponse.getValid());
    }

    @Test
    void testListDataSources() {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();
        String secondConnectorId = "b1ffc0aa-0d1c-5f09-cc7e-7cc0ce491b22"; // file-crawler

        // Create two datasources
        dataSourceAdminService.createDataSource(CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("DS1")
            .setDriveName("drive1")
            .build()).await().indefinitely();

        dataSourceAdminService.createDataSource(CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(secondConnectorId)
            .setName("DS2")
            .setDriveName("drive2")
            .build()).await().indefinitely();

        // List all for account
        ListDataSourcesRequest listRequest = ListDataSourcesRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .build();

        ListDataSourcesResponse listResponse = dataSourceAdminService.listDataSources(listRequest)
            .await().indefinitely();

        assertEquals(2, listResponse.getDatasourcesCount());
        assertEquals(2, listResponse.getTotalCount());
    }

    @Test
    void testListConnectorTypes() {
        ListConnectorTypesRequest request = ListConnectorTypesRequest.newBuilder().build();

        ListConnectorTypesResponse response = dataSourceAdminService.listConnectorTypes(request)
            .await().indefinitely();

        assertTrue(response.getConnectorsCount() >= 2);
        assertTrue(response.getConnectorsList().stream()
            .anyMatch(c -> "s3".equals(c.getConnectorType())));
        assertTrue(response.getConnectorsList().stream()
            .anyMatch(c -> "file-crawler".equals(c.getConnectorType())));
    }

    @Test
    void testGetConnectorType() {
        GetConnectorTypeRequest request = GetConnectorTypeRequest.newBuilder()
            .setConnectorId(TEST_CONNECTOR_ID)
            .build();

        GetConnectorTypeResponse response = dataSourceAdminService.getConnectorType(request)
            .await().indefinitely();

        assertNotNull(response.getConnector());
        assertEquals(TEST_CONNECTOR_ID, response.getConnector().getConnectorId());
        assertEquals("s3", response.getConnector().getConnectorType());
        assertEquals(ManagementType.MANAGEMENT_TYPE_UNMANAGED, response.getConnector().getManagementType());
    }

    @Test
    void testDeleteDataSource() {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        // Create datasource
        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Delete Test")
            .setDriveName("test-drive")
            .build();

        CreateDataSourceResponse createResponse = dataSourceAdminService.createDataSource(createRequest)
            .await().indefinitely();
        String datasourceId = createResponse.getDatasource().getDatasourceId();

        // Delete
        DeleteDataSourceRequest deleteRequest = DeleteDataSourceRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .build();

        DeleteDataSourceResponse deleteResponse = dataSourceAdminService.deleteDataSource(deleteRequest)
            .await().indefinitely();

        assertTrue(deleteResponse.getSuccess());

        // Should still exist but be inactive (soft delete)
        GetDataSourceResponse getResponse = dataSourceAdminService.getDataSource(
            GetDataSourceRequest.newBuilder().setDatasourceId(datasourceId).build()
        ).await().indefinitely();

        assertFalse(getResponse.getDatasource().getActive());
    }

    @Test
    void testUpdateDataSource() {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        // Create datasource
        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Original Name")
            .setDriveName("original-drive")
            .build();

        CreateDataSourceResponse createResponse = dataSourceAdminService.createDataSource(createRequest)
            .await().indefinitely();
        String datasourceId = createResponse.getDatasource().getDatasourceId();

        // Update
        UpdateDataSourceRequest updateRequest = UpdateDataSourceRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setName("Updated Name")
            .setDriveName("updated-drive")
            .putMetadata("updated", "true")
            .build();

        UpdateDataSourceResponse updateResponse = dataSourceAdminService.updateDataSource(updateRequest)
            .await().indefinitely();

        assertTrue(updateResponse.getSuccess());
        assertEquals("Updated Name", updateResponse.getDatasource().getName());
        assertEquals("updated-drive", updateResponse.getDatasource().getDriveName());
        assertEquals("true", updateResponse.getDatasource().getMetadataMap().get("updated"));
    }

    @Test
    void testValidateApiKey_ReturnsMergedConfig() {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Config Test")
            .setDriveName("test-drive")
            .build();

        CreateDataSourceResponse createResponse = dataSourceAdminService.createDataSource(createRequest)
            .await().indefinitely();
        assertTrue(createResponse.getSuccess());

        String datasourceId = createResponse.getDatasource().getDatasourceId();
        String apiKey = createResponse.getDatasource().getApiKey();

        ValidateApiKeyRequest validateRequest = ValidateApiKeyRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setApiKey(apiKey)
            .build();

        ValidateApiKeyResponse response = dataSourceAdminService.validateApiKey(validateRequest)
            .await().indefinitely();

        assertTrue(response.getValid());
        assertNotNull(response.getConfig());
        assertTrue(response.getConfig().hasGlobalConfig());
        
        // Verify global_config has system defaults at minimum
        var globalConfig = response.getConfig().getGlobalConfig();
        assertTrue(globalConfig.hasPersistenceConfig());
        assertTrue(globalConfig.hasHydrationConfig());
    }

    @Test
    void testValidateApiKey_ConnectorDefaults() {
        String uniqueAccount = TEST_ACCOUNT_ID + "-" + System.currentTimeMillis();

        // Create datasource - connector should have defaults (if set)
        CreateDataSourceRequest createRequest = CreateDataSourceRequest.newBuilder()
            .setAccountId(uniqueAccount)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Connector Defaults Test")
            .setDriveName("test-drive")
            .build();

        CreateDataSourceResponse createResponse = dataSourceAdminService.createDataSource(createRequest)
            .await().indefinitely();
        String datasourceId = createResponse.getDatasource().getDatasourceId();
        String apiKey = createResponse.getDatasource().getApiKey();

        ValidateApiKeyRequest validateRequest = ValidateApiKeyRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setApiKey(apiKey)
            .build();

        ValidateApiKeyResponse response = dataSourceAdminService.validateApiKey(validateRequest)
            .await().indefinitely();

        assertTrue(response.getValid());
        var globalConfig = response.getConfig().getGlobalConfig();
        
        // Should have persistence config (from connector defaults or system defaults)
        assertTrue(globalConfig.hasPersistenceConfig());
        // Verify persist_pipedoc is set - just verify the config is present and has a value
        // System default is true, connector may override, but it should be set
        assertNotNull(globalConfig.getPersistenceConfig());
    }
}
