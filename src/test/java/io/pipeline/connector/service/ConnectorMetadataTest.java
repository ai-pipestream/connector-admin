package io.pipeline.connector.service;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.pipeline.connector.entity.Connector;
import io.pipeline.connector.entity.ConnectorAccount;
import io.pipeline.connector.intake.*;
import io.pipeline.grpc.wiremock.AccountManagerMock;
import io.pipeline.grpc.wiremock.AccountManagerMockTestResource;
import io.pipeline.grpc.wiremock.InjectWireMock;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for connector metadata storage (S3 config, limits, default metadata).
 */
@QuarkusTest
@QuarkusTestResource(AccountManagerMockTestResource.class)
public class ConnectorMetadataTest {

    @GrpcClient
    MutinyConnectorAdminServiceGrpc.MutinyConnectorAdminServiceStub connectorAdminService;

    @InjectWireMock
    WireMockServer wireMockServer;

    private AccountManagerMock accountManagerMock;

    @BeforeEach
    @Transactional
    void setUp() {
        // Set up account mock
        accountManagerMock = new AccountManagerMock(wireMockServer.port());
        accountManagerMock.mockGetAccount("test-account", "Test", "Test account", true);

        // Clean up
        ConnectorAccount.deleteAll();
        Connector.deleteAll();
    }

    @Test
    void testRegisterConnector_WithS3Config() {
        var request = RegisterConnectorRequest.newBuilder()
            .setConnectorName("s3-config-test-" + System.currentTimeMillis())
            .setConnectorType("filesystem")
            .setAccountId("test-account")
            .setS3Bucket("my-bucket")
            .setS3BasePath("connectors/test/")
            .setMaxFileSize(104857600)  // 100 MB
            .setRateLimitPerMinute(1000)
            .putDefaultMetadata("source", "filesystem")
            .putDefaultMetadata("env", "test")
            .build();

        var response = connectorAdminService.registerConnector(request)
            .await().indefinitely();

        assertTrue(response.getSuccess());

        // Get the connector and verify S3 config is stored
        var connector = connectorAdminService.getConnector(
            GetConnectorRequest.newBuilder()
                .setConnectorId(response.getConnectorId())
                .build()
        ).await().indefinitely();

        assertEquals("my-bucket", connector.getS3Bucket());
        assertEquals("connectors/test/", connector.getS3BasePath());
        assertEquals(104857600, connector.getMaxFileSize());
        assertEquals(1000, connector.getRateLimitPerMinute());
        assertEquals("filesystem", connector.getDefaultMetadataMap().get("source"));
        assertEquals("test", connector.getDefaultMetadataMap().get("env"));
    }

    @Test
    void testRegisterConnector_WithoutS3Config() {
        // Register without S3 config
        var request = RegisterConnectorRequest.newBuilder()
            .setConnectorName("no-s3-test-" + System.currentTimeMillis())
            .setConnectorType("api")
            .setAccountId("test-account")
            .build();

        var response = connectorAdminService.registerConnector(request)
            .await().indefinitely();

        assertTrue(response.getSuccess());

        // Get connector - S3 fields should be empty/default
        var connector = connectorAdminService.getConnector(
            GetConnectorRequest.newBuilder()
                .setConnectorId(response.getConnectorId())
                .build()
        ).await().indefinitely();

        assertEquals("", connector.getS3Bucket());
        assertEquals("", connector.getS3BasePath());
        assertEquals(0, connector.getMaxFileSize());
        assertEquals(0, connector.getRateLimitPerMinute());
        assertTrue(connector.getDefaultMetadataMap().isEmpty());
    }

    @Test
    void testUpdateConnector_UpdateS3Config() {
        // Register connector
        var registerRequest = RegisterConnectorRequest.newBuilder()
            .setConnectorName("update-s3-test-" + System.currentTimeMillis())
            .setConnectorType("filesystem")
            .setAccountId("test-account")
            .setS3Bucket("original-bucket")
            .build();

        var registerResponse = connectorAdminService.registerConnector(registerRequest)
            .await().indefinitely();

        // Update S3 config
        var updateRequest = UpdateConnectorRequest.newBuilder()
            .setConnectorId(registerResponse.getConnectorId())
            .setConnectorName("updated-name")
            .setS3Bucket("updated-bucket")
            .setS3BasePath("new/path/")
            .build();

        var updateResponse = connectorAdminService.updateConnector(updateRequest)
            .await().indefinitely();

        assertTrue(updateResponse.getSuccess());
        assertEquals("updated-bucket", updateResponse.getConnector().getS3Bucket());
        assertEquals("new/path/", updateResponse.getConnector().getS3BasePath());
    }
}
