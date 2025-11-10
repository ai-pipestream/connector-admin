package ai.pipestream.connector.service;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.grpc.StatusRuntimeException;
import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.ConnectorAccount;
import ai.pipestream.connector.intake.*;
import ai.pipestream.grpc.wiremock.AccountManagerMock;
import ai.pipestream.grpc.wiremock.AccountManagerMockTestResource;
import ai.pipestream.grpc.wiremock.InjectWireMock;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for ConnectorAdminService gRPC API.
 * <p>
 * Tests the full stack: gRPC → Service → Repository → Database
 * with mocked account-manager via grpc-wiremock.
 */
@QuarkusTest
@QuarkusTestResource(AccountManagerMockTestResource.class)
public class ConnectorAdminServiceTest {

    @GrpcClient
    MutinyConnectorAdminServiceGrpc.MutinyConnectorAdminServiceStub connectorAdminService;

    @InjectWireMock
    WireMockServer wireMockServer;

    private AccountManagerMock accountManagerMock;

    @BeforeEach
    @Transactional
    void setUp() {
        // Set up account mocks
        accountManagerMock = new AccountManagerMock(wireMockServer.port());
        accountManagerMock.mockGetAccount("valid-account", "Valid Account", "Active account", true);
        accountManagerMock.mockGetAccount("inactive-account", "Inactive", "Inactive account", false);
        accountManagerMock.mockAccountNotFound("nonexistent");

        // Clean up test data
        ConnectorAccount.deleteAll();
        Connector.deleteAll();
    }

    @Test
    void testRegisterConnector_Success() {
        var request = RegisterConnectorRequest.newBuilder()
            .setConnectorName("test-connector-" + System.currentTimeMillis())
            .setConnectorType("filesystem")
            .setAccountId("valid-account")
            .build();

        var response = connectorAdminService.registerConnector(request)
            .await().indefinitely();

        assertTrue(response.getSuccess());
        assertNotNull(response.getConnectorId());
        assertFalse(response.getApiKey().isEmpty(), "API key should be returned");
        assertEquals("Connector registered successfully", response.getMessage());
    }

    @Test
    void testRegisterConnector_AccountNotFound() {
        var request = RegisterConnectorRequest.newBuilder()
            .setConnectorName("test-" + System.currentTimeMillis())
            .setConnectorType("filesystem")
            .setAccountId("nonexistent")
            .build();

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> {
            connectorAdminService.registerConnector(request)
                .await().indefinitely();
        });

        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("does not exist"));
    }

    @Test
    void testRegisterConnector_AccountInactive() {
        var request = RegisterConnectorRequest.newBuilder()
            .setConnectorName("test-" + System.currentTimeMillis())
            .setConnectorType("filesystem")
            .setAccountId("inactive-account")
            .build();

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> {
            connectorAdminService.registerConnector(request)
                .await().indefinitely();
        });

        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("inactive"));
    }

    @Test
    void testRegisterConnector_DuplicateName() {
        String name = "duplicate-" + System.currentTimeMillis();

        // Register first connector
        var request = RegisterConnectorRequest.newBuilder()
            .setConnectorName(name)
            .setConnectorType("filesystem")
            .setAccountId("valid-account")
            .build();

        connectorAdminService.registerConnector(request)
            .await().indefinitely();

        // Try to register with same name
        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> {
            connectorAdminService.registerConnector(request)
                .await().indefinitely();
        });

        assertEquals(io.grpc.Status.Code.ALREADY_EXISTS, exception.getStatus().getCode());
    }

    @Test
    void testGetConnector() {
        // Register connector first
        String name = "get-test-" + System.currentTimeMillis();
        var registerResponse = connectorAdminService.registerConnector(
            RegisterConnectorRequest.newBuilder()
                .setConnectorName(name)
                .setConnectorType("database")
                .setAccountId("valid-account")
                .build()
        ).await().indefinitely();

        // Get connector
        var connector = connectorAdminService.getConnector(
            GetConnectorRequest.newBuilder()
                .setConnectorId(registerResponse.getConnectorId())
                .build()
        ).await().indefinitely();

        assertEquals(registerResponse.getConnectorId(), connector.getConnectorId());
        assertEquals(name, connector.getConnectorName());
        assertEquals("database", connector.getConnectorType());
        assertEquals("valid-account", connector.getAccountId());
        assertTrue(connector.getActive());
        assertEquals("", connector.getApiKey(), "API key should never be returned");
    }

    @Test
    void testGetConnector_NotFound() {
        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> {
            connectorAdminService.getConnector(
                GetConnectorRequest.newBuilder()
                    .setConnectorId("does-not-exist")
                    .build()
            ).await().indefinitely();
        });

        assertEquals(io.grpc.Status.Code.NOT_FOUND, exception.getStatus().getCode());
    }

    @Test
    void testListConnectors_All() {
        // Register multiple connectors
        String name1 = "list-1-" + System.currentTimeMillis();
        String name2 = "list-2-" + System.currentTimeMillis();

        connectorAdminService.registerConnector(
            RegisterConnectorRequest.newBuilder()
                .setConnectorName(name1)
                .setConnectorType("filesystem")
                .setAccountId("valid-account")
                .build()
        ).await().indefinitely();

        connectorAdminService.registerConnector(
            RegisterConnectorRequest.newBuilder()
                .setConnectorName(name2)
                .setConnectorType("api")
                .setAccountId("valid-account")
                .build()
        ).await().indefinitely();

        // List all
        var response = connectorAdminService.listConnectors(
            ListConnectorsRequest.newBuilder().build()
        ).await().indefinitely();

        assertTrue(response.getConnectorsCount() >= 2);
        assertTrue(response.getTotalCount() >= 2);
    }

    @Test
    void testListConnectors_FilterByAccount() {
        // Register connector
        String name = "filter-test-" + System.currentTimeMillis();
        connectorAdminService.registerConnector(
            RegisterConnectorRequest.newBuilder()
                .setConnectorName(name)
                .setConnectorType("filesystem")
                .setAccountId("valid-account")
                .build()
        ).await().indefinitely();

        // List for specific account
        var response = connectorAdminService.listConnectors(
            ListConnectorsRequest.newBuilder()
                .setAccountId("valid-account")
                .build()
        ).await().indefinitely();

        assertTrue(response.getConnectorsCount() > 0);
        assertTrue(response.getConnectorsList().stream()
            .anyMatch(c -> c.getConnectorName().equals(name)));
    }

    @Test
    void testSetConnectorStatus() {
        // Register connector
        var registerResponse = connectorAdminService.registerConnector(
            RegisterConnectorRequest.newBuilder()
                .setConnectorName("status-test-" + System.currentTimeMillis())
                .setConnectorType("filesystem")
                .setAccountId("valid-account")
                .build()
        ).await().indefinitely();

        // Inactivate it
        var response = connectorAdminService.setConnectorStatus(
            SetConnectorStatusRequest.newBuilder()
                .setConnectorId(registerResponse.getConnectorId())
                .setActive(false)
                .setReason("Testing")
                .build()
        ).await().indefinitely();

        assertTrue(response.getSuccess());

        // Verify inactive
        var connector = connectorAdminService.getConnector(
            GetConnectorRequest.newBuilder()
                .setConnectorId(registerResponse.getConnectorId())
                .build()
        ).await().indefinitely();

        assertFalse(connector.getActive());
    }

    @Test
    void testDeleteConnector() {
        // Register connector
        var registerResponse = connectorAdminService.registerConnector(
            RegisterConnectorRequest.newBuilder()
                .setConnectorName("delete-test-" + System.currentTimeMillis())
                .setConnectorType("filesystem")
                .setAccountId("valid-account")
                .build()
        ).await().indefinitely();

        // Delete it
        var response = connectorAdminService.deleteConnector(
            DeleteConnectorRequest.newBuilder()
                .setConnectorId(registerResponse.getConnectorId())
                .setHardDelete(false)
                .build()
        ).await().indefinitely();

        assertTrue(response.getSuccess());

        // Verify deleted (soft delete - still exists but inactive)
        var connector = connectorAdminService.getConnector(
            GetConnectorRequest.newBuilder()
                .setConnectorId(registerResponse.getConnectorId())
                .build()
        ).await().indefinitely();

        assertFalse(connector.getActive());
    }

    @Test
    void testRotateApiKey() {
        // Register connector
        var registerResponse = connectorAdminService.registerConnector(
            RegisterConnectorRequest.newBuilder()
                .setConnectorName("rotate-test-" + System.currentTimeMillis())
                .setConnectorType("filesystem")
                .setAccountId("valid-account")
                .build()
        ).await().indefinitely();

        String originalApiKey = registerResponse.getApiKey();

        // Rotate API key
        var response = connectorAdminService.rotateApiKey(
            RotateApiKeyRequest.newBuilder()
                .setConnectorId(registerResponse.getConnectorId())
                .setInvalidateOldImmediately(true)
                .build()
        ).await().indefinitely();

        assertTrue(response.getSuccess());
        assertFalse(response.getNewApiKey().isEmpty());
        assertNotEquals(originalApiKey, response.getNewApiKey(), "New API key should be different");
    }

    @Test
    void testUpdateConnector() {
        // Register connector
        var registerResponse = connectorAdminService.registerConnector(
            RegisterConnectorRequest.newBuilder()
                .setConnectorName("update-test-" + System.currentTimeMillis())
                .setConnectorType("filesystem")
                .setAccountId("valid-account")
                .build()
        ).await().indefinitely();

        // Update connector name
        String newName = "updated-" + System.currentTimeMillis();
        var response = connectorAdminService.updateConnector(
            UpdateConnectorRequest.newBuilder()
                .setConnectorId(registerResponse.getConnectorId())
                .setConnectorName(newName)
                .build()
        ).await().indefinitely();

        assertTrue(response.getSuccess());
        assertEquals(newName, response.getConnector().getConnectorName());
    }
}
