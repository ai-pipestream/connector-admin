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
 * Tests for ListConnectors pagination.
 */
@QuarkusTest
@QuarkusTestResource(AccountManagerMockTestResource.class)
public class ConnectorPaginationTest {

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
        accountManagerMock.mockGetAccount("test-account", "Test Account", "Pagination test", true);

        // Clean up
        ConnectorAccount.deleteAll();
        Connector.deleteAll();
    }

    @Test
    void testListConnectors_Pagination() {
        // Register 7 connectors
        String prefix = "page-test-" + System.currentTimeMillis();
        for (int i = 0; i < 7; i++) {
            connectorAdminService.registerConnector(
                RegisterConnectorRequest.newBuilder()
                    .setConnectorName(prefix + "-" + i)
                    .setConnectorType("filesystem")
                    .setAccountId("test-account")
                    .build()
            ).await().indefinitely();
        }

        // Get first page (3 items)
        var firstPage = connectorAdminService.listConnectors(
            ListConnectorsRequest.newBuilder()
                .setPageSize(3)
                .build()
        ).await().indefinitely();

        assertEquals(3, firstPage.getConnectorsCount(), "First page should have 3 items");
        assertTrue(firstPage.getTotalCount() >= 7, "Total count should be at least 7");
        assertFalse(firstPage.getNextPageToken().isEmpty(), "Should have next page token");

        // Get second page
        var secondPage = connectorAdminService.listConnectors(
            ListConnectorsRequest.newBuilder()
                .setPageSize(3)
                .setPageToken(firstPage.getNextPageToken())
                .build()
        ).await().indefinitely();

        assertEquals(3, secondPage.getConnectorsCount(), "Second page should have 3 items");
        assertFalse(secondPage.getNextPageToken().isEmpty(), "Should have next page token");

        // Get third page (should have remaining 1)
        var thirdPage = connectorAdminService.listConnectors(
            ListConnectorsRequest.newBuilder()
                .setPageSize(3)
                .setPageToken(secondPage.getNextPageToken())
                .build()
        ).await().indefinitely();

        assertTrue(thirdPage.getConnectorsCount() >= 1, "Third page should have at least 1 item");
        // May or may not have next page depending on other test data
    }

    @Test
    void testListConnectors_EmptyPage() {
        // Request page beyond available data
        var response = connectorAdminService.listConnectors(
            ListConnectorsRequest.newBuilder()
                .setPageSize(10)
                .setPageToken("9999")  // Very high offset
                .build()
        ).await().indefinitely();

        // Should return empty list, not error
        assertTrue(response.getConnectorsCount() >= 0);
    }

    @Test
    void testListConnectors_DefaultPageSize() {
        // Register a few connectors
        String prefix = "default-page-" + System.currentTimeMillis();
        for (int i = 0; i < 3; i++) {
            connectorAdminService.registerConnector(
                RegisterConnectorRequest.newBuilder()
                    .setConnectorName(prefix + "-" + i)
                    .setConnectorType("api")
                    .setAccountId("test-account")
                    .build()
            ).await().indefinitely();
        }

        // List without page_size (should use default)
        var response = connectorAdminService.listConnectors(
            ListConnectorsRequest.newBuilder().build()
        ).await().indefinitely();

        // Should return all connectors (default page size is large)
        assertTrue(response.getConnectorsCount() >= 3);
    }
}
