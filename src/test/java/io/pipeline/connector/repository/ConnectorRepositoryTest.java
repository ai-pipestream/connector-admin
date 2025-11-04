package io.pipeline.connector.repository;

import io.pipeline.connector.entity.Connector;
import io.pipeline.connector.entity.ConnectorAccount;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Repository CRUD tests for Connector entity against MySQL.
 * Tests the repository layer in isolation without gRPC.
 */
@QuarkusTest
public class ConnectorRepositoryTest {

    @Inject
    ConnectorRepository connectorRepository;

    @BeforeEach
    @Transactional
    void cleanupBefore() {
        // Clean up test data from previous runs
        ConnectorAccount.deleteAll();
        Connector.deleteAll();
    }

    @Test
    void testCreateConnector() {
        // Create connector
        String name = "test-connector-" + System.currentTimeMillis();
        Connector connector = connectorRepository.createConnector(
            name,
            "filesystem",
            "Test connector",
            "hashed-api-key-123"
        );

        // Verify
        assertNotNull(connector);
        assertNotNull(connector.connectorId);
        assertEquals(name, connector.connectorName);
        assertEquals("filesystem", connector.connectorType);
        assertEquals("Test connector", connector.description);
        assertEquals("hashed-api-key-123", connector.apiKeyHash);
        assertTrue(connector.active);
        assertNotNull(connector.createdAt);
        assertNotNull(connector.updatedAt);
        assertNull(connector.lastRotatedAt);
        assertEquals("{}", connector.metadata);
    }

    @Test
    void testFindByConnectorId() {
        // Create connector
        String name = "test-find-id-" + System.currentTimeMillis();
        Connector created = connectorRepository.createConnector(name, "database", "Test", "hash");

        // Find by ID
        Connector found = connectorRepository.findByConnectorId(created.connectorId);

        // Verify
        assertNotNull(found);
        assertEquals(created.connectorId, found.connectorId);
        assertEquals(name, found.connectorName);
        assertEquals("database", found.connectorType);
    }

    @Test
    void testFindByConnectorName() {
        // Create connector
        String name = "test-find-name-" + System.currentTimeMillis();
        connectorRepository.createConnector(name, "api", "Test", "hash");

        // Find by name
        Connector found = connectorRepository.findByConnectorName(name);

        // Verify
        assertNotNull(found);
        assertEquals(name, found.connectorName);
        assertEquals("api", found.connectorType);
    }

    @Test
    void testFindByConnectorName_NotFound() {
        Connector notFound = connectorRepository.findByConnectorName("does-not-exist");
        assertNull(notFound);
    }

    @Test
    void testListConnectors_All() {
        // Create multiple connectors
        connectorRepository.createConnector("connector-1-" + System.currentTimeMillis(), "filesystem", "First", "hash1");
        connectorRepository.createConnector("connector-2-" + System.currentTimeMillis(), "database", "Second", "hash2");

        // List all
        List<Connector> all = connectorRepository.listConnectors(null, true, 0, 0);

        // Verify
        assertTrue(all.size() >= 2);
    }

    @Test
    void testListConnectors_ActiveOnly() {
        // Create active and inactive connectors
        String activeName = "active-" + System.currentTimeMillis();
        String inactiveName = "inactive-" + System.currentTimeMillis();

        Connector active = connectorRepository.createConnector(activeName, "filesystem", "Active", "hash1");
        Connector inactive = connectorRepository.createConnector(inactiveName, "database", "Inactive", "hash2");

        // Inactivate one
        connectorRepository.setConnectorStatus(inactive.connectorId, false, "Test");

        // List active only
        List<Connector> activeList = connectorRepository.listConnectors(null, false, 0, 0);

        // Verify active connector is in list
        assertTrue(activeList.stream().anyMatch(c -> c.connectorName.equals(activeName)));
        // Verify inactive connector is NOT in list
        assertFalse(activeList.stream().anyMatch(c -> c.connectorName.equals(inactiveName)));
    }

    @Test
    void testSetConnectorStatus() {
        // Create connector
        String name = "test-status-" + System.currentTimeMillis();
        Connector connector = connectorRepository.createConnector(name, "filesystem", "Test", "hash");
        assertTrue(connector.active);

        // Inactivate
        boolean success = connectorRepository.setConnectorStatus(connector.connectorId, false, "Testing");
        assertTrue(success);

        // Verify inactive
        Connector updated = connectorRepository.findByConnectorId(connector.connectorId);
        assertFalse(updated.active);
        assertTrue(updated.updatedAt.isAfter(connector.createdAt));
    }

    @Test
    void testSetConnectorStatus_Idempotent() {
        // Create and inactivate connector
        String name = "test-idempotent-status-" + System.currentTimeMillis();
        Connector connector = connectorRepository.createConnector(name, "filesystem", "Test", "hash");
        connectorRepository.setConnectorStatus(connector.connectorId, false, "First");

        // Inactivate again
        boolean secondSuccess = connectorRepository.setConnectorStatus(connector.connectorId, false, "Second");

        // Verify still succeeds
        assertTrue(secondSuccess);

        // Verify still inactive
        Connector updated = connectorRepository.findByConnectorId(connector.connectorId);
        assertFalse(updated.active);
    }

    @Test
    void testSetConnectorStatus_NotFound() {
        boolean success = connectorRepository.setConnectorStatus("does-not-exist", false, "Test");
        assertFalse(success);
    }

    @Test
    void testRotateApiKey() {
        // Create connector
        String name = "test-rotate-" + System.currentTimeMillis();
        Connector connector = connectorRepository.createConnector(name, "filesystem", "Test", "old-hash");
        assertNull(connector.lastRotatedAt);

        // Rotate key
        boolean success = connectorRepository.rotateApiKey(connector.connectorId, "new-hash");
        assertTrue(success);

        // Verify
        Connector updated = connectorRepository.findByConnectorId(connector.connectorId);
        assertEquals("new-hash", updated.apiKeyHash);
        assertNotNull(updated.lastRotatedAt);
        assertTrue(updated.updatedAt.isAfter(connector.createdAt));
    }

    @Test
    void testRotateApiKey_NotFound() {
        boolean success = connectorRepository.rotateApiKey("does-not-exist", "new-hash");
        assertFalse(success);
    }

    @Test
    void testLinkConnectorToAccount() {
        // Create connector
        String name = "test-link-" + System.currentTimeMillis();
        Connector connector = connectorRepository.createConnector(name, "filesystem", "Test", "hash");

        // Link to account
        String accountId = "test-account-123";
        connectorRepository.linkConnectorToAccount(connector.connectorId, accountId);

        // Verify link exists
        List<String> linkedAccounts = connectorRepository.getLinkedAccounts(connector.connectorId);
        assertTrue(linkedAccounts.contains(accountId));
    }

    @Test
    void testLinkConnectorToAccount_Idempotent() {
        // Create connector
        String name = "test-link-idempotent-" + System.currentTimeMillis();
        Connector connector = connectorRepository.createConnector(name, "filesystem", "Test", "hash");
        String accountId = "test-account-456";

        // Link twice
        connectorRepository.linkConnectorToAccount(connector.connectorId, accountId);
        connectorRepository.linkConnectorToAccount(connector.connectorId, accountId);

        // Verify only one link
        List<String> linkedAccounts = connectorRepository.getLinkedAccounts(connector.connectorId);
        assertEquals(1, linkedAccounts.stream().filter(a -> a.equals(accountId)).count());
    }

    @Test
    void testUnlinkConnectorFromAccount() {
        // Create connector and link
        String name = "test-unlink-" + System.currentTimeMillis();
        Connector connector = connectorRepository.createConnector(name, "filesystem", "Test", "hash");
        String accountId = "test-account-789";
        connectorRepository.linkConnectorToAccount(connector.connectorId, accountId);

        // Unlink
        boolean success = connectorRepository.unlinkConnectorFromAccount(connector.connectorId, accountId);
        assertTrue(success);

        // Verify link removed
        List<String> linkedAccounts = connectorRepository.getLinkedAccounts(connector.connectorId);
        assertFalse(linkedAccounts.contains(accountId));
    }

    @Test
    void testUnlinkConnectorFromAccount_NotFound() {
        boolean success = connectorRepository.unlinkConnectorFromAccount("does-not-exist", "account-123");
        assertFalse(success);
    }

    @Test
    void testGetLinkedAccounts_Multiple() {
        // Create connector
        String name = "test-multi-link-" + System.currentTimeMillis();
        Connector connector = connectorRepository.createConnector(name, "filesystem", "Test", "hash");

        // Link to multiple accounts
        connectorRepository.linkConnectorToAccount(connector.connectorId, "account-1");
        connectorRepository.linkConnectorToAccount(connector.connectorId, "account-2");
        connectorRepository.linkConnectorToAccount(connector.connectorId, "account-3");

        // Verify all links
        List<String> linkedAccounts = connectorRepository.getLinkedAccounts(connector.connectorId);
        assertEquals(3, linkedAccounts.size());
        assertTrue(linkedAccounts.contains("account-1"));
        assertTrue(linkedAccounts.contains("account-2"));
        assertTrue(linkedAccounts.contains("account-3"));
    }

    @Test
    void testListConnectors_FilteredByAccount() {
        // Create connectors
        String name1 = "connector-acct1-" + System.currentTimeMillis();
        String name2 = "connector-acct2-" + System.currentTimeMillis();

        Connector c1 = connectorRepository.createConnector(name1, "filesystem", "For account 1", "hash1");
        Connector c2 = connectorRepository.createConnector(name2, "database", "For account 2", "hash2");

        // Link to different accounts
        connectorRepository.linkConnectorToAccount(c1.connectorId, "account-1");
        connectorRepository.linkConnectorToAccount(c2.connectorId, "account-2");

        // List connectors for account-1
        List<Connector> account1Connectors = connectorRepository.listConnectors("account-1", true, 0, 0);

        // Verify only c1 is returned
        assertEquals(1, account1Connectors.size());
        assertEquals(c1.connectorId, account1Connectors.get(0).connectorId);
    }

    @Test
    void testDeleteConnector() {
        // Create connector
        String name = "test-delete-" + System.currentTimeMillis();
        Connector connector = connectorRepository.createConnector(name, "filesystem", "Test", "hash");

        // Delete (soft delete - sets active=false)
        boolean success = connectorRepository.deleteConnector(connector.connectorId, "Test deletion");
        assertTrue(success);

        // Verify inactive
        Connector deleted = connectorRepository.findByConnectorId(connector.connectorId);
        assertNotNull(deleted); // Still exists
        assertFalse(deleted.active); // But inactive
    }
}
