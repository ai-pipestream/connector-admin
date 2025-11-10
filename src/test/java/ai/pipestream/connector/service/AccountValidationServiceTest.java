package ai.pipestream.connector.service;

import io.grpc.StatusRuntimeException;
import ai.pipestream.grpc.wiremock.AccountManagerMock;
import ai.pipestream.grpc.wiremock.AccountManagerMockTestResource;
import ai.pipestream.grpc.wiremock.InjectWireMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.github.tomakehurst.wiremock.WireMockServer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AccountValidationService with mocked account-manager.
 * <p>
 * Uses AccountManagerMockTestResource to properly configure Stork static discovery
 * and route account-manager calls to WireMock.
 */
@QuarkusTest
@QuarkusTestResource(AccountManagerMockTestResource.class)
public class AccountValidationServiceTest {

    @Inject
    AccountValidationService accountValidationService;

    @InjectWireMock
    WireMockServer wireMockServer;

    private AccountManagerMock accountManagerMock;

    @BeforeEach
    void setUp() {
        accountManagerMock = new AccountManagerMock(wireMockServer.port());
    }

    @Test
    void testValidateAccountExistsAndActive_Success() {
        // Mock active account
        accountManagerMock.mockGetAccount("valid-account", "Valid Account", "Active account", true);

        // Validate
        assertDoesNotThrow(() -> {
            accountValidationService.validateAccountExistsAndActive("valid-account")
                .await().indefinitely();
        });
    }

    @Test
    void testValidateAccountExistsAndActive_InactiveAccount() {
        // Mock inactive account
        accountManagerMock.mockGetAccount("inactive-account", "Inactive", "Inactive account", false);

        // Validate - should fail with INVALID_ARGUMENT
        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> {
            accountValidationService.validateAccountExistsAndActive("inactive-account")
                .await().indefinitely();
        });

        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("inactive"));
    }

    @Test
    void testValidateAccountExistsAndActive_NotFound() {
        // Mock account not found
        accountManagerMock.mockAccountNotFound("nonexistent");

        // Validate - should fail with INVALID_ARGUMENT
        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> {
            accountValidationService.validateAccountExistsAndActive("nonexistent")
                .await().indefinitely();
        });

        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("does not exist"));
    }

    @Test
    void testValidateAccountExists_Success() {
        // Mock any account (active or inactive)
        accountManagerMock.mockGetAccount("any-account", "Any", "Any account", false);

        // Validate - should succeed even if inactive
        assertDoesNotThrow(() -> {
            accountValidationService.validateAccountExists("any-account")
                .await().indefinitely();
        });
    }

    @Test
    void testValidateAccountExists_NotFound() {
        // Mock account not found
        accountManagerMock.mockAccountNotFound("missing");

        // Validate - should fail
        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> {
            accountValidationService.validateAccountExists("missing")
                .await().indefinitely();
        });

        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
    }
}
