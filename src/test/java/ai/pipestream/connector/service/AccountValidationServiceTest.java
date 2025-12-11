package ai.pipestream.connector.service;

import io.grpc.StatusRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AccountValidationService using stub mode.
 * <p>
 * Uses built-in stub validation that emulates account service responses without
 * requiring external gRPC calls or containers.
 */
@QuarkusTest
public class AccountValidationServiceTest {

    @Inject
    AccountValidationService accountValidationService;

    @Test
    void testValidateAccountExistsAndActive_Success() {
        // Uses default "default-account" mock from WireMock container (active account)
        assertDoesNotThrow(() -> {
            accountValidationService.validateAccountExistsAndActive("default-account")
                .await().indefinitely();
        });
    }

    @Test
    void testValidateAccountExistsAndActive_InactiveAccount() {
        // Test stub mode - should work without any external gRPC calls
        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> {
            accountValidationService.validateAccountExistsAndActive("inactive-account")
                .await().indefinitely();
        });

        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("inactive"));
    }

    @Test
    void testValidateAccountExistsAndActive_NotFound() {
        // Uses "nonexistent" NOT_FOUND mock from WireMock container
        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> {
            accountValidationService.validateAccountExistsAndActive("nonexistent")
                .await().indefinitely();
        });

        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("does not exist"));
    }

    @Test
    void testValidateAccountExists_Success() {
        // Uses "inactive-account" mock - should succeed even if inactive
        assertDoesNotThrow(() -> {
            accountValidationService.validateAccountExists("inactive-account")
                .await().indefinitely();
        });
    }

    @Test
    void testValidateAccountExists_NotFound() {
        // Uses "nonexistent" NOT_FOUND mock
        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> {
            accountValidationService.validateAccountExists("nonexistent")
                .await().indefinitely();
        });

        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
    }
}
