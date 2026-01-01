package ai.pipestream.connector.service;

import io.grpc.StatusRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
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
    @RunOnVertxContext
    void testValidateAccountExistsAndActive_Success(UniAsserter asserter) {
        // Uses default "default-account" mock from WireMock container (active account)
        asserter.assertThat(() -> 
            accountValidationService.validateAccountExistsAndActive("default-account")
                .map(v -> "SUCCESS"), // Convert Uni<Void> to Uni<String> for assertion
            result -> {
                // Validation succeeds, no exception thrown
                assertEquals("SUCCESS", result);
            }
        );
    }

    @Test
    @RunOnVertxContext
    void testValidateAccountExistsAndActive_InactiveAccount(UniAsserter asserter) {
        // Test stub mode - should work without any external gRPC calls
        asserter.assertThat(() -> 
            accountValidationService.validateAccountExistsAndActive("inactive-account")
                .map(v -> null) // This won't execute if it fails
                .onFailure().recoverWithItem(failure -> {
                    // Expected StatusRuntimeException for inactive account
                    if (failure instanceof StatusRuntimeException) {
                        StatusRuntimeException ex = (StatusRuntimeException) failure;
                        // Return a marker string with exception details for assertion
                        return "EXCEPTION:" + ex.getStatus().getCode() + ":" + ex.getMessage();
                    }
                    // Re-throw unexpected exceptions
                    return "UNEXPECTED:" + failure.getClass().getSimpleName();
                }),
            result -> {
                assertNotNull(result);
                String resultStr = (String) result;
                assertTrue(resultStr.startsWith("EXCEPTION:"));
                assertTrue(resultStr.contains("INVALID_ARGUMENT"));
                assertTrue(resultStr.contains("inactive"));
            }
        );
    }

    @Test
    @RunOnVertxContext
    void testValidateAccountExistsAndActive_NotFound(UniAsserter asserter) {
        // Uses "nonexistent" NOT_FOUND mock from WireMock container
        asserter.assertThat(() -> 
            accountValidationService.validateAccountExistsAndActive("nonexistent")
                .map(v -> null) // This won't execute if it fails
                .onFailure().recoverWithItem(failure -> {
                    // Expected StatusRuntimeException for not found
                    if (failure instanceof StatusRuntimeException) {
                        StatusRuntimeException ex = (StatusRuntimeException) failure;
                        return "EXCEPTION:" + ex.getStatus().getCode() + ":" + ex.getMessage();
                    }
                    return "UNEXPECTED:" + failure.getClass().getSimpleName();
                }),
            result -> {
                assertNotNull(result);
                String resultStr = (String) result;
                assertTrue(resultStr.startsWith("EXCEPTION:"));
                assertTrue(resultStr.contains("INVALID_ARGUMENT"));
                assertTrue(resultStr.contains("does not exist"));
            }
        );
    }

    @Test
    @RunOnVertxContext
    void testValidateAccountExists_Success(UniAsserter asserter) {
        // Uses "inactive-account" mock - should succeed even if inactive
        asserter.assertThat(() -> 
            accountValidationService.validateAccountExists("inactive-account")
                .map(v -> "SUCCESS"), // Convert Uni<Void> to Uni<String> for assertion
            result -> {
                // Validation succeeds, no exception thrown
                assertEquals("SUCCESS", result);
            }
        );
    }

    @Test
    @RunOnVertxContext
    void testValidateAccountExists_NotFound(UniAsserter asserter) {
        // Uses "nonexistent" NOT_FOUND mock
        asserter.assertThat(() -> 
            accountValidationService.validateAccountExists("nonexistent")
                .map(v -> null) // This won't execute if it fails
                .onFailure().recoverWithItem(failure -> {
                    // Expected StatusRuntimeException for not found
                    if (failure instanceof StatusRuntimeException) {
                        StatusRuntimeException ex = (StatusRuntimeException) failure;
                        return "EXCEPTION:" + ex.getStatus().getCode() + ":" + ex.getMessage();
                    }
                    return "UNEXPECTED:" + failure.getClass().getSimpleName();
                }),
            result -> {
                assertNotNull(result);
                String resultStr = (String) result;
                assertTrue(resultStr.startsWith("EXCEPTION:"));
                assertTrue(resultStr.contains("INVALID_ARGUMENT"));
            }
        );
    }
}
