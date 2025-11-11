package ai.pipestream.connector.util;

import com.password4j.Argon2Function;
import com.password4j.Hash;
import com.password4j.Password;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.security.SecureRandom;
import java.util.Base64;

/**
 * Utility class for secure API key generation, hashing, and verification.
 * <p>
 * This class implements the security-critical operations for connector API key management,
 * using industry best practices and OWASP-recommended algorithms as of 2024.
 *
 * <h2>Security Architecture</h2>
 * <ul>
 *   <li><b>Generation</b>: Cryptographically secure random 256-bit keys (Base64 URL-encoded)</li>
 *   <li><b>Hashing</b>: Argon2id algorithm (winner of Password Hashing Competition)</li>
 *   <li><b>Storage</b>: Only hashes stored; plaintext keys NEVER persisted</li>
 *   <li><b>Verification</b>: Constant-time comparison to prevent timing attacks</li>
 *   <li><b>Exposure</b>: Plaintext returned ONLY ONCE at creation/rotation</li>
 * </ul>
 *
 * <h2>Argon2id Configuration</h2>
 * The implementation uses the following parameters, aligned with OWASP recommendations for 2024:
 * <pre>
 * Algorithm:     Argon2id (hybrid mode - combines Argon2i and Argon2d)
 * Memory Cost:   65536 KB (64 MB) - memory-hard to resist GPU attacks
 * Iterations:    3 (time cost)
 * Parallelism:   4 threads
 * Hash Length:   32 bytes (256 bits)
 * Salt:          Automatically generated per hash (random)
 * Output Format: PHC string format ($argon2id$v=19$...)
 * </pre>
 *
 * <h2>Security Properties</h2>
 * <ul>
 *   <li><b>GPU Resistance</b>: High memory cost makes GPU cracking impractical</li>
 *   <li><b>Side-Channel Protection</b>: Argon2id hybrid mode resists cache-timing attacks</li>
 *   <li><b>Timing Attack Prevention</b>: Constant-time verification prevents key extraction</li>
 *   <li><b>Forward Secrecy</b>: Each hash uses unique random salt</li>
 *   <li><b>Future-Proof</b>: PHC format allows algorithm migration without breaking existing hashes</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>
 * // At connector registration
 * String apiKey = apiKeyUtil.generateApiKey();
 * String hash = apiKeyUtil.hashApiKey(apiKey);
 * connector.setApiKeyHash(hash);
 * // Return apiKey to user ONCE; it cannot be retrieved later
 *
 * // At connector authentication
 * boolean valid = apiKeyUtil.verifyApiKey(providedKey, connector.getApiKeyHash());
 * if (valid) {
 *     // Proceed with authentication
 * }
 * </pre>
 *
 * <h2>Performance Characteristics</h2>
 * Argon2id with these parameters takes approximately 50-100ms per operation on modern hardware.
 * This deliberate slowness protects against brute-force attacks while remaining acceptable for
 * authentication use cases (not high-frequency operations).
 *
 * <h2>References</h2>
 * <ul>
 *   <li>OWASP Password Storage Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html</li>
 *   <li>Argon2 RFC 9106: https://www.rfc-editor.org/rfc/rfc9106.html</li>
 *   <li>Password Hashing Competition: https://password-hashing.net/</li>
 * </ul>
 *
 * @see ai.pipestream.connector.entity.Connector#apiKeyHash
 * @see ai.pipestream.connector.service.ConnectorAdminServiceImpl#registerConnector
 * @see ai.pipestream.connector.service.ConnectorAdminServiceImpl#rotateApiKey
 * @since 1.0.0
 */
@ApplicationScoped
public class ApiKeyUtil {

    private static final Logger LOG = Logger.getLogger(ApiKeyUtil.class);
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final int API_KEY_BYTES = 32; // 256 bits

    // Argon2id parameters (balanced security/performance for API keys)
    // These match OWASP recommendations for 2024
    private static final int MEMORY_COST = 65536;  // 64 MB
    private static final int ITERATIONS = 3;        // Time cost
    private static final int PARALLELISM = 4;       // Number of threads
    private static final int HASH_LENGTH = 32;      // 256 bits output

    private static final Argon2Function ARGON2 = Argon2Function.getInstance(
        MEMORY_COST,
        ITERATIONS,
        PARALLELISM,
        HASH_LENGTH,
        com.password4j.types.Argon2.ID  // Argon2id variant (hybrid mode)
    );

    /**
     * Generate a new secure random API key.
     * <p>
     * Generates a cryptographically secure random byte array and encodes
     * it as Base64. The result is URL-safe and suitable for HTTP headers.
     *
     * @return Base64-encoded API key (plaintext - must be hashed before storage)
     */
    public String generateApiKey() {
        byte[] randomBytes = new byte[API_KEY_BYTES];
        SECURE_RANDOM.nextBytes(randomBytes);
        String apiKey = Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes);
        LOG.debugf("Generated new API key (length: %d)", apiKey.length());
        return apiKey;
    }

    /**
     * Hash an API key for secure storage.
     * <p>
     * Uses Argon2id with the following parameters:
     * <ul>
     *   <li>Memory cost: 64 MB</li>
     *   <li>Iterations: 3</li>
     *   <li>Parallelism: 4 threads</li>
     *   <li>Hash length: 256 bits</li>
     * </ul>
     * <p>
     * The hash includes a randomly generated salt and is stored in PHC string format.
     *
     * @param plaintextApiKey The plaintext API key
     * @return Argon2id hash in PHC format suitable for database storage
     */
    public String hashApiKey(String plaintextApiKey) {
        Hash hash = Password.hash(plaintextApiKey).with(ARGON2);
        String hashString = hash.getResult();
        LOG.debugf("Hashed API key with Argon2id (hash length: %d)", hashString.length());
        return hashString;
    }

    /**
     * Verify an API key against a stored hash.
     * <p>
     * Uses constant-time comparison to prevent timing attacks.
     * Automatically detects the hashing algorithm from the PHC format.
     *
     * @param plaintextApiKey The plaintext API key to verify
     * @param storedHash The Argon2id hash from database (PHC format)
     * @return true if the key matches the hash, false otherwise
     */
    public boolean verifyApiKey(String plaintextApiKey, String storedHash) {
        try {
            boolean matches = Password.check(plaintextApiKey, storedHash).with(ARGON2);
            LOG.debugf("API key verification: %s", matches ? "SUCCESS" : "FAILED");
            return matches;
        } catch (Exception e) {
            LOG.warnf(e, "API key verification failed with exception");
            return false;
        }
    }
}

