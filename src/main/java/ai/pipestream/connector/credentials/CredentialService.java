package ai.pipestream.connector.credentials;

/**
 * Pluggable interface for credential management (API key generation, hashing, verification).
 * <p>
 * Default implementation uses Argon2id hashing with local storage.
 * Production implementations can delegate to AWS Secrets Manager, Azure Key Vault,
 * HashiCorp Vault, or other external credential stores.
 * <p>
 * Select implementation via configuration:
 * <pre>
 *   pipestream.credentials.provider=local    # Argon2id (default)
 *   pipestream.credentials.provider=aws      # AWS Secrets Manager (future)
 * </pre>
 */
public interface CredentialService {

    /**
     * Generate a new secure random API key.
     *
     * @return Plaintext API key (must be hashed before storage, returned to user once)
     */
    String generateApiKey();

    /**
     * Hash or encrypt an API key for secure storage.
     * <p>
     * For local provider: Argon2id hash in PHC format.
     * For external providers: may return an encrypted ciphertext or a reference ID.
     *
     * @param plaintextApiKey The plaintext API key
     * @return Secure representation suitable for database storage
     */
    String hashApiKey(String plaintextApiKey);

    /**
     * Verify an API key against a stored hash/ciphertext.
     *
     * @param plaintextApiKey The plaintext API key to verify
     * @param storedCredential The stored hash or encrypted value from database
     * @return true if the key matches, false otherwise
     */
    boolean verifyApiKey(String plaintextApiKey, String storedCredential);
}
