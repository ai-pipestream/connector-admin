/**
 * Utility classes for the Connector Admin service.
 *
 * <p>{@link ai.pipestream.connector.util.ApiKeyUtil} is the default
 * {@link ai.pipestream.connector.credentials.CredentialService} implementation.
 * It generates cryptographically secure API keys (256-bit random Base64) and hashes
 * them with Argon2id (64 MB memory, 3 iterations, 4-thread parallelism) — parameters
 * aligned with OWASP recommendations for 2024.
 */
package ai.pipestream.connector.util;
