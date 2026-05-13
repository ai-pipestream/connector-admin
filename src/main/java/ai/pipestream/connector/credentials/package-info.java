/**
 * Pluggable API-key credential management.
 *
 * <p>{@link ai.pipestream.connector.credentials.CredentialService} is the CDI interface
 * for generating, hashing, and verifying API keys.  The default implementation
 * ({@link ai.pipestream.connector.util.ApiKeyUtil}) uses Argon2id via Password4j and
 * is selected automatically via {@link io.quarkus.arc.DefaultBean} when no other
 * implementation is present.
 *
 * <p>Production deployments can replace the default by providing an alternative
 * {@code @ApplicationScoped} CDI bean that implements
 * {@link ai.pipestream.connector.credentials.CredentialService}, for example to
 * delegate key storage to AWS Secrets Manager or HashiCorp Vault.
 */
package ai.pipestream.connector.credentials;
