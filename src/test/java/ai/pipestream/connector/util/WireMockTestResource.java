package ai.pipestream.connector.util;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

/**
 * Test resource that starts a WireMock server container for mocking gRPC services.
 * <p>
 * This resource:
 * <ul>
 *   <li>Starts the pipestream-wiremock-server container</li>
 *   <li>Configures Stork static discovery to route account-manager to WireMock</li>
 *   <li>Exposes WireMock host and port via system properties for test access</li>
 * </ul>
 */
public class WireMockTestResource implements QuarkusTestResourceLifecycleManager {

    private static WireMockTestResource instance;
    private GenericContainer<?> wireMockContainer;
    private int wireMockPort;
    private String wireMockHost;

    public WireMockTestResource() {
        instance = this;
    }

    @Override
    public Map<String, String> start() {
        // Use configurable gRPC port, defaulting to 50052
        int grpcPort = Integer.parseInt(System.getProperty("wiremock.grpc.port", "50052"));
        wireMockContainer = new GenericContainer<>(
                DockerImageName.parse("docker.io/pipestreamai/pipestream-wiremock-server:0.1.8"))
                .withExposedPorts(8080, grpcPort)
                .waitingFor(Wait.forLogMessage(".*Direct Streaming gRPC Server started.*", 1))
                // Configure additional test accounts via environment variables
                .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_DEFAULT_ID", "valid-account")
                .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_DEFAULT_NAME", "Valid Account")
                .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_DEFAULT_DESCRIPTION", "Valid account for testing")
                .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_DEFAULT_ACTIVE", "true")
                // Configure inactive account
                .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_INACTIVE_ID", "inactive-account")
                .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_INACTIVE_NAME", "Inactive Account")
                .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_INACTIVE_DESCRIPTION", "Inactive account for testing")
                .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_INACTIVE_ACTIVE", "false")
                // Configure not found account
                .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_NOTFOUND_ID", "nonexistent");
        
        wireMockContainer.start();

        wireMockHost = wireMockContainer.getHost();
        // Use configurable gRPC port, defaulting to 50052
        int grpcPort = Integer.parseInt(System.getProperty("wiremock.grpc.port", "50052"));
        wireMockPort = wireMockContainer.getMappedPort(grpcPort);

        // Configure Stork with staticlist service discovery
        return Map.of(
            "quarkus.stork.account-manager.service-discovery.type", "staticlist",
            "quarkus.stork.account-manager.service-discovery.staticlist.address-list", wireMockHost + ":" + wireMockPort,
            "quarkus.stork.account-manager.load-balancer.type", "round-robin",
            // Expose WireMock connection info for tests
            "wiremock.host", wireMockHost,
            "wiremock.port", String.valueOf(wireMockPort)
        );
    }

    /**
     * Get the WireMock server port (for use in tests).
     */
    public int getWireMockPort() {
        return wireMockPort;
    }

    /**
     * Get the WireMock server host (for use in tests).
     */
    public String getWireMockHost() {
        return wireMockHost;
    }

    /**
     * Get the singleton instance (for use in tests).
     */
    public static WireMockTestResource getInstance() {
        return instance;
    }

    @Override
    public void stop() {
        if (wireMockContainer != null) {
            wireMockContainer.stop();
        }
    }
}

