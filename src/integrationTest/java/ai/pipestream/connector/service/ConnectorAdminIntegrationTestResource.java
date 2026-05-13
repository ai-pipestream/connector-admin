package ai.pipestream.connector.service;

import ai.pipestream.test.support.BaseWireMockTestResource;
import org.testcontainers.containers.GenericContainer;

import java.util.HashMap;
import java.util.Map;

/**
 * Starts pipestream-wiremock-server for packaged connector-admin integration tests.
 *
 * <p>The account-manager mock is a unary gRPC service served through WireMock's
 * standard port. The direct gRPC port is for streaming mocks and does not serve
 * AccountService unary requests.
 */
public class ConnectorAdminIntegrationTestResource extends BaseWireMockTestResource {

    @Override
    protected String readyLogPattern() {
        return ".*Direct Streaming gRPC Server started.*";
    }

    @Override
    protected void configureContainer(GenericContainer<?> container) {
        container
            .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_DEFAULT_ID", "valid-account")
            .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_DEFAULT_NAME", "Valid Account")
            .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_DEFAULT_DESCRIPTION", "Valid account for integration tests")
            .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_DEFAULT_ACTIVE", "true")
            .withEnv("JAVA_TOOL_OPTIONS", "-Dwiremock.account.GetAccount.notfound.id=nonexistent");
    }

    @Override
    protected Map<String, String> buildConfig(GenericContainer<?> container) {
        String address = getHost() + ":" + getMappedPort(DEFAULT_HTTP_PORT);

        Map<String, String> config = new HashMap<>();
        config.put("pipestream.registration.enabled", "false");
        config.put("quarkus.grpc.clients.account-manager.host", "account-manager");
        config.put("quarkus.grpc.clients.account-manager.name-resolver", "stork");
        config.put("quarkus.stork.\"account-manager\".service-discovery.type", "static");
        config.put("quarkus.stork.\"account-manager\".service-discovery.address-list", address);
        config.put("stork.account-manager.service-discovery.type", "static");
        config.put("stork.account-manager.service-discovery.address-list", address);
        config.put("wiremock.host", getHost());
        config.put("wiremock.port", String.valueOf(getMappedPort(DEFAULT_HTTP_PORT)));
        return config;
    }
}
