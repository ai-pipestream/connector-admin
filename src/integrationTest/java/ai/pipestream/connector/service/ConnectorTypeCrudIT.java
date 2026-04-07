package ai.pipestream.connector.service;

import ai.pipestream.connector.intake.v1.MutinyConnectorRegistrationServiceGrpc;
import ai.pipestream.connector.intake.v1.MutinyDataSourceAdminServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for connector type CRUD via gRPC.
 * <p>
 * Runs against the production JAR in a separate JVM with {@code @QuarkusIntegrationTest}.
 * No CDI — gRPC channel is created manually from the injected URL.
 * All test logic lives in {@link ConnectorTypeCrudBaseTest}.
 */
@QuarkusIntegrationTest
public class ConnectorTypeCrudIT extends ConnectorTypeCrudBaseTest {

    @TestHTTPResource
    URL url;

    private ManagedChannel channel;
    private MutinyConnectorRegistrationServiceGrpc.MutinyConnectorRegistrationServiceStub registrationStub;
    private MutinyDataSourceAdminServiceGrpc.MutinyDataSourceAdminServiceStub adminStub;

    @BeforeEach
    void setUp() {
        channel = ManagedChannelBuilder
            .forAddress(url.getHost(), url.getPort())
            .usePlaintext()
            .build();
        registrationStub = MutinyConnectorRegistrationServiceGrpc.newMutinyStub(channel);
        adminStub = MutinyDataSourceAdminServiceGrpc.newMutinyStub(channel);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Override
    protected MutinyConnectorRegistrationServiceGrpc.MutinyConnectorRegistrationServiceStub registrationStub() {
        return registrationStub;
    }

    @Override
    protected MutinyDataSourceAdminServiceGrpc.MutinyDataSourceAdminServiceStub adminStub() {
        return adminStub;
    }
}
