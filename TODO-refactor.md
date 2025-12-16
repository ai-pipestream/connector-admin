# Service Migration Checklist: Converting to New Pipestream Architecture

This document provides a comprehensive checklist for migrating any Pipestream service to the new architecture. Use `connector-admin` and `account-service` as the reference implementations.

**IMPORTANT**: Before starting, review `connector-admin` (completed) and `account-service` to see the completed pattern. This checklist ensures no steps are missed.

## Summary of Key Changes

This migration standardizes all Pipestream services with:

1. **BOM Updates**: Migrated from `pipeline-bom` (defunct) to `pipestream-bom:0.7.2`
2. **Protobuf Generation**: Using Buf Registry exports instead of local proto files
3. **Kafka Configuration**: Simplified using `@ProtobufChannel` and `@ProtobufIncoming` annotations
4. **Service Registration**: Using `pipestream-service-registration` extension (zero-config)
5. **Multi-Arch Docker**: CVE-free builds for linux/amd64 and linux/arm64
6. **CI/CD Standardization**: Reusable workflows for snapshots and releases
7. **Maven Publishing**: Snapshots to GitHub Packages + Maven Local, Releases to Maven Central + GitHub Packages
8. **WireMock Integration**: Docker containers for mocking dependent gRPC services in tests
9. **Health Checks**: Using pre-installed `curl-minimal` (no need to install `curl`)
10. **Quarkus Version**: Managed via BOM (currently 3.30.3)

## Prerequisites

- [ ] Verify the service's protobuf definitions are available in the appropriate Buf registry:
  - `buf.build/pipestreamai/admin` - Account, Admin services
  - `buf.build/pipestreamai/intake` - Connector, Intake services
  - `buf.build/pipestreamai/registration` - Registration service
  - Check which registry contains your service's proto definitions

## 1. Build Configuration (`build.gradle`)

### 1.1 Update BOM Version
- [x] Update `pipestreamBomVersion` to `0.7.2` (or latest stable version)
- [x] Ensure BOM platform dependency is present:
  ```groovy
  def pipestreamBomVersion = findProperty('pipestreamBomVersion') ?: '0.7.2'
  implementation platform("ai.pipestream:pipestream-bom:${pipestreamBomVersion}")
  ```
- [x] **REMOVE** any references to old `pipeline-bom` (defunct, replaced by `pipestream-bom`)

### 1.2 Update Dependencies
- [x] **ADD** Apicurio Registry Protobuf extension:
  ```groovy
  implementation libs.quarkus.apicurio.registry.protobuf
  ```
- [x] **ADD** Service Registration extension (BOM-managed, no version):
  ```groovy
  implementation 'ai.pipestream:pipestream-service-registration'
  ```
- [x] **ADD** Dynamic gRPC extension (BOM-managed, no version):
  ```groovy
  implementation 'ai.pipestream:quarkus-dynamic-grpc'
  ```
- [x] **ADD** Dev Services infrastructure:
  ```groovy
  implementation libs.pipestream.devservices
  ```
- [x] **REMOVE** any explicit internal stubs (e.g., `libs.pipestream.grpc.stubs`) - protos will be generated locally
- [x] **REMOVE** any manual Apicurio serializer/deserializer dependencies - handled by extension
- [x] **REMOVE** WireMock server as a dependency - use Docker container via `WireMockTestResource` instead
- [x] **ADD** Testcontainers for WireMock integration tests:
  ```groovy
  testImplementation platform('org.testcontainers:testcontainers-bom:2.0.1')
  testImplementation 'org.testcontainers:testcontainers'
  testImplementation 'org.testcontainers:junit-jupiter'
  ```

### 1.3 Configure Protobuf Generation via Buf
- [x] Add `bufExportDir` definition:
  ```groovy
  def bufExportDir = layout.buildDirectory.dir("generated/buf-protos")
  ```
- [x] Add `syncProtos` task(s) for your service's proto registry:
  ```groovy
  // For multiple registries, create separate tasks
  tasks.register('syncProtosIntake', Exec) {
      group = 'protobuf'
      description = 'Exports proto files from Buf Registry (intake)'
      // Use bash -lc so the user's PATH (sdkman, homebrew, etc.) is honored for the buf CLI
      commandLine 'bash', '-lc', "buf export buf.build/pipestreamai/intake --output ${bufExportDir.get().asFile.path}"
      outputs.dir(bufExportDir)
      doFirst { bufExportDir.get().asFile.mkdirs() }
  }
  
  tasks.register('syncProtosAdmin', Exec) {
      group = 'protobuf'
      description = 'Exports proto files from Buf Registry (admin)'
      commandLine 'bash', '-lc', "buf export buf.build/pipestreamai/admin --output ${bufExportDir.get().asFile.path}"
      outputs.dir(bufExportDir)
      doFirst { bufExportDir.get().asFile.mkdirs() }
  }
  
  // Aggregate task
  tasks.register('syncProtos') {
      group = 'protobuf'
      description = 'Exports all required proto files'
      dependsOn 'syncProtosIntake', 'syncProtosAdmin'
  }
  ```
  - Replace registries with the appropriate ones for your service (admin, intake, registration, etc.)
  - Use `bash -lc` to honor user's PATH for buf CLI (sdkman, homebrew, etc.)
- [x] Configure Quarkus to use exported protos:
  ```groovy
  quarkus {
      quarkusBuildProperties.put("quarkus.grpc.codegen.proto-directory", bufExportDir.get().asFile.path)
      quarkusBuildProperties.put("quarkus.grpc.codegen.enabled", "true")
  }
  ```
- [x] Update source sets for IDE support:
  ```groovy
  sourceSets {
      main {
          java {
              srcDir layout.buildDirectory.dir("classes/java/quarkus-generated-sources/grpc")
          }
      }
  }
  ```
- [x] Hook proto sync to code generation:
  ```groovy
  tasks.named('quarkusGenerateCode').configure { dependsOn 'syncProtos' }
  tasks.named('quarkusGenerateCodeDev').configure { dependsOn 'syncProtos' }
  tasks.named('quarkusGenerateCodeTests').configure { dependsOn 'syncProtos' }
  tasks.named('compileJava').configure { dependsOn 'quarkusGenerateCode' }
  ```
- [x] Ensure sourcesJar includes generated sources:
  ```groovy
  tasks.named('sourcesJar').configure { dependsOn 'quarkusGenerateCode' }
  ```

### 1.4 Optimize Javadoc (Exclude Generated Code)
- [ ] Add Javadoc configuration to exclude generated protobuf files:
  ```groovy
  tasks.withType(Javadoc).configureEach { task ->
      def opts = task.options
      opts.encoding = 'UTF-8'
      opts.charSet = 'UTF-8'
      opts.locale = 'en'
      // Whitelist only application source packages
      include '**/ai/pipestream/YOUR_SERVICE_PACKAGE/**'
  }
  ```

## 2. Application Configuration (`application.properties`)

### 2.1 Service Registration Configuration
- [ ] **REMOVE** old `service.registration.*` properties
- [ ] **ADD** new `pipestream.registration.*` properties:
  ```properties
  pipestream.registration.enabled=true
  pipestream.registration.service-name=YOUR_SERVICE_NAME
  pipestream.registration.description=Your service description
  pipestream.registration.type=SERVICE  # or MODULE
  pipestream.registration.advertised-host=${SERVICE_HOST:host.docker.internal}
  pipestream.registration.advertised-port=${quarkus.http.port}
  # Internal host/port for Docker - Consul uses this for health checks
  pipestream.registration.internal-host=${DOCKER_BRIDGE_IP:172.17.0.1}
  pipestream.registration.internal-port=${quarkus.http.port}
  pipestream.registration.capabilities=capability1,capability2
  pipestream.registration.tags=tag1,tag2
  
  # Registration service connection
  pipestream.registration.registration-service.host=localhost
  pipestream.registration.registration-service.port=38101
  
  %test.pipestream.registration.enabled=false
  ```

### 2.2 Quarkus Index Dependencies (CRITICAL)
- [ ] **ADD** index dependencies for extensions (required for ConfigMapping discovery):
  ```properties
  quarkus.index-dependency.pipestream-registration.group-id=ai.pipestream
  quarkus.index-dependency.pipestream-registration.artifact-id=pipestream-service-registration
  quarkus.index-dependency.pipestream-dynamic-grpc.group-id=ai.pipestream
  quarkus.index-dependency.pipestream-dynamic-grpc.artifact-id=quarkus-dynamic-grpc
  ```

### 2.3 Apicurio Registry Configuration
- [x] **REMOVE** manual `quarkus.index-dependency.apicurio-registry.*` entries (handled by extension)
- [x] **REMOVE** manual `mp.messaging.*.apicurio.registry.url` overrides in main resources
- [x] **REMOVE** manual `value.serializer`/`value.deserializer` config from main resources (handled by extension)
- [x] **USE** `@ProtobufChannel` for publishers and `@ProtobufIncoming` for consumers (automatic configuration)
- [x] For test profile, use standard deserializer if needed:
  ```properties
  %test.mp.messaging.incoming.CHANNEL_NAME.value.deserializer=io.apicurio.registry.serde.protobuf.ProtobufKafkaDeserializer
  ```

### 2.4 Dev Services Configuration
- [ ] Ensure Compose Dev Services configuration matches account-service pattern:
  ```properties
  %dev.quarkus.compose.devservices.enabled=true
  %dev.quarkus.compose.devservices.files=${user.home}/.pipeline/compose-devservices.yml
  %dev.quarkus.compose.devservices.project-name=pipeline-shared-devservices
  %dev.quarkus.compose.devservices.start-services=true
  %dev.quarkus.compose.devservices.stop-services=false
  %dev.quarkus.compose.devservices.reuse-project-for-tests=true
  %dev.quarkus.compose.devservices.stop-timeout=30s
  %test.quarkus.compose.devservices.stop-services=false
  ```

## 3. Docker Configuration (`src/main/docker/Dockerfile.jvm`)

### 3.1 CVE-Free Multi-Arch Build
- [x] Update to use optimized base image:
  ```dockerfile
  FROM registry.access.redhat.com/ubi9-minimal:9.5
  ```
- [x] Install headless JDK and update CVEs:
  ```dockerfile
  # Install Java 21 runtime (headless for smaller size) and update CVEs
  # curl-minimal is pre-installed in ubi9-minimal and supports -f flag for health checks
  RUN microdnf install -y \
      java-21-openjdk-headless \
      ca-certificates \
      && microdnf update -y \
      && microdnf clean all \
      && rm -rf /var/cache/yum
  ```
  **IMPORTANT**: Do NOT install `curl` - `curl-minimal` is pre-installed and supports `-f` flag
- [x] Use direct `java -jar` entrypoint (not `run-java.sh`):
  ```dockerfile
  ENTRYPOINT ["java", "-jar", "/deployments/quarkus-run.jar"]
  ```
- [x] Add HEALTHCHECK using Quarkus health endpoint:
  ```dockerfile
  HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
      CMD curl -f http://localhost:8080/SERVICE_ROOT_PATH/q/health || exit 1
  ```
  Replace `SERVICE_ROOT_PATH` with your service's root path (e.g., `/connector`, `/account`, `/platform-registration`)
- [x] Expose HTTP port:
  ```dockerfile
  EXPOSE 8080
  ```
- [x] Set non-root user:
  ```dockerfile
  USER 185
  ```
- [x] Copy Quarkus app with proper ownership:
  ```dockerfile
  COPY --chown=185:185 build/quarkus-app/lib/ /deployments/lib/
  COPY --chown=185:185 build/quarkus-app/*.jar /deployments/
  COPY --chown=185:185 build/quarkus-app/app/ /deployments/app/
  COPY --chown=185:185 build/quarkus-app/quarkus/ /deployments/quarkus/
  ```

## 4. Test Configuration

### 4.1 WireMock for Dependent gRPC Services
- [x] **CREATE** `WireMockTestResource.java` to mock external gRPC services:
  ```java
  public class WireMockTestResource implements QuarkusTestResourceLifecycleManager {
      private GenericContainer<?> wireMockContainer;
      private int wireMockPort;
      private String wireMockHost;
      
      @Override
      public Map<String, String> start() {
          int grpcPort = Integer.parseInt(System.getProperty("wiremock.grpc.port", "50052"));
          wireMockContainer = new GenericContainer<>(
                  DockerImageName.parse("docker.io/pipestreamai/pipestream-wiremock-server:0.1.18"))
                  .withExposedPorts(8080, grpcPort)
                  .waitingFor(Wait.forLogMessage(".*Direct Streaming gRPC Server started.*", 1))
                  .withEnv("WIREMOCK_SERVICE_MOCK_CONFIG", "...");
          wireMockContainer.start();
          
          wireMockHost = wireMockContainer.getHost();
          wireMockPort = wireMockContainer.getMappedPort(grpcPort);
          
          return Map.of(
              "stork.SERVICE_NAME.service-discovery.address-list", 
              wireMockHost + ":" + wireMockPort
          );
      }
      
      @Override
      public void stop() {
          if (wireMockContainer != null) {
              wireMockContainer.stop();
          }
      }
  }
  ```
- [x] **USE** WireMock in tests:
  ```java
  @QuarkusTest
  @QuarkusTestResource(WireMockTestResource.class)
  public class YourServiceTest {
      // Tests use mocked gRPC services via WireMock
  }
  ```
- [x] **REMOVE** any direct gRPC client dependencies in tests - use WireMock container instead

### 4.2 Docker Compose Test Services (`src/test/resources/compose-test-services.yml`)
- [x] **REMOVE** manual Apicurio config map (handled by extension)
- [x] Ensure compose services are properly configured for test profile

### 4.3 Test Resources
- [x] **DELETE** any manual TestResource classes (e.g., `ApicurioTestResource.java`)
- [x] **REMOVE** `@QuarkusTestResource` annotations referencing deleted resources
- [x] Use WireMock for all external gRPC service dependencies

## 5. Code Changes

### 5.1 Kafka Messaging Annotations
- [ ] **Publishers** (in `src/main`): Replace `@Channel` with `@ProtobufChannel("channel-name")`
- [ ] **Consumers** (in `src/main`): Replace `@Incoming` with `@ProtobufIncoming("channel-name")`
- [ ] **Test Scope**: In `src/test/java`, use standard `@Incoming` + manual `value.deserializer` in `%test` profile if needed
- [ ] **REMOVE** manual `@Produces` annotations for channels (handled by extension)

### 5.2 gRPC Service Implementation
- [ ] Update gRPC service implementations to extend new `*ImplBase` classes generated from Buf registry
- [ ] Verify all proto imports are from generated classes (not old stubs)
- [ ] Update any manual proto message construction to use new generated classes

### 5.3 Scripts
- [ ] Update startup scripts to use new property names:
  - Change `-Dservice.registration.host` to `-Dpipestream.registration.advertised-host`
  - Or remove if using environment variables

## 6. CI/CD Standardization (`.github/workflows`)

### 6.1 Snapshot Builds
- [x] **build-and-publish.yml**: Use reusable workflow:
  ```yaml
  name: Build and Publish
  
  on:
    push:
      branches: [main]
    pull_request:
      branches: [main]
  
  jobs:
    call-reusable-build:
      uses: ai-pipestream/.github/.github/workflows/reusable-service-build.yml@main
      with:
        docker-image-name: YOUR_SERVICE_NAME
      secrets: inherit
  ```
  - Pushes snapshot Docker images to GHCR
  - Publishes snapshot Maven artifacts to GitHub Packages and Maven Local
  - Uses multi-arch builds (linux/amd64,linux/arm64) automatically

### 6.2 Release Builds
- [x] **release-and-publish.yml**: Use reusable workflow:
  ```yaml
  name: Release and Publish
  
  on:
    workflow_dispatch:
      inputs:
        release_type:
          description: 'Release type'
          required: true
          default: 'patch'
          type: choice
          options: [patch, minor, major, manual]
        custom_version:
          description: 'Custom version (if manual)'
          required: false
  
  jobs:
    call-reusable-release:
      uses: ai-pipestream/.github/.github/workflows/reusable-service-release.yml@main
      with:
        release_type: ${{ inputs.release_type }}
        custom_version: ${{ inputs.custom_version }}
        docker-image-name: YOUR_SERVICE_NAME
        maven-artifact-id: YOUR_SERVICE_NAME
      secrets: inherit
  ```
  - Creates git tag and GitHub release
  - Publishes to Maven Central (via NMCP)
  - Publishes to GitHub Packages
  - Builds and pushes multi-arch Docker images to both GHCR and Docker Hub
  - Includes SBOM and provenance attestations

### 6.3 Key Features
- [x] Multi-arch Docker builds (linux/amd64,linux/arm64) via QEMU and Buildx
- [x] Buf setup included for protobuf generation
- [x] Snapshot packages published to GitHub Packages and Maven Local
- [x] Release packages published to Maven Central and GitHub Packages
- [x] Docker images pushed to both GHCR and Docker Hub (releases only)

## 7. Settings Configuration (`settings.gradle`)

### 7.1 BOM Catalog
- [x] Verify BOM catalog is configured:
  ```groovy
  dependencyResolutionManagement {
      versionCatalogs {
          libs {
              from("ai.pipestream:pipestream-bom-catalog:0.7.2")
          }
      }
  }
  ```

### 7.2 Maven Repositories
- [x] Ensure Maven repositories include:
  ```groovy
  repositories {
      // Maven Local for local development (releases only for ai.pipestream)
      mavenLocal {
          mavenContent {
              releasesOnly()
          }
          content {
              includeGroupByRegex "ai\\.pipestream(\\..*)?"
          }
      }
      
      mavenCentral()
      
      // Sonatype snapshots
      maven {
          url = uri('https://central.sonatype.com/repository/maven-snapshots/')
          mavenContent {
              snapshotsOnly()
          }
      }
      
      // GitHub Packages for ai.pipestream catalog and dependencies
      def githubToken = providers.environmentVariable("GITHUB_TOKEN")
          .orElse(providers.gradleProperty("gpr.key"))
          .orElse(providers.environmentVariable("GPR_TOKEN"))
      
      def githubActor = providers.environmentVariable("GITHUB_ACTOR")
          .orElse(providers.gradleProperty("gpr.user"))
          .orElse(providers.environmentVariable("GPR_USER"))
      
      if (githubToken.isPresent() && githubActor.isPresent()) {
          maven {
              url = uri("https://maven.pkg.github.com/ai-pipestream/bom")
              credentials {
                  username = githubActor.get()
                  password = githubToken.get()
              }
              content {
                  includeGroupByRegex "ai\\.pipestream(\\..*)?"
              }
          }
      }
  }
  ```

## 8. Verification Steps

- [ ] Run `./gradlew clean build` - should sync protos and generate code
- [ ] Run `./gradlew quarkusDev` - service should start successfully
- [ ] Verify Apicurio extension logs at startup (auto-configuration)
- [ ] Verify service registration logs (should register with platform-registration-service)
- [ ] Check Consul UI - service should appear with correct metadata
- [ ] Run `./gradlew test` - all tests should pass
- [ ] Verify Docker build: `./gradlew build -Dquarkus.container-image.build=true`
- [ ] Test Docker container starts and registers correctly

## 9. Common Issues & Solutions

### Issue: `RegistrationConfig` unsatisfied dependency
**Solution**: Add `quarkus.index-dependency` entries (see section 2.2)

### Issue: Protobuf classes not found
**Solution**: Ensure `syncProtos` task runs before code generation, check `bufExportDir` path

### Issue: Apicurio not auto-configuring
**Solution**: Remove manual serializer/deserializer config, use `@ProtobufChannel`/`@ProtobufIncoming`

### Issue: Service not registering
**Solution**: Check `pipestream.registration.*` properties, verify platform-registration-service is running

### Issue: Consul health checks failing
**Solution**: Verify `internal-host` is set to Docker bridge IP (172.17.0.1) for Docker environments

## 10. Post-Migration Cleanup

- [ ] Remove any TODO comments related to migration
- [ ] Update README.md with new architecture details
- [ ] Verify all documentation reflects new patterns
- [ ] Check for any remaining references to old patterns (grep for `service.registration`, old proto imports, etc.)

---

## Reference Implementation

**Use `account-service` as the reference** - it has completed all these steps and serves as the template for other services.

## 11. Additional Configuration Steps

### 11.1 Quarkus Version
- [x] Ensure Quarkus version is managed via BOM (currently 3.30.3 in pipestream-bom 0.7.2)
- [x] No need to specify Quarkus version directly - BOM manages it

### 11.2 Kafka Topic Configuration
- [x] **SIMPLIFY** Kafka topic configuration - use extension annotations instead of manual config
- [x] Remove redundant topic/connector configuration (handled by extension)

### 11.3 Maven Publishing
- [x] Configure Maven publishing for snapshots and releases
- [x] Snapshots go to GitHub Packages and Maven Local
- [x] Releases go to Maven Central (via NMCP) and GitHub Packages
- [x] Ensure `maven-artifact-id` matches service name in reusable workflows

### 11.4 Buf CLI Requirements
- [x] Ensure `buf` CLI is available in CI/CD (handled by `bufbuild/buf-action@v1`)
- [x] For local development, install buf CLI:
  - macOS: `brew install bufbuild/buf/buf`
  - Linux: See https://buf.build/docs/installation
  - Or use SDKMAN if available

## Notes

## 12. Protobuf Package and Codegen Notes

This section captures the authoritative protobuf package mapping and practical tips when migrating services that depend on `pipestreamai/intake` (and shared core types).

### 12.1 Registry and Packages
- Buf module: https://buf.build/pipestreamai/intake
- Protobuf package: `ai.pipestream.connector.intake.v1`
- Java package (generated): `ai.pipestream.connector.intake.v1` (via `option java_package`)
- Shared/core types: `ai.pipestream.data.v1`

### 12.2 Generated Services and Classes
- gRPC services defined in Intake:
  - `ConnectorIntakeService`
  - `ConnectorAdminService`
- Quarkus/Mutiny server stubs (extend these in service impls):
  - `ai.pipestream.connector.intake.v1.MutinyConnectorIntakeServiceGrpc`
  - `ai.pipestream.connector.intake.v1.MutinyConnectorAdminServiceGrpc`
- Example messages you will use:
  - `RegisterConnectorRequest` / `RegisterConnectorResponse`
  - `GetConnectorRequest` / `GetConnectorResponse`
  - `ListConnectorsRequest` / `ListConnectorsResponse`
  - `SetConnectorStatusRequest` / `SetConnectorStatusResponse`
  - `RotateApiKeyRequest` / `RotateApiKeyResponse`
  - `ValidateApiKeyRequest` / `ValidateApiKeyResponse`
  - Upload paths: `UploadPipeDocRequest` / `UploadPipeDocResponse`, `UploadBlobRequest` / `UploadBlobResponse`
  - Session mgmt: `StartCrawlSession*`, `EndCrawlSession*`, `Heartbeat*`
  - Shared types from `ai.pipestream.data.v1` such as `PipeDoc`, `Blob`

### 12.3 Example Imports (server implementation)
```java
import ai.pipestream.connector.intake.v1.MutinyConnectorAdminServiceGrpc;
import ai.pipestream.connector.intake.v1.RegisterConnectorRequest;
import ai.pipestream.connector.intake.v1.RegisterConnectorResponse;
import ai.pipestream.connector.intake.v1.GetConnectorRequest;
import ai.pipestream.connector.intake.v1.GetConnectorResponse;
// ... other intake v1 messages
import ai.pipestream.data.v1.PipeDoc;
```

### 12.4 Migration Guidance
- Replace any legacy imports that omit the `.v1` segment (e.g., `ai.pipestream.connector.intake.*`) with the new `ai.pipestream.connector.intake.v1.*` packages.
- Ensure shared/core types import from `ai.pipestream.data.v1.*`.
- Remove references to old prebuilt stubs; rely on Quarkus codegen from Buf-exported protos.

### 12.5 Verifying Locally
Export from Buf and validate package settings in the `.proto` files:
```bash
buf export buf.build/pipestreamai/intake --output build/generated/buf-protos
grep -R "^package\|java_package" build/generated/buf-protos
```
You should see:
```
package ai.pipestream.connector.intake.v1;
option java_package = "ai.pipestream.connector.intake.v1";
```

### 12.6 Codegen Configuration Reminders
- `quarkus.grpc.codegen.proto-directory` must point to the Buf export directory (see section 1.3).
- Ensure generated sources directory is added to the main source set so IDEs can resolve classes.

### 12.7 TL;DR
- Intake protos: `ai.pipestream.connector.intake.v1`
- Shared/core types: `ai.pipestream.data.v1`
- Server base classes: `MutinyConnectorIntakeServiceGrpc`, `MutinyConnectorAdminServiceGrpc`
- Point codegen at Buf export dir; do not depend on legacy internal stubs.

- All services should use the same BOM version (`0.7.2` or latest)
- Protobuf generation is now handled via Buf, not local proto files
- Apicurio configuration is automatic via extension annotations (`@ProtobufChannel`, `@ProtobufIncoming`)
- Service registration is automatic via extension (zero-config)
- All services share the same CI/CD workflows for consistency
- Multi-arch Docker builds (linux/amd64,linux/arm64) are automatic via reusable workflows
- CVE-free Docker images using `ubi9-minimal:9.5` with `microdnf update -y`
- WireMock containers replace direct gRPC dependencies in tests
- Health checks use pre-installed `curl-minimal` (supports `-f` flag)

---

# Connector-Intake-Service Migration Guide

This section provides specific guidance for migrating `connector-intake-service` to the new Pipestream architecture. Use this as a reference when performing the migration.

## FAQ: Connector-Intake-Service Specific Questions

### 1. Protobuf Sources

**Q: Besides `buf.build/pipestreamai/intake`, do we also need `buf.build/pipestreamai/admin` or `buf.build/pipestreamai/registration` for shared/core messages used by connector-intake-service?**

**A:** Yes, `connector-intake-service` requires multiple Buf registries:
- **`buf.build/pipestreamai/intake`** - Primary registry containing connector intake service protos
- **`buf.build/pipestreamai/admin`** - Required for `ConnectorAdminService` gRPC client calls (ValidateApiKey, GetConnector)
- **`buf.build/pipestreamai/registration`** - **NOT required** - connector-intake-service does not use registration service protos directly

**Exact registries needed:**
```groovy
tasks.register('syncProtosIntake', Exec) {
    commandLine 'bash', '-lc', "buf export buf.build/pipestreamai/intake --output ${bufExportDir.get().asFile.path}"
}

tasks.register('syncProtosAdmin', Exec) {
    commandLine 'bash', '-lc', "buf export buf.build/pipestreamai/admin --output ${bufExportDir.get().asFile.path}"
}
```

**Q: Any private Buf modules or pinned commits/tags we must lock to?**

**A:** No private Buf modules required. All registries (`buf.build/pipestreamai/intake`, `buf.build/pipestreamai/admin`) are publicly accessible. No pinned commits/tags needed - use latest from main branch of each registry.

---

### 2. Package and API Changes

**Q: Have Java package names for generated classes changed vs the old stubs (e.g., `ai.pipestream.intake.*`)?**

**A:** Yes, package names have changed. Here's the mapping:

**Old Package Structure (from local stubs):**
- `ai.pipeline.connector.intake.*` - Old package structure

**New Package Structure (from Buf registries):**
- `ai.pipestream.connector.intake.*` - New package structure (note: `pipestream` not `pipeline`)
- `ai.pipestream.connector.intake.v1.*` - Service definitions (if versioned)
- `ai.pipestream.repository.*` - Repository service protos (unchanged)
- `ai.pipestream.repository.account.*` - Account service protos (unchanged)

**Example Mapping:**
```java
// OLD (from local stubs)
import ai.pipeline.connector.intake.ConnectorConfig;
import ai.pipeline.connector.intake.ConnectorRegistration;

// NEW (from Buf registry)
import ai.pipestream.connector.intake.ConnectorConfig;
import ai.pipestream.connector.intake.ConnectorRegistration;
```

**Key Changes:**
- `ai.pipeline.*` → `ai.pipestream.*` (package rename)
- Service implementation classes remain in `ai.pipeline.connector.intake.*` (application code, not generated)
- Only generated proto classes change package

**Files requiring updates:**
- `ConnectorValidationService.java` - Update imports from `ai.pipestream.connector.intake.*`
- `DocumentProcessor.java` - Update imports
- Any test files using proto classes

---

### 3. Kafka Topology

**Q: List of Kafka channel names used by connector-intake-service and whether they are publishers or consumers. Any channel renamed during this migration?**

**A:** **Current Status:** `connector-intake-service` does **NOT** currently use Kafka channels. No `@Incoming`, `@Outgoing`, `@Channel`, `@ProtobufChannel`, or `@ProtobufIncoming` annotations found in the codebase.

**Kafka Configuration:**
- No Kafka channels configured in `application.properties`
- No Kafka topic configuration
- Service is primarily gRPC-based for document intake

**Migration Impact:**
- No Kafka channel migration required
- No topic configuration needed
- If Kafka integration is added in the future, use `@ProtobufChannel` for publishers and `@ProtobufIncoming` for consumers

**Note:** The service may add Kafka integration in the future for event publishing (e.g., document processing events), but this is not part of the current migration scope.

---

### 4. gRPC Dependencies

**Q: Which external gRPC services does connector-intake-service call? Provide the Stork service names (or target host:port) so we can configure WireMock to emulate them in tests.**

**A:** `connector-intake-service` calls two external gRPC services:

**1. Connector Admin Service (via Stork)**
- **Stork Service Name:** `connector-service` (defined in `ConnectorValidationService.CONNECTOR_SERVICE_NAME`)
- **gRPC Method:** `ValidateApiKey` (from `ConnectorAdminService`)
- **Purpose:** Validates connector API keys and retrieves connector configuration
- **WireMock Configuration:**
  ```text
  # In WireMockTestResource.java
  "stork.connector-service.service-discovery.address-list" -> wireMockHost + ":" + wireMockPort
  ```

**2. Account Service (via Stork)**
- **Stork Service Name:** `account-manager` (defined in `ConnectorValidationService.ACCOUNT_SERVICE_NAME`)
- **gRPC Method:** `GetAccount` (from `AccountService`)
- **Purpose:** Validates that the account exists and is active
- **WireMock Configuration:**
  ```text
  # In WireMockTestResource.java
  "stork.account-manager.service-discovery.address-list" -> wireMockHost + ":" + wireMockPort
  ```

**3. Repository Service (via Static Configuration)**
- **Configuration:** `quarkus.grpc.clients.repo-service.host=localhost` and `port=38105`
- **gRPC Methods:** `CreateNode` (from `FileSystemService`)
- **Purpose:** Creates file system nodes for uploaded documents
- **Note:** Currently uses static host:port, not Stork. Consider migrating to Stork service discovery in the future.
- **WireMock Configuration:** Not needed for tests (uses static config), but can be added if migrating to Stork.

**Q: Any streaming RPCs or bidirectional calls we should replicate in WireMock configuration?**

**A:** No streaming RPCs or bidirectional calls. All gRPC calls are unary:
- `ValidateApiKey` - Unary request/response
- `GetAccount` - Unary request/response
- `CreateNode` - Unary request/response

**WireMock Setup:**
- Use standard WireMock gRPC stubs (no streaming configuration needed)
- Configure mocks for `ValidateApiKeyRequest` → `ValidateApiKeyResponse`
- Configure mocks for `GetAccountRequest` → `GetAccountResponse`

---

### 5. Service Registration

**Q: Final values for these properties: `pipestream.registration.service-name`, `description`, `type` (SERVICE vs MODULE), `capabilities`, `tags`.**

**A:** Here are the final values for `connector-intake-service`:

```properties
pipestream.registration.enabled=true
pipestream.registration.service-name=connector-intake-service
pipestream.registration.description=Connector document ingestion, authentication, metadata enrichment, and rate limiting
pipestream.registration.type=SERVICE
pipestream.registration.advertised-host=${CONNECTOR_INTAKE_SERVICE_HOST:host.docker.internal}
pipestream.registration.advertised-port=${quarkus.http.port}
pipestream.registration.internal-host=${DOCKER_BRIDGE_IP:172.17.0.1}
pipestream.registration.internal-port=${quarkus.http.port}
pipestream.registration.capabilities=document-ingestion,metadata-enrichment,rate-limiting
pipestream.registration.tags=connector,intake,core-service
# Registration service connection
pipestream.registration.registration-service.host=localhost
pipestream.registration.registration-service.port=38101

%test.pipestream.registration.enabled=false
```

**Key Points:**
- **Type:** `SERVICE` (not `MODULE` - connector-intake-service is a platform service, not a processing module)
- **Capabilities:** `document-ingestion,metadata-enrichment,rate-limiting`
- **Tags:** `connector,intake,core-service`
---

### 6. HTTP Root Path and Healthcheck

**Q: What is the canonical service root path segment used in HTTP endpoints for health checks?**

**A:** The canonical root path is `/intake`.

**Current Configuration:**
```properties
%dev.quarkus.http.root-path=/intake
```

**Docker HEALTHCHECK:**
```dockerfile
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/intake/q/health || exit 1
```

**Note:** The service uses port `38108` in dev mode, but the Docker container exposes port `8080` (standard Quarkus port). The health check uses the internal container port `8080` with the root path `/intake`.

**Health Endpoints:**
- Liveness: `http://localhost:8080/intake/q/health/live`
- Readiness: `http://localhost:8080/intake/q/health/ready`
- Startup: `http://localhost:8080/intake/q/health/started`
- Full Health: `http://localhost:8080/intake/q/health`

---

### 7. Java and Build Tool Constraints

**Q: We'll align with Java 21 (as per BOM/Quarkus); confirm connector-intake-service is already on JDK 21 in CI and local dev.**

**A:** **Confirmed:** `connector-intake-service` is already on Java 21.

**Evidence:**
```groovy
// build.gradle
java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}
```

**Q: Any Gradle wrapper version constraints we must keep?**

**A:** No specific Gradle wrapper version constraints. Use the same Gradle wrapper version as `connector-admin` and `account-service` (typically Gradle 8.x). Check `gradle/wrapper/gradle-wrapper.properties` in those services for the exact version.

**Recommendation:** Use Gradle 8.5 or later (compatible with Java 21 and Quarkus 3.30.3).

---

### 8. Test Suites

**Q: Are there any existing integration tests that rely on in-project proto files or legacy stubs we should delete/replace?**

**A:** **Yes, tests need updates:**

**Files to Update:**
1. **`ConnectorIntakeIntegrationTest.java`** - Likely uses old proto imports (`ai.pipeline.connector.intake.*`)
2. **`BenchmarkIntakeTest.java`** - May use old proto classes
3. **Any test files importing from `ai.pipeline.connector.intake.*`** - Update to `ai.pipestream.connector.intake.*`

**Test Resources to Preserve:**
- `src/test/resources/application.properties` - Keep, but update property names (`service.registration.*` → `pipestream.registration.*`)
- `src/test/resources/compose-test-services.yml` - Keep, but verify it matches the pattern from `connector-admin`

**WireMock Integration:**
- **CREATE** `WireMockTestResource.java` similar to `connector-admin` to mock:
  - `connector-service` (Stork service name)
  - `account-manager` (Stork service name)
- **REMOVE** any direct gRPC client dependencies in tests
- **UPDATE** tests to use WireMock container instead of real gRPC services

**Q: Any custom test resources we must preserve?**

**A:** Preserve:
- `src/test/resources/application.properties` - Test configuration
- `src/test/resources/compose-test-services.yml` - Test infrastructure (if exists)
- Any test data files or fixtures
- **DO NOT** preserve old proto stub JARs or local proto files (replaced by Buf generation)

---

### 9. CI/CD and Credentials

**Q: Do we have GH Packages tokens configured for this repo (so the catalog/artifacts resolve) and for publishing snapshots?**

**A:** **Yes, GitHub Packages is configured:**

**Evidence from `build.gradle`:**
```groovy
def ghRepo = System.getenv('GITHUB_REPOSITORY') ?: 'ai-pipestream/connector-intake-service'
def ghActor = System.getenv('GITHUB_ACTOR') ?: (project.findProperty('gpr.user') ?: System.getenv('GPR_USER'))
def ghToken = System.getenv('GITHUB_TOKEN') ?: (project.findProperty('gpr.key') ?: System.getenv('GPR_TOKEN'))
```

**Current Setup:**
- Uses `GITHUB_TOKEN` environment variable (automatically provided by GitHub Actions)
- Falls back to `gpr.user`/`gpr.key` Gradle properties or `GPR_USER`/`GPR_TOKEN` environment variables
- Repository: `ai-pipestream/connector-intake-service`

**Migration Requirements:**
- **NO changes needed** - GitHub Packages authentication is already configured
- Ensure `GITHUB_TOKEN` secret is available in GitHub Actions workflows
- Verify `settings.gradle` includes GitHub Packages repository (already present)

**Q: Docker image name and organization to use (GHCR only for snapshots; GHCR + Docker Hub for releases): confirm naming.**

**A:** **Current Configuration:**
```properties
quarkus.container-image.registry=ghcr.io
quarkus.container-image.group=ai-pipestream
quarkus.container-image.name=connector-intake-service
```

**Docker Image Names:**
- **Snapshots (GHCR only):**
  - `ghcr.io/ai-pipestream/connector-intake-service:main-<sha>`
  - `ghcr.io/ai-pipestream/connector-intake-service:<sha>`
  - `ghcr.io/ai-pipestream/connector-intake-service:snapshot`

- **Releases (GHCR + Docker Hub):**
  - `ghcr.io/ai-pipestream/connector-intake-service:<version>`
  - `ghcr.io/ai-pipestream/connector-intake-service:latest`
  - `docker.io/pipestreamai/connector-intake-service:<version>`
  - `docker.io/pipestreamai/connector-intake-service:latest`

**Migration:**
- Update reusable workflows to use `docker-image-name: connector-intake-service`
- Ensure Docker Hub organization is `pipestreamai` (matches other services)

---

## A note about the protobufs

The interfaces and definitions are copied to the pipstream-wiremock-server - if you want to look up what definitions we are downloading,  /home/krickert/IdeaProjects/pipestream-wiremock-server/build/generated/buf-protos is a good place to look.

## Migration Checklist for Connector-Intake-Service

### Pre-Migration Verification
- [ ] Confirm Java 21 is used (already verified)
- [ ] Review current `build.gradle` dependencies
- [ ] Identify all files using old package names (`ai.pipeline.connector.intake.*`)
- [ ] List all gRPC service calls (connector-service, account-manager, repo-service)

### Build Configuration
- [ ] Update BOM from `pipeline-bom:0.2.10-SNAPSHOT` to `pipestream-bom:0.7.2`
- [ ] Update `settings.gradle` catalog from `pipeline-bom-catalog:0.2.7` to `pipestream-bom-catalog:0.7.2`
- [ ] Add Buf proto sync tasks for `buf.build/pipestreamai/intake` and `buf.build/pipestreamai/admin`
- [ ] Configure Quarkus gRPC codegen to use exported protos
- [ ] Add `pipestream-service-registration` extension
- [ ] Add `quarkus-apicurio-registry-protobuf` extension (if Kafka is added in future)
- [ ] Remove old `libs.pipestream.grpc.stubs` dependency
- [ ] Remove old `libs.pipestream.dynamic.grpc.registration.clients` (if present)
- [ ] Add Testcontainers for WireMock integration

### Application Configuration
- [ ] Update `service.registration.*` → `pipestream.registration.*` properties
- [ ] Add `quarkus.index-dependency` entries for extensions
- [ ] Verify HTTP root path is `/intake`
- [ ] Update gRPC client configurations (connector-service, account-manager)
- [ ] Consider migrating repo-service from static config to Stork (optional)

### Code Updates
- [ ] Update all imports from `ai.pipeline.connector.intake.*` → `ai.pipestream.connector.intake.*`
- [ ] Update `ConnectorValidationService.java` imports
- [ ] Update `DocumentProcessor.java` imports
- [ ] Update any other files using proto classes
- [ ] Verify gRPC service implementations extend new generated `*ImplBase` classes

### Test Updates
- [ ] Create `WireMockTestResource.java` for mocking connector-service and account-manager
- [ ] Update test imports to use new package names
- [ ] Update `ConnectorIntakeIntegrationTest.java`
- [ ] Update `BenchmarkIntakeTest.java`
- [ ] Remove any direct gRPC client dependencies in tests
- [ ] Update `src/test/resources/application.properties` property names

### Docker Configuration
- [ ] Update `Dockerfile.jvm` to use `ubi9-minimal:9.5`
- [ ] Update health check to use `/intake/q/health/ready`
- [ ] Ensure `curl-minimal` is used (pre-installed, no need to install `curl`)
- [ ] Verify port is `8080` (standard Quarkus port in container)

### CI/CD Updates
- [ ] Update `build-and-publish.yml` to use reusable workflow with `docker-image-name: connector-intake-service`
- [ ] Update `release-and-publish.yml` to use reusable workflow
- [ ] Verify `GITHUB_TOKEN` secret is available
- [ ] Verify Docker Hub credentials are configured for releases

### Verification
- [ ] Run `./gradlew clean build` - should sync protos and generate code
- [ ] Run `./gradlew quarkusDev` - service should start successfully
- [ ] Verify service registers with platform-registration-service
- [ ] Check Consul UI - service should appear with correct metadata
- [ ] Run `./gradlew test` - all tests should pass
- [ ] Verify Docker build works
- [ ] Test Docker container starts and registers correctly

---

## Assumptions Confirmed

✅ **Only connector-intake-service code and its config will be changed. No changes to shared platform modules or other services.**
- Confirmed: Migration scope is limited to `connector-intake-service` repository

✅ **Topic names and RPC contracts are stable; only package locations for protos have shifted.**
- Confirmed: No RPC contract changes, only package name changes (`ai.pipeline.*` → `ai.pipestream.*`)

✅ **Buf registries are publicly accessible to CI via `bufbuild/buf-action@v1` (no private auth needed), unless you advise otherwise.**
- Confirmed: All required registries (`buf.build/pipestreamai/intake`, `buf.build/pipestreamai/admin`) are public, no auth needed

---

## 13. Dynamic-gRPC API Migration (CRITICAL CHANGES)

**Status:** Major API changes in `quarkus-dynamic-grpc` from 0.7.1 to 0.7.2

### 13.1 Package Relocation
**Old Package:** `ai.pipestream.dynamic.grpc.client.*`
**New Package:** `ai.pipestream.quarkus.dynamicgrpc.*`

**Migration Required:**
```java
// OLD imports
import ai.pipestream.dynamic.grpc.client.DynamicGrpcClientFactory;
import ai.pipestream.dynamic.grpc.client.GrpcClientProvider;

// NEW imports
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import ai.pipestream.quarkus.dynamicgrpc.GrpcClientFactory;
```

### 13.2 API Simplification - Generic Client Method

The API moved from service-specific typed methods to a single generic method using method references.

**OLD API (0.7.1):**
```java
@Inject
DynamicGrpcClientFactory grpcClientFactory;

// Service-specific methods (NO LONGER EXIST)
return grpcClientFactory.getConnectorAdminServiceClient(serviceName)
    .flatMap(stub -> stub.validateApiKey(request));

return grpcClientFactory.getAccountServiceClient(serviceName)
    .flatMap(stub -> stub.getAccount(request));

return grpcClientFactory.getFilesystemServiceClient(serviceName)
    .flatMap(stub -> stub.createNode(request));

// Direct host:port connection (METHOD SIGNATURE CHANGED)
grpcClientProvider.getClient(
    MutinyNodeUploadServiceGrpc.MutinyNodeUploadServiceStub.class,
    host,
    port
);
```

**NEW API (0.7.2):**
```java
@Inject
DynamicGrpcClientFactory grpcClientFactory;  // Or GrpcClientFactory

// Generic method with method reference for stub creation
return grpcClientFactory.getClient(
    serviceName,
    MutinyConnectorAdminServiceGrpc::newMutinyStub
).flatMap(stub -> stub.validateApiKey(request));

return grpcClientFactory.getClient(
    serviceName,
    MutinyAccountServiceGrpc::newMutinyStub
).flatMap(stub -> stub.getAccount(request));

return grpcClientFactory.getClient(
    serviceName,
    MutinyFilesystemServiceGrpc::newMutinyStub
).flatMap(stub -> stub.createNode(request));

// For static host:port, use ManagedChannelBuilder directly
MutinyNodeUploadServiceGrpc.newMutinyStub(
    io.grpc.ManagedChannelBuilder
        .forTarget("static://" + host + ":" + port)
        .usePlaintext()
        .build()
);
```

### 13.3 Method Signature Changes

**Interface: `GrpcClientFactory`**
```java
// Generic client creation
<T extends MutinyStub> Uni<T> getClient(
    String serviceName,
    Function<Channel, T> stubCreator
);

// Raw channel access
Uni<Channel> getChannel(String serviceName);

// Cache management
int getActiveServiceCount();
void evictChannel(String serviceName);
String getCacheStats();
```

**Key Points:**
- All service-specific methods removed (`getConnectorAdminServiceClient`, `getAccountServiceClient`, etc.)
- Single generic `getClient` method replaces all service-specific methods
- Uses method references (e.g., `MutinyFooServiceGrpc::newMutinyStub`) instead of class literals
- Returns `Uni<T>` where T is the stub type, maintaining reactive semantics

### 13.4 Migration Examples

**Example 1: ConnectorValidationService**
```java
// OLD
import ai.pipestream.dynamic.grpc.client.DynamicGrpcClientFactory;

return grpcClientFactory.getConnectorAdminServiceClient(CONNECTOR_SERVICE_NAME)
    .flatMap(stub -> stub.validateApiKey(request));

// NEW
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import ai.pipestream.connector.intake.v1.MutinyConnectorAdminServiceGrpc;

return grpcClientFactory.getClient(
    CONNECTOR_SERVICE_NAME,
    MutinyConnectorAdminServiceGrpc::newMutinyStub
).flatMap(stub -> stub.validateApiKey(request));
```

**Example 2: Account Service Client**
```java
// OLD
return grpcClientFactory.getAccountServiceClient(ACCOUNT_SERVICE_NAME)
    .flatMap(stub -> stub.getAccount(request));

// NEW
import ai.pipestream.repository.v1.account.MutinyAccountServiceGrpc;

return grpcClientFactory.getClient(
    ACCOUNT_SERVICE_NAME,
    MutinyAccountServiceGrpc::newMutinyStub
).flatMap(stub -> stub.getAccount(request));
```

**Example 3: Static Host:Port Connection (Repository Service)**
```java
// OLD
import ai.pipestream.dynamic.grpc.client.GrpcClientProvider;

@Inject
GrpcClientProvider grpcClientProvider;

repoService = grpcClientProvider.getClient(
    MutinyNodeUploadServiceGrpc.MutinyNodeUploadServiceStub.class,
    repoServiceHost,
    port
);

// NEW - Use ManagedChannelBuilder directly for static connections
repoService = MutinyNodeUploadServiceGrpc.newMutinyStub(
    io.grpc.ManagedChannelBuilder
        .forTarget("static://" + repoServiceHost + ":" + port)
        .usePlaintext()
        .build()
);
```

### 13.5 Required Import Additions

When updating to the new API, add imports for the Mutiny service stubs:

```java
// Add these imports for each service you call
import ai.pipestream.connector.intake.v1.MutinyConnectorAdminServiceGrpc;
import ai.pipestream.repository.v1.account.MutinyAccountServiceGrpc;
import ai.pipestream.repository.v1.filesystem.MutinyFilesystemServiceGrpc;
import ai.pipestream.repository.v1.filesystem.upload.MutinyNodeUploadServiceGrpc;
```

### 13.6 Proto API Breaking Changes

**Repository Upload API:**
The `uploadPipeDoc` method in `MutinyNodeUploadServiceGrpc` changed from accepting a `PipeDoc` directly to requiring an `UploadPipeDocRequest` wrapper.

**OLD API:**
```java
// Repository service accepted PipeDoc directly
getRepoService().uploadPipeDoc(pipeDoc)
```

**NEW API:**
```java
// Must wrap PipeDoc in UploadPipeDocRequest
ai.pipestream.repository.v1.filesystem.upload.UploadPipeDocRequest repoRequest =
    ai.pipestream.repository.v1.filesystem.upload.UploadPipeDocRequest.newBuilder()
        .setDocument(pipeDoc)
        .build();

getRepoService().uploadPipeDoc(repoRequest)
```

**Response Type Changes:**
- Returns: `ai.pipestream.repository.v1.filesystem.upload.UploadPipeDocResponse`
- Contains: `getSuccess()`, `getDocumentId()`, `getMessage()`

**Connector Intake Response Types:**
The connector-intake-service API no longer has a generic `UploadResponse`. Use specific response types:

- `UploadPipeDocResponse` for `uploadPipeDoc` method (field: `setDocId` not `setDocumentId`)
- `UploadBlobResponse` for `uploadBlob` method (field: `setDocId` not `setDocumentId`)

**Example:**
```java
// Map repository response to connector response
.map(repoResponse -> UploadPipeDocResponse.newBuilder()
    .setSuccess(repoResponse.getSuccess())
    .setDocId(repoResponse.getDocumentId())  // Note: setDocId on intake, getDocumentId from repo
    .setMessage(repoResponse.getMessage())
    .build())
```

### 13.7 Filesystem Service Node Type Enum

The `NodeType` enum values changed to include prefix:

**OLD:**
```java
.setType(Node.NodeType.FILE)
```

**NEW:**
```java
.setType(Node.NodeType.NODE_TYPE_FILE)
```

Available values: `NODE_TYPE_UNSPECIFIED`, `NODE_TYPE_FOLDER`, `NODE_TYPE_FILE`

### 13.8 Account Response Structure

The `GetAccountResponse` no longer exposes account fields directly - use the nested `Account` object:

**OLD:**
```java
.flatMap(account -> {
    if (!account.getActive()) {
        // ...
    }
})
```

**NEW:**
```java
.flatMap(response -> {
    Account account = response.getAccount();
    if (!account.getActive()) {
        // ...
    }
})
```

### 13.9 CreateNode Response Structure

Similar change for `CreateNodeResponse` - access the `Node` object:

**OLD:**
```java
.map(response -> {
    String docId = response.getDocumentId();
})
```

**NEW:**
```java
.map(response -> {
    Node node = response.getNode();
    String docId = node.getDocumentId();
})
```

### 13.10 Migration Checklist for Dynamic-gRPC

- [ ] Update all `import ai.pipestream.dynamic.grpc.client.*` to `import ai.pipestream.quarkus.dynamicgrpc.*`
- [ ] Replace `DynamicGrpcClientFactory.getXxxServiceClient(serviceName)` with `getClient(serviceName, MutinyXxxServiceGrpc::newMutinyStub)`
- [ ] Add imports for all Mutiny service stub classes (`MutinyXxxServiceGrpc`)
- [ ] Replace static host:port connections with `ManagedChannelBuilder` or migrate to Stork
- [ ] Wrap `PipeDoc` in `UploadPipeDocRequest` for repository uploads
- [ ] Update response handling to access nested objects (`response.getAccount()`, `response.getNode()`)
- [ ] Update `NodeType` enum references to include `NODE_TYPE_` prefix
- [ ] Replace generic `UploadResponse` with specific `UploadPipeDocResponse` / `UploadBlobResponse`
- [ ] Update response builders to use `setDocId` instead of `setDocumentId` for connector-intake responses

### 13.11 Benefits of New API

- **Type Safety:** Method references ensure compile-time validation of stub creators
- **Simplicity:** Single generic method instead of multiple service-specific methods
- **Flexibility:** Easy to add new service clients without modifying the factory interface
- **Zero Reflection:** Method references are more efficient than class-based lookup
- **Clear Separation:** Stork-based discovery vs. static host:port patterns are more explicit

---



