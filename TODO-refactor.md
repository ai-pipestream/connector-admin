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
                  DockerImageName.parse("docker.io/pipestreamai/pipestream-wiremock-server:0.1.8"))
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

- All services should use the same BOM version (`0.7.2` or latest)
- Protobuf generation is now handled via Buf, not local proto files
- Apicurio configuration is automatic via extension annotations (`@ProtobufChannel`, `@ProtobufIncoming`)
- Service registration is automatic via extension (zero-config)
- All services share the same CI/CD workflows for consistency
- Multi-arch Docker builds (linux/amd64,linux/arm64) are automatic via reusable workflows
- CVE-free Docker images using `ubi9-minimal:9.5` with `microdnf update -y`
- WireMock containers replace direct gRPC dependencies in tests
- Health checks use pre-installed `curl-minimal` (supports `-f` flag)
