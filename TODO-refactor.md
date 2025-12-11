# Service Migration Checklist: Converting to New Pipestream Architecture

This document provides a comprehensive checklist for migrating any Pipestream service to the new architecture. Use `account-service` as the reference implementation.

**IMPORTANT**: Before starting, review `account-service` to see the completed pattern. This checklist ensures no steps are missed.

## Prerequisites

- [ ] Verify the service's protobuf definitions are available in the appropriate Buf registry:
  - `buf.build/pipestreamai/admin` - Account, Admin services
  - `buf.build/pipestreamai/intake` - Connector, Intake services
  - `buf.build/pipestreamai/registration` - Registration service
  - Check which registry contains your service's proto definitions

## 1. Build Configuration (`build.gradle`)

### 1.1 Update BOM Version
- [ ] Update `pipestreamBomVersion` to `0.7.1` (or latest stable version)
- [ ] Ensure BOM platform dependency is present:
  ```groovy
  implementation platform("ai.pipestream:pipestream-bom:${pipestreamBomVersion}")
  ```

### 1.2 Update Dependencies
- [ ] **ADD** Apicurio Registry Protobuf extension:
  ```groovy
  implementation libs.quarkus.apicurio.registry.protobuf
  ```
- [ ] **ADD** Service Registration extension (BOM-managed, no version):
  ```groovy
  implementation 'ai.pipestream:pipestream-service-registration'
  ```
- [ ] **ADD** Dynamic gRPC extension (BOM-managed, no version):
  ```groovy
  implementation 'ai.pipestream:quarkus-dynamic-grpc'
  ```
- [ ] **REMOVE** any explicit internal stubs (e.g., `libs.pipestream.grpc.stubs`) - protos will be generated locally
- [ ] **REMOVE** any manual Apicurio serializer/deserializer dependencies - handled by extension

### 1.3 Configure Protobuf Generation via Buf
- [ ] Add `bufExportDir` definition:
  ```groovy
  def bufExportDir = layout.buildDirectory.dir("generated/buf-protos")
  ```
- [ ] Add `syncProtos` task(s) for your service's proto registry:
  ```groovy
  tasks.register('syncProtos', Exec) {
      group = 'protobuf'
      description = 'Exports proto files from Buf Registry'
      commandLine 'buf', 'export', 'buf.build/pipestreamai/YOUR_REGISTRY', 
                   '--output', bufExportDir.get().asFile.path
      outputs.dir(bufExportDir)
      doFirst { bufExportDir.get().asFile.mkdirs() }
  }
  ```
  - Replace `YOUR_REGISTRY` with the appropriate registry (admin, intake, etc.)
  - If multiple registries are needed, create separate tasks and combine them
- [ ] Configure Quarkus to use exported protos:
  ```groovy
  quarkus {
      quarkusBuildProperties.put("quarkus.grpc.codegen.proto-directory", bufExportDir.get().asFile.path)
      quarkusBuildProperties.put("quarkus.grpc.codegen.enabled", "true")
  }
  ```
- [ ] Update source sets for IDE support:
  ```groovy
  sourceSets.main.java.srcDir layout.buildDirectory.dir("classes/java/quarkus-generated-sources/grpc")
  ```
- [ ] Hook proto sync to code generation:
  ```groovy
  tasks.named('quarkusGenerateCode').configure { dependsOn 'syncProtos' }
  tasks.named('quarkusGenerateCodeDev').configure { dependsOn 'syncProtos' }
  tasks.named('quarkusGenerateCodeTests').configure { dependsOn 'syncProtos' }
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
- [ ] **REMOVE** manual `quarkus.index-dependency.apicurio-registry.*` entries (handled by extension)
- [ ] **REMOVE** manual `mp.messaging.*.apicurio.registry.url` overrides in main resources
- [ ] **KEEP** dev/test profile overrides if needed:
  ```properties
  %dev.mp.messaging.connector.smallrye-kafka.apicurio.registry.url=http://localhost:8081/apis/registry/v3
  %prod.mp.messaging.connector.smallrye-kafka.apicurio.registry.url=http://apicurio-registry:8080/apis/registry/v3
  ```
- [ ] **REMOVE** manual `value.serializer`/`value.deserializer` config from main resources (handled by extension)

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

- [ ] Update to use optimized base image:
  ```dockerfile
  FROM registry.access.redhat.com/ubi9-minimal:9.5
  ```
- [ ] Install headless JDK:
  ```dockerfile
  RUN microdnf update -y && \
      microdnf install -y java-21-openjdk-headless && \
      microdnf clean all
  ```
- [ ] Use direct `java -jar` entrypoint (not `run-java.sh`)
- [ ] Add HEALTHCHECK for gRPC health endpoint
- [ ] Expose both HTTP and gRPC ports if applicable
- [ ] Add OCI labels (org.opencontainers.image.*)
- [ ] Add JVM container tuning (-XX:+UseContainerSupport, -XX:MaxRAMPercentage)
- [ ] Set timezone to UTC

## 4. Test Configuration

### 4.1 Docker Compose Test Services (`src/test/resources/compose-test-services.yml`)
- [ ] **REMOVE** manual Apicurio config map
- [ ] **ADD** extension label for Apicurio:
  ```yaml
  labels:
    quarkus-dev-service-apicurio-registry-protobuf: test
  ```

### 4.2 Test Resources
- [ ] **DELETE** any manual TestResource classes (e.g., `ApicurioTestResource.java`)
- [ ] **REMOVE** `@QuarkusTestResource` annotations referencing deleted resources
- [ ] Update WireMock test resources to use new registration properties if applicable

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

- [ ] **build-and-publish.yml**: Use reusable workflow:
  ```yaml
  uses: ai-pipestream/.github/.github/workflows/reusable-service-build.yml@main
  ```
- [ ] **release-and-publish.yml**: Use reusable workflow:
  ```yaml
  uses: ai-pipestream/.github/.github/workflows/reusable-service-release.yml@main
  ```
- [ ] **dockerhub-publish.yml**: Use reusable workflow:
  ```yaml
  uses: ai-pipestream/.github/.github/workflows/reusable-dockerhub-publish.yml@main
  ```
- [ ] Ensure workflows use multi-arch Docker builds (linux/amd64,linux/arm64)
- [ ] Verify Buf setup is included for protobuf generation

## 7. Settings Configuration (`settings.gradle`)

- [ ] Verify BOM catalog is configured:
  ```groovy
  dependencyResolutionManagement {
      versionCatalogs {
          libs {
              from("ai.pipestream:pipestream-bom-catalog:0.7.1")
          }
      }
  }
  ```
- [ ] Ensure Maven repositories include:
  - `mavenLocal()` (for local development)
  - `mavenCentral()`
  - GitHub Packages (with proper authentication)
  - Sonatype snapshots (if needed)

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

## Notes

- All services should use the same BOM version (`0.7.1` or latest)
- Protobuf generation is now handled via Buf, not local proto files
- Apicurio configuration is automatic via extension annotations
- Service registration is automatic via extension (zero-config)
- All services share the same CI/CD workflows for consistency
