# Service Migration Checklist (Recipe Book)

This document is the **authoritative checklist** for migrating any Pipestream service from the old platform patterns to the **current standardized architecture**.

It is intentionally opinionated: if you follow this, you end up with:
- Consistent Gradle + BOM usage
- Consistent protobuf / gRPC codegen via our **proto-toolchain plugin** (no hand-maintained stubs)
- Consistent Kafka + Apicurio integration via our **Apicurio Protobuf extension** (no manual serializers)
- Consistent multi-arch Docker images (SBOM/provenance, hardened base image)
- Consistent CI/CD via shared reusable workflows

## Current “known good” baseline (use these as references)

- **`account-service`**: end-to-end baseline for CI/CD, publishing, Docker builds, and service registration.
- **`connector-admin`**: baseline for dynamic-gRPC + Kafka/Apicurio + DB + service registration.
- **`mapping-service`**: baseline for lift-and-shift utilities, JaCoCo exclusions for generated protos, and the new release workflow wiring.

## Versions (keep these consistent)

- **pipestream BOM**: `pipestreamBomVersion=0.7.8` (single source of truth per repo via `gradle.properties`)
- **Quarkus**: managed by the BOM (currently **3.30.3** via `pipestream-bom`)
- **Proto toolchain plugin**: use `alias(libs.plugins.proto.toolchain)` (version comes from the BOM catalog)
- **Java**: **21**

If the BOM changes, the Quarkus + gRPC/Netty ecosystem changes with it. Prefer upgrading BOM rather than pinning individual transitive deps.

---

## 1) Build configuration (`gradle.properties`, `settings.gradle`, `build.gradle`)

### 1.1 `gradle.properties`

- Set:

```properties
pipestreamBomVersion=0.7.8
```

### 1.2 `settings.gradle`

- Ensure the BOM catalog is used (exact coordinates may vary by repo template):
  - `ai.pipestream:pipestream-bom-catalog:${pipestreamBomVersion}`
- Ensure repos are present for:
  - Maven Central
  - GitHub Packages (for `ai.pipestream` artifacts)
  - Sonatype snapshots (only if you consume snapshot artifacts)

### 1.3 `build.gradle` plugins

Use the standardized plugin set:

```groovy
plugins {
  alias(libs.plugins.java)
  alias(libs.plugins.quarkus)
  alias(libs.plugins.maven.publish)
  id 'signing'
  alias(libs.plugins.nmcp.single)
  alias(libs.plugins.axion.release)
  alias(libs.plugins.proto.toolchain)
}
```

### 1.4 Protobuf / gRPC codegen (NEW STANDARD)

We no longer maintain local stubs and we no longer run ad-hoc `buf export` tasks in each service.

Instead, we use our **proto toolchain plugin**:

- Fetches protos from `pipestream-protos` (git)
- Generates Java + gRPC (including Mutiny stubs)
- Uses Buf internally (you don’t need to wire Quarkus codegen properties)

Example:

```groovy
pipestreamProtos {
  sourceMode = 'git'
  modules {
    register("admin") {
      gitRepo = "https://github.com/ai-pipestream/pipestream-protos.git"
      gitRef = "main"
      gitSubdir = "admin"
    }
    register("intake") {
      gitRepo = "https://github.com/ai-pipestream/pipestream-protos.git"
      gitRef = "main"
      gitSubdir = "intake"
    }
    // Add the modules your service actually needs:
    // - common
    // - registration
    // - repo
    // - pipeline-module
  }
}
```

**Remove/avoid** these old patterns:
- `buf export ...` tasks in Gradle
- `quarkus.grpc.codegen.proto-directory=...`
- `quarkus.grpc.codegen.enabled=true`
- Any committed/generated “grpc stubs” libraries

### 1.5 Dependencies

**Always import the BOM**:

```groovy
implementation platform("ai.pipestream:pipestream-bom:${pipestreamBomVersion}")
```

**Core runtime (typical service)**:
- `io.quarkus:quarkus-container-image-docker`
- `io.quarkus:quarkus-arc`
- `io.quarkus:quarkus-grpc`
- `io.quarkus:quarkus-smallrye-health`
- `io.quarkus:quarkus-smallrye-openapi`
- `io.quarkus:quarkus-smallrye-stork`
- `ai.pipestream:quarkus-dynamic-grpc`
- `ai.pipestream:pipestream-service-registration`
- `ai.pipestream:pipestream-quarkus-devservices`

**Kafka + schema registry (NEW STANDARD)**:
- Add only when the service actually produces/consumes Kafka messages:

```groovy
implementation 'io.quarkus:quarkus-messaging-kafka'
implementation 'ai.pipestream:quarkus-apicurio-registry-protobuf'
```

Then in code:
- Producers: `@ProtobufChannel("channel")`
- Consumers: `@ProtobufIncoming("channel")`

**Do NOT** manually configure:
- `value.serializer` / `value.deserializer`
- `mp.messaging.connector.smallrye-kafka.apicurio.registry.url`
- Connector-level Apicurio settings in main profile

The extension auto-detects protobuf types and configures:
- serializer/deserializer
- registry integration
- channel defaults

**DevServices / infra**
- Prefer Quarkus DevServices + our extensions over custom compose stacks.
- For S3/claim-check services, consider `io.quarkus:quarkus-minio` and DevServices (so we can remove test compose later).

### 1.6 Javadoc noise (generated sources)

We standardize on **not failing builds** due to doclint noise in generated code:
- Set `failOnError = false`
- Disable doclint (or at least suppress)
- Exclude generated paths (e.g. `**/v1/**`, `**/generated/source/proto/**`)

---

## 2) Application config (`src/main/resources/application.properties`)

### 2.1 Service identity and ports

Standard container port is **8080**. Dev profile can override to avoid collisions.

```properties
quarkus.http.port=8080
quarkus.http.host=0.0.0.0
quarkus.http.root-path=/YOUR_ROOT
quarkus.grpc.server.use-separate-server=false
quarkus.grpc.server.enable-reflection-service=true
```

### 2.2 Service registration (NEW STANDARD)

Replace any legacy `service.registration.*` with:

```properties
pipestream.registration.enabled=true
pipestream.registration.service-name=YOUR_SERVICE_NAME
pipestream.registration.description=...
pipestream.registration.type=SERVICE  # or MODULE
pipestream.registration.advertised-host=${YOUR_SERVICE_HOST:host.docker.internal}
pipestream.registration.advertised-port=${quarkus.http.port}
pipestream.registration.internal-host=${DOCKER_BRIDGE_IP:172.17.0.1}
pipestream.registration.internal-port=${quarkus.http.port}
pipestream.registration.capabilities=...
pipestream.registration.tags=...

pipestream.registration.registration-service.host=localhost
pipestream.registration.registration-service.port=38101

%test.pipestream.registration.enabled=false
```

### 2.3 Quarkus indexing for non-extension deps (CRITICAL)

If you consume CDI/config-mapped beans from libraries that are not Quarkus extensions, add:

```properties
quarkus.index-dependency.pipestream-registration.group-id=ai.pipestream
quarkus.index-dependency.pipestream-registration.artifact-id=pipestream-service-registration
quarkus.index-dependency.pipestream-dynamic-grpc.group-id=ai.pipestream
quarkus.index-dependency.pipestream-dynamic-grpc.artifact-id=quarkus-dynamic-grpc
```

### 2.4 DevServices (Dev profile)

We keep a shared **dev** compose stack (for local dev quality of life). Example:

```properties
%dev.quarkus.compose.devservices.enabled=true
%dev.quarkus.compose.devservices.files=${user.home}/.pipeline/compose-devservices.yml
%dev.quarkus.compose.devservices.project-name=pipeline-shared-devservices
%dev.quarkus.compose.devservices.start-services=true
%dev.quarkus.compose.devservices.stop-services=false
%dev.quarkus.compose.devservices.reuse-project-for-tests=true
%dev.quarkus.devservices.timeout=120s
```

### 2.5 Test infra (future simplification)

We can likely eliminate service-specific `compose-test-services.yml` in many repos by:
- Using Quarkus DevServices extensions (MySQL, Kafka)
- Using our Apicurio Protobuf extension (registry)
- Using Quarkus MinIO extension for claim-check services

Keep test compose **only** when you truly need a bespoke integration topology.

---

## 3) Docker image (`src/main/docker/Dockerfile.jvm`)

### 3.1 Hardened JVM Dockerfile (CURRENT STANDARD)

- Base: `registry.access.redhat.com/ubi9-minimal:9.5`
- Install: `java-21-openjdk-headless`, `ca-certificates`
- `microdnf update -y` to pull CVE fixes
- Run as non-root user `185`
- Healthcheck uses **curl-minimal** (already present)

Healthcheck path should match your root-path + health configuration:
- If using default Quarkus health endpoints: `/YOUR_ROOT/q/health`
- If you set `quarkus.smallrye-health.root-path=/health`: `/YOUR_ROOT/health`

---

## 4) CI/CD standardization (`.github/workflows`)

We now standardize on org-level reusable workflows:

### 4.1 Build + snapshots (`build-and-publish.yml`)

- Runs on PR + main
- Builds/tests
- Publishes snapshots (Maven) and pushes GHCR snapshot images
- Multi-arch (amd64/arm64)
- SBOM + provenance enabled

### 4.2 Release (`release-and-publish.yml`)

- `workflow_dispatch` with `patch/minor/major/manual`
- Creates tag + GitHub Release
- Publishes versioned artifacts to **Maven Central via NMCP** + GitHub Packages
- Pushes multi-arch images to **GHCR + Docker Hub** (release)

### 4.3 Dependabot + release notes

- Add `.github/dependabot.yml` (Gradle + GitHub Actions)
- Add `.github/release.yml` (categorized autogenerated release notes)

---

## 5) Verification checklist

- `./gradlew clean build`
- `./gradlew test`
- `./gradlew quarkusDev`
- `./gradlew build -Dquarkus.container-image.build=true`
- Run the container and verify:
  - health endpoint
  - service registers with platform-registration-service
  - any Kafka channels work (if applicable)

---

## 6) Common “gotchas” (things that used to waste days)

- **Don’t pin Netty/Vert.x directly** to fix CVEs. Upgrade the BOM.
- **Don’t hand-maintain proto stubs**. Use the proto-toolchain.
- **Don’t configure Kafka serializers/deserializers**. Use the Apicurio protobuf extension.
- **If CDI injection fails** for config-mapped beans coming from libs: add `quarkus.index-dependency.*`.
- **Avoid parallel `clean` + `test` locally** in the same checkout (it can delete outputs while tests run).

---

## 7) Repository-service conversion questions (align on these before coding)

Repository-service is the most complex service because it implements the claim-check pattern, storage abstraction, and large-payload handling. Before we start the conversion, let’s align on:

### 7.1 Protobuf modules and API surface
- Which proto modules are required (likely `repo`, plus `common`, and any shared `data` protos it imports)?
- Which public gRPC services are exposed (filesystem, upload, metadata, admin, internal-only endpoints)?
- Any streaming RPCs (upload/download) and max message/window size requirements.

### 7.2 Storage / claim-check design
- Storage backend(s): S3, MinIO (dev), filesystem, or both?
- Do we need server-side encryption, object naming conventions, retention policies, or lifecycle rules?
- What metadata is stored in DB vs embedded in object store?

### 7.3 Datastore + migrations
- DB type (MySQL) and schema migration strategy (Flyway): any legacy tables/constraints we need to keep?
- Are we using outbox/event tables for publishing “document stored” events?

### 7.4 Kafka + Apicurio usage
- What topics/events does repository-service publish/consume today?
- Are we using the Apicurio protobuf extension end-to-end (no manual serde), and do we need compatibility checks?

### 7.5 Test strategy (simplify vs compose)
- Can we eliminate `compose-test-services.yml` by using:
  - Quarkus DevServices for MySQL + Kafka
  - our Apicurio protobuf extension (registry)
  - Quarkus MinIO extension (claim-check)
- Which external gRPC deps should be mocked via WireMock container?

### 7.6 Docker + production readiness
- Root path, health endpoint path, and any readiness/liveness semantics.
- Image tags expected (GHCR snapshots, GHCR+DockerHub releases).

---

## 8) Next step

After you review this document, we’ll create a concrete repo-service conversion plan (tasks + sequencing) and then start the migration.
