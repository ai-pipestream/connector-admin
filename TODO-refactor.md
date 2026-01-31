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
- For S3/claim-check services, prefer **AWS S3 client semantics**. Use an S3-compatible backend (e.g., MinIO) only as an implementation target for local dev/testing, but avoid coupling the service to MinIO-specific client APIs.

### 1.6 Javadoc noise (generated sources)

We want to keep Javadoc quality high for **our application code** without letting generated protobuf/gRPC sources slow down or block refactors.

**Standard policy:**
- **Local/dev**: run Javadoc/doclint as **warn-only** (do not fail the build).
- **CI**: add a **separate job** that runs strict Javadoc and fails if our code has Javadoc lint errors.
- **Generated sources** must be excluded from Javadoc inputs in both cases.

**Gradle pattern (exclude generated code; warn-only locally):**

```groovy
tasks.withType(Javadoc).configureEach { task ->
  // Generated protobuf/gRPC sources must not participate in Javadoc linting
  task.exclude('**/v1/**')
  task.exclude('**/generated/**')
  task.exclude('**/generated/source/proto/**')
  task.exclude('**/quarkus-generated-sources/**')

  // Local/dev: do not fail the build on Javadoc issues
  task.failOnError = false
}
```

**CI: strict Javadoc as a dedicated job**
- Keep the main build/test jobs fast.
- Add a dedicated CI job/step that runs Javadoc in strict mode (fails the job).
- Easiest implementation: add a `-PstrictJavadoc=true` flag and flip `failOnError` to `true` only when that property is set.

Example GitHub Actions job (conceptual; adapt to your repo/workflows):

```yaml
jobs:
  javadoc-strict:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          java-version: '21'
          distribution: temurin
      - run: ./gradlew javadoc -PstrictJavadoc=true --no-daemon --stacktrace
```

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

If you consume CDI/config-mapped beans from libraries that are not Quarkus extensions, add `quarkus.index-dependency.*`.

Note: `pipestream-service-registration` and `quarkus-dynamic-grpc` now register their own index dependencies in their extensions, so per-service config is not required.

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
- Using an S3-compatible backend (e.g., MinIO) for claim-check testing while keeping the application’s storage API AWS S3 compatible (no MinIO-specific client coupling)

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
- Publishes **snapshot Maven artifacts** (GitHub Packages + Maven Central snapshots, when configured) and pushes **GHCR snapshot images**
- Multi-arch (amd64/arm64)
- SBOM + provenance enabled
- Typical image tags include branch+SHA and SHA (exact tagging is defined by the org reusable workflow)

### 4.2 Release (`release-and-publish.yml`)

- `workflow_dispatch` with `patch/minor/major/manual`
- Creates tag + GitHub Release
- Publishes versioned artifacts to **Maven Central via NMCP** + GitHub Packages (with GPG signing)
- Pushes multi-arch images to **GHCR + Docker Hub** (release)
- Release image tags are versioned (e.g. `0.x.y`) and `latest` (exact tagging is defined by the org reusable workflow)

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

## 6.1 gRPC large payload performance: HTTP/2 flow-control window (Quarkus unified server)

When running gRPC in **unified server mode**:

```properties
quarkus.grpc.server.use-separate-server=false
```

gRPC runs on the main Vert.x HTTP server. For **large messages** (multi‑MB / tens of MB), throughput can degrade significantly because HTTP/2 flow control defaults are conservative (e.g., 64KB windows). As of Quarkus 3.x, Quarkus does **not** expose a supported configuration knob for the unified server’s HTTP/2 flow-control window.

See: [Quarkus issue #51129](https://github.com/quarkusio/quarkus/issues/51129).

### Recommended temporary standard (until Quarkus exposes the setting)

We prefer to use Vert.x/unified mode long-term, but until Quarkus exposes these HTTP/2 tunables, we use a **temporary per-service workaround class** for services that push large gRPC payloads:

- Add a small “VertxHttp2Tuning” class **in the service repo** (copied across services as needed).\n- It should run at startup and attempt to set the HTTP/2 initial window size on the underlying Vert.x HTTP server options.\n- This is inherently somewhat tied to Quarkus internals. Keep it small, isolated, and well-commented.\n\nIf the workaround cannot be applied safely in a given Quarkus version, the fallback is to use the separate gRPC server for that service (see below).
- Add a small “VertxHttp2Tuning” class **in the service repo** (copied across services as needed).
- It should run at startup and attempt to set the HTTP/2 initial window size on the underlying Vert.x HTTP server options.
- This is inherently somewhat tied to Quarkus internals. Keep it small, isolated, and well-commented.

If the workaround cannot be applied safely in a given Quarkus version, the fallback is to use the separate gRPC server for that service (see below).

### Fallback (supported): separate gRPC server (Netty) for large payload services

For services where we must reliably tune throughput today, run gRPC on the separate Netty server:

```properties
quarkus.grpc.server.use-separate-server=true
```

Then tune the flow-control window using the Netty server builder customizer (supported approach):

```java
// Example only: keep settings centralized and configurable per service/container
// NettyServerBuilder exposes initialFlowControlWindow(...)
```

This avoids the unified-server limitation, at the cost of running the extra Netty gRPC server.

**Important**: don’t try to “fix” this by pinning Netty/Vert.x versions directly; upgrade via the BOM instead.

## 7) Repository-service conversion notes (align before coding)

Repository-service is the most complex service because it implements **claim-check storage**, large payload handling, and repository metadata semantics. Below is what we’ve already aligned on, and what we still need to confirm by inspecting the codebase/protos.

### 7.0 Confirmed direction (captured from prior attempts)

- **Who calls repo-service (v1)**: repo-service is expected to be called by **connector-intake-service and the engine**. Processing modules are **not** repository-aware and must not mutate object references; the engine will hydrate/dehydrate objects around module execution.

- **Storage backend**:
  - The contract is **AWS S3 compatible API** (even when running MinIO locally).
  - MinIO is treated as an S3-compatible backend only; we avoid coupling to MinIO-specific client APIs.
  - **Filesystem is not a backend** (distributed system).
  - We prefer S3 over Redis for cost/reliability reasons.

- **Payload shape and upload size**:
  - Most stored objects are protobuf payloads (e.g., `.pb`) stored in S3.
  - Uploads will be **chunked/multipart**; parts can be **> 5MB**.
  - Upload APIs likely need streaming or explicit chunk semantics.
  - A future capability is to support **HTTP form POST uploads** as an ingress method (not implemented yet).

- **Encryption strategy**:
  - Target state: all data stored in S3 should be **encrypted at rest** using a **client-provided key**.
  - Initial implementation: store unencrypted (null key) but **log explicitly** at the “encryption step” so it’s visible in production.
  - Key sharing/management will integrate with **account-service**.

- **Metadata ownership and indexing**:
  - Object-store metadata is limited; repo-specific metadata should be **authoritative in the DB**.
  - Minimal non-sensitive reference metadata (e.g., SHA) can be stored with the object.
  - Repo metadata changes should emit events so **opensearch-manager** can index for analysis.

- **Database**:
  - MySQL works today; a move to **Postgres** is desired but should likely happen **after** service conversions are complete.

- **Kafka/Apicurio**:
  - Nothing is live today.
  - When Kafka is needed, use protobuf objects + the **Apicurio protobuf extension** (no manual serde).
  - `Any` payloads may need explicit validation/descriptor checks.

- **Testing**:
  - Repo-service will be verified via an integration test using the **sample data doc repo**.
  - Any outbound gRPC dependency should be mocked using our **WireMock container**.

- **Health/readiness**:
  - gRPC health is required (Consul/service mesh depends on it). Use Quarkus health to expose the standard gRPC health endpoint.

### 7.1 Needs investigation (must be answered by code/proto inspection)

- Enumerate the repo-service gRPC APIs and decide what is truly **public** vs internal.
- Identify upload/download RPCs and confirm whether they are streaming; capture message/window sizing requirements.
- List which proto modules/subdirs are needed (`repo`, `common`, etc.) and which APIs are versioned.
- Locate DB tables/migrations for repository state/metadata and confirm Flyway strategy.
- Confirm outbox/status tables and define which events will be emitted for opensearch indexing.
- Document the “three upload modes” and which are v1 vs later.
- Confirm bucket ownership/tenant bucket model (including “stop indexing” semantics when access is revoked).

---

## 8) Next step

After you review this document, we’ll create a concrete repo-service conversion plan (tasks + sequencing) and then start the migration.
