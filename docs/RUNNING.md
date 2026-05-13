# Running the Connector Admin Service

This guide explains how to start the connector-admin service locally, call its gRPC APIs
with `grpcurl`, and simulate the complete document-ingestion workflow — from creating a
datasource to validating the API key that a connector uses when it delivers a document.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Start the Service](#start-the-service)
3. [Concepts: Connectors, DataSources, and Document Upload](#concepts)
4. [Quick-start: Full Workflow in 5 Steps](#quick-start-full-workflow-in-5-steps)
5. [Detailed API Reference](#detailed-api-reference)
6. [Running Against a Document — the Intake Flow](#running-against-a-document--the-intake-flow)
7. [Environment Variables](#environment-variables)
8. [Troubleshooting](#troubleshooting)

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Java | 21+ | Build & runtime |
| Gradle | 8+ (wrapper included) | Build tool |
| Docker | 24+ | Dev-services (PostgreSQL, Kafka) |
| `grpcurl` | any | Calling gRPC APIs from the terminal |
| `jq` | 1.6+ | Parsing JSON responses in shell scripts |

Install `grpcurl` on macOS with Homebrew:

```bash
brew install grpcurl
```

On Linux, download the binary from https://github.com/fullstorydev/grpcurl/releases.

---

## Start the Service

### Dev mode (recommended for local development)

Dev mode starts Quarkus with live reload, auto-provisions a PostgreSQL container and a
Kafka broker via Docker Compose DevServices, and runs Flyway migrations automatically.

```bash
./gradlew quarkusDev
```

The service listens on **`localhost:18107`** (HTTP + gRPC on the same port, multiplexed
by the Quarkus HTTP layer).

> **Note:** The first start may take 1–2 minutes while Docker images are pulled.

### Running the packaged JAR

Build a native-optimised JVM Uber-JAR first:

```bash
./gradlew quarkusBuild
```

Then start with an external PostgreSQL and Kafka:

```bash
DB_HOST=localhost DB_PORT=5432 DB_NAME=connector_admin \
DB_USER=pipeline DB_PASSWORD=password \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
java -jar build/quarkus-app/quarkus-run.jar
```

### Running unit tests

```bash
./gradlew test
```

Quarkus starts a PostgreSQL and a Kafka container automatically (Testcontainers) so no
external infrastructure is needed.

### Running integration tests (against the packaged JAR)

```bash
./gradlew integrationTest
```

The `integrationTest` task builds the Uber-JAR, starts it in a child process, and runs
tests annotated with `@QuarkusIntegrationTest` against it.

---

## Concepts

### Connectors

A **Connector** is a template (e.g., `s3`, `file-crawler`, `jdbc`) that describes what
kind of external data source can be crawled.  Connectors are seeded once at startup and
are shared across all accounts.

### DataSources

A **DataSource** is an account-specific binding to a connector type.  When you create a
datasource you receive a one-time plaintext **API key**.  That key is the credential an
external crawler presents when it submits documents to the intake service.

### Document Upload Flow

```
                        ┌───────────────────┐
  Admin / DevOps        │  connector-admin  │
  ─────────────────────►│                   │
  1. CreateDataSource   │  (this service)   │
  ◄─────────────────────│                   │
  2. Receive API key    └───────────────────┘
                                │
                                │ API key stored as
                                │ Argon2id hash
                                ▼
                        ┌───────────────────┐
  Crawler / Connector   │  intake service   │
  ─────────────────────►│                   │
  3. POST /ingest       │  validates key    │
     (with API key)     │  via gRPC call ──►│─── ValidateApiKey ──►
                        │  to connector-    │                       connector-admin
                        │  admin            │◄── DataSourceConfig ──
                        └───────────────────┘
```

Steps 1–2 happen in **connector-admin** (this service).  Step 3 happens in the
`connector-intake` service, which calls `ValidateApiKey` here to authenticate each
document upload.

---

## Quick-start: Full Workflow in 5 Steps

Make sure the service is running (`./gradlew quarkusDev`), then:

```bash
HOST="localhost:18107"
SVC="ai.pipestream.connector.intake.v1.DataSourceAdminService"
S3_CONNECTOR="a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"   # pre-seeded

# 1. List available connector types
grpcurl -plaintext $HOST $SVC/ListConnectorTypes | jq .

# 2. Create a datasource — save the API key, it is returned ONCE
RESPONSE=$(grpcurl -plaintext -d "{
  \"account_id\": \"my-account-001\",
  \"connector_id\": \"$S3_CONNECTOR\",
  \"name\": \"Production S3 bucket\",
  \"drive_name\": \"primary-drive\"
}" $HOST $SVC/CreateDataSource)

DATASOURCE_ID=$(echo "$RESPONSE" | jq -r '.datasource.datasourceId')
API_KEY=$(echo "$RESPONSE" | jq -r '.datasource.apiKey')

echo "DataSource ID : $DATASOURCE_ID"
echo "API Key       : $API_KEY"

# 3. Validate the API key (this is what the intake service calls per document)
grpcurl -plaintext -d "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"api_key\": \"$API_KEY\"
}" $HOST $SVC/ValidateApiKey | jq .

# 4. Rotate the API key (update the crawler's config with the new key)
NEW_KEY=$(grpcurl -plaintext -d "{\"datasource_id\": \"$DATASOURCE_ID\"}" \
  $HOST $SVC/RotateApiKey | jq -r '.newApiKey')
echo "New API Key: $NEW_KEY"

# 5. Disable the datasource (e.g., for maintenance)
grpcurl -plaintext -d "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"active\": false,
  \"reason\": \"maintenance\"
}" $HOST $SVC/SetDataSourceStatus | jq .
```

---

## Detailed API Reference

### DataSourceAdminService (`localhost:18107`)

| Method | Required fields | Returns |
|--------|----------------|---------|
| `CreateDataSource` | `account_id`, `connector_id`, `name`, `drive_name` | `datasource` with one-time `api_key` |
| `GetDataSource` | `datasource_id` | `datasource` (no API key) |
| `UpdateDataSource` | `datasource_id` + fields to change | updated `datasource` |
| `ListDataSources` | optional `account_id`, `page_size`, `page_token` | paginated list |
| `DeleteDataSource` | `datasource_id`; optional `hard_delete` (bool) | soft-deletes by default; pass `hard_delete: true` to permanently remove |
| `ValidateApiKey` | `datasource_id`, `api_key` | `valid` bool + merged `DataSourceConfig` |
| `RotateApiKey` | `datasource_id` | one-time `new_api_key` |
| `SetDataSourceStatus` | `datasource_id`, `active` | `success` bool |
| `ListConnectorTypes` | _(none)_ | all registered connector types |
| `GetConnectorType` | `connector_id` | single connector type |

### ConnectorRegistrationService (`localhost:18107`)

| Method | Required fields | Returns |
|--------|----------------|---------|
| `CreateConnectorType` | `connector_type`, `name` | created connector with generated ID |
| `DeleteConnectorType` | `connector_id` | success (blocked if datasources exist) |
| `CreateConnectorConfigSchema` | `connector_id`, `schema_version`, `custom_config_schema`, `node_custom_config_schema` | created schema |
| `GetConnectorConfigSchema` | `schema_id` | schema details |
| `ListConnectorConfigSchemas` | `connector_id` | paginated list |
| `DeleteConnectorConfigSchema` | `schema_id` | success (blocked if referenced) |
| `SetConnectorCustomConfigSchema` | `connector_id` | links/unlinks schema to connector |
| `UpdateConnectorTypeDefaults` | `connector_id` | updated connector defaults |

### Health check

```bash
grpcurl -plaintext localhost:18107 grpc.health.v1.Health/Check
```

---

## Running Against a Document — the Intake Flow

The connector-admin service itself does **not** accept document uploads; it is an
_administrative_ service.  The actual document ingestion path goes through the
`connector-intake` service.  This section describes what happens end-to-end so you can
test the connector-admin portion in isolation.

### What the intake service does

When a crawler POSTs a document to the intake service, the intake service:

1. Calls `ValidateApiKey` on connector-admin with the datasource ID and the API key
   from the `Authorization: Bearer <api-key>` header.
2. Uses the returned `DataSourceConfig` (drive name, config, limits) to route the
   document to the correct storage drive and apply ingestion policies.
3. If `ValidateApiKey` returns `valid=false` the document is rejected (HTTP 401).

### Simulating an intake validation with grpcurl

```bash
# Replace with real values obtained from CreateDataSource
DATASOURCE_ID="<your-datasource-id>"
API_KEY="<your-api-key>"

grpcurl -plaintext \
  -d "{\"datasource_id\": \"$DATASOURCE_ID\", \"api_key\": \"$API_KEY\"}" \
  localhost:18107 \
  ai.pipestream.connector.intake.v1.DataSourceAdminService/ValidateApiKey
```

A successful response looks like:

```json
{
  "valid": true,
  "message": "API key is valid",
  "config": {
    "accountId": "my-account-001",
    "datasourceId": "<id>",
    "connectorId": "<connector-id>",
    "driveName": "primary-drive",
    "globalConfig": {
      "persistenceConfig": { "persistPipedoc": true, "maxInlineSizeBytes": 1048576 },
      "hydrationConfig": { "defaultHydrationPolicy": "HYDRATION_POLICY_AUTO" }
    }
  }
}
```

The `config` block is the merged Tier 1 configuration that the intake service uses to
determine ingestion policy.

### Using curl via gRPC-Web (if transcoding is enabled)

If the deployment enables gRPC-Web transcoding (Quarkus `quarkus-grpc-web` extension),
you can call the service with plain `curl` using JSON content:

```bash
curl -s -X POST http://localhost:18107/ai.pipestream.connector.intake.v1.DataSourceAdminService/ValidateApiKey \
  -H "Content-Type: application/grpc-web+json" \
  -H "Accept: application/grpc-web+json" \
  -d "{\"datasource_id\": \"$DATASOURCE_ID\", \"api_key\": \"$API_KEY\"}"
```

> **Note:** gRPC-Web transcoding requires an additional Quarkus extension.  The scripts
> in `scripts/` use `grpcurl` which works without extra configuration.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `pipeline_connector` | Database name |
| `DB_USER` | `pipeline` | Database username |
| `DB_PASSWORD` | `password` | Database password |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka bootstrap servers (prod) |
| `QUARKUS_PROFILE` | _(none)_ | Active profile (`dev`, `test`, `prod`) |

---

## Troubleshooting

### Service fails to start: "Connection refused" to PostgreSQL

Make sure Docker is running.  In dev mode, Quarkus DevServices starts a PostgreSQL
container automatically.  If you see errors like `FATAL: password authentication failed`,
check that no other PostgreSQL is bound on port 5432 and conflicting.

You can override the dev-mode database connection:

```bash
DB_HOST=mydb.example.com DB_PORT=5432 ./gradlew quarkusDev
```

### grpcurl: "Failed to dial target host": connection refused

The service is not running on port 18107.  Start it with `./gradlew quarkusDev` and wait
for the `Quarkus ... started in` log line.

### "Account does not exist" when creating a DataSource

In dev mode the service calls the live `account-manager` service via Consul service
discovery.  If `account-manager` is not running you will see this error.

**Workaround for testing:** set `connector.admin.account.validation.stub=true` in
`application.properties` (or via `-Dconnector.admin.account.validation.stub=true`).  In
stub mode any account ID not equal to `nonexistent` or `inactive-account` is accepted.

### API key validation returns `valid=false`

- Make sure you are using the key returned by `CreateDataSource` (or the latest
  `RotateApiKey` response) — the API key is a one-time plaintext value.
- Check that the datasource is active (`GetDataSource` → `active: true`).
- After a `RotateApiKey` call the old key is immediately invalidated.
