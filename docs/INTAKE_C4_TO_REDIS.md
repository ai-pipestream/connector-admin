# Intake Layer C4 Diagrams To Redis

This document describes the ingestion path from an external connector through the intake layer up to the Redis handoff. It focuses on the boundary that connector-admin owns or authorizes: datasource credentials, account status, and the `DataSourceConfig` returned to connector-intake before a document can move deeper into the pipeline.

The Redis box below is the downstream queue/cache boundary. This repository does not contain the connector-intake implementation, so the diagrams stop at the handoff rather than documenting Redis consumers that live in other services.

## C4 Level 1: System Context

```mermaid
C4Context
    title Connector Intake Context Up To Redis

    Person(operator, "Operator / Admin Tool", "Creates datasources, rotates API keys, and disables datasource access.")
    Person(connector, "External Connector", "Submits documents with datasource credentials.")

    System_Boundary(pipestream, "Pipestream Platform") {
        System(admin, "connector-admin", "Datasource lifecycle and credential authority.")
        System(intake, "connector-intake-service", "Receives connector document submissions and validates access.")
        System(accounts, "account-manager", "Account identity and lifecycle authority.")
        System(redis, "Redis", "Downstream intake handoff for accepted work.")
    }

    Rel(operator, admin, "CreateDataSource / RotateApiKey / SetDataSourceStatus", "gRPC")
    Rel(connector, intake, "Submit document + datasource_id + API key", "HTTP/gRPC")
    Rel(intake, admin, "ValidateApiKey", "gRPC")
    Rel(admin, accounts, "GetAccount / account-events", "gRPC / Kafka")
    Rel(intake, redis, "Accepted document work item", "Redis")
```

## C4 Level 2: Containers

```mermaid
C4Container
    title Intake Containers And Trust Boundaries

    Person(connector, "External Connector", "Connector process outside the platform trust boundary.")
    Person(operator, "Operator / Admin Tool", "Control-plane caller.")

    Container_Boundary(control, "Connector Admin Control Plane") {
        Container(adminGrpc, "DataSourceAdminService", "Quarkus gRPC", "Creates datasources, validates API keys, rotates keys, and controls datasource status.")
        Container(registrationGrpc, "ConnectorRegistrationService", "Quarkus gRPC", "Registers connector types and custom config schemas.")
        ContainerDb(adminDb, "connector-admin PostgreSQL", "PostgreSQL", "Stores datasource records, Argon2id API-key hashes, connector catalog, and schemas.")
    }

    Container_Boundary(intakeBoundary, "Connector Intake Layer") {
        Container(intakeApi, "Intake API", "HTTP/gRPC", "Accepts document submissions from connectors.")
        Container(authz, "Credential Validation Adapter", "gRPC client", "Calls connector-admin before the document is accepted.")
        Container(normalizer, "Document Intake Normalizer", "Application component", "Builds the internal work item from document metadata and DataSourceConfig.")
        Container(redisWriter, "Redis Handoff Writer", "Redis client", "Publishes accepted work to Redis.")
    }

    Container(accounts, "account-manager", "gRPC service", "Owns account active/inactive state.")
    Container(redis, "Redis", "Redis", "Boundary for accepted intake work.")

    Rel(operator, adminGrpc, "Datasource lifecycle", "gRPC")
    Rel(connector, intakeApi, "Document submission", "HTTP/gRPC")
    Rel(intakeApi, authz, "Validate credentials")
    Rel(authz, adminGrpc, "ValidateApiKey(datasource_id, api_key)", "gRPC")
    Rel(adminGrpc, accounts, "Validate account on datasource creation", "gRPC")
    Rel(adminGrpc, adminDb, "Read/write datasource and credential hash", "JDBC")
    Rel(authz, normalizer, "valid=true + DataSourceConfig")
    Rel(normalizer, redisWriter, "Accepted work item")
    Rel(redisWriter, redis, "Push/publish work", "Redis protocol")
```

## C4 Level 3: Intake Components

```mermaid
C4Component
    title Intake Component Flow Up To Redis

    Container_Boundary(intake, "connector-intake-service") {
        Component(requestHandler, "Request Handler", "Controller / gRPC service", "Receives document payload and extracts datasource credentials.")
        Component(credentialExtractor, "Credential Extractor", "Application component", "Reads datasource ID and bearer API key from the request.")
        Component(adminClient, "Connector Admin Client", "gRPC blocking or async client", "Calls ValidateApiKey on connector-admin.")
        Component(policyGate, "Policy Gate", "Application service", "Rejects invalid, inactive, disabled, or soft-deleted datasource submissions.")
        Component(configMapper, "DataSourceConfig Mapper", "Application component", "Maps returned DataSourceConfig into intake routing and persistence policy.")
        Component(workBuilder, "Work Item Builder", "Application component", "Builds a normalized document work item.")
        Component(redisPublisher, "Redis Publisher", "Redis client", "Writes accepted work to Redis.")
    }

    System_Ext(connector, "External Connector", "Submits documents.")
    System_Ext(admin, "connector-admin", "Credential and datasource authority.")
    System_Ext(redis, "Redis", "Downstream handoff.")

    Rel(connector, requestHandler, "Document + credentials")
    Rel(requestHandler, credentialExtractor, "Request metadata")
    Rel(credentialExtractor, adminClient, "datasource_id + api_key")
    Rel(adminClient, admin, "ValidateApiKey", "gRPC")
    Rel(admin, adminClient, "valid flag + DataSourceConfig")
    Rel(adminClient, policyGate, "Validation result")
    Rel(policyGate, configMapper, "Only when valid=true")
    Rel(configMapper, workBuilder, "Routing, limits, persistence, hydration policy")
    Rel(workBuilder, redisPublisher, "Work item")
    Rel(redisPublisher, redis, "Accepted intake handoff")
```

## C4 Level 4: Validation Sequence

```mermaid
sequenceDiagram
    autonumber
    participant Connector as External Connector
    participant Intake as Intake Request Handler
    participant Extractor as Credential Extractor
    participant AdminClient as Connector Admin Client
    participant Admin as connector-admin
    participant Policy as Policy Gate
    participant Builder as Work Item Builder
    participant Redis as Redis

    Connector->>Intake: Submit document + datasource_id + API key
    Intake->>Extractor: Extract credentials
    Extractor->>AdminClient: datasource_id + api_key
    AdminClient->>Admin: ValidateApiKey(datasource_id, api_key)

    alt valid datasource and current key
        Admin-->>AdminClient: valid=true + DataSourceConfig
        AdminClient->>Policy: validation accepted
        Policy->>Builder: apply config and build work item
        Builder->>Redis: write accepted work
        Redis-->>Builder: acknowledged
        Builder-->>Intake: accepted
        Intake-->>Connector: accepted response
    else invalid key, inactive datasource, disabled datasource, or soft-delete
        Admin-->>AdminClient: valid=false
        AdminClient->>Policy: validation rejected
        Policy-->>Intake: reject request
        Intake-->>Connector: unauthorized / rejected response
    end
```

## Error Handling Expectations

The normal path should be quiet: valid requests authenticate, produce a `DataSourceConfig`, and hand off to Redis. Exceptions should be rare and should represent infrastructure or data integrity failures, not expected business decisions.

Expected business outcomes should stay explicit:

- Invalid API key: return `valid=false`.
- Disabled or soft-deleted datasource: return `valid=false`.
- Missing or inactive account during datasource creation: return `INVALID_ARGUMENT`.
- Missing connector type during datasource creation: return `NOT_FOUND`.

Unexpected runtime failures should be allowed to cross the service boundary where they can be logged once and reported to the caller or messaging layer. connector-admin follows this pattern by catching named gRPC status exceptions first, then `RuntimeException` at the boundary. It should not catch generic `Exception` in transactional paths.

## Redis Handoff Contract

At the Redis boundary, connector-intake should already know:

- The datasource exists and is active.
- The supplied API key matched the current Argon2id hash.
- The account was valid when the datasource was created and account-events can later disable access.
- The work item has the routing data from `DataSourceConfig`, including `drive_name` and merged Tier 1 policy.

Redis should receive only accepted work. Rejected requests should not create Redis entries.
