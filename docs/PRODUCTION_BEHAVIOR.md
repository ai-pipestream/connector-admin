# Connector Admin Production Behavior

Connector Admin is the control-plane gateway for datasource onboarding and API-key validation. It owns the records that allow external connectors to reach the ingestion path, and it is the only service that can create, rotate, disable, or soft-delete datasource credentials.

## System Role

```mermaid
flowchart LR
    Operator[Admin operator or control-plane tool] -->|CreateDataSource / RotateApiKey| Admin[connector-admin]
    Connector[External connector] -->|Document + datasource_id + api_key| Intake[connector-intake-service]
    Intake -->|ValidateApiKey| Admin
    Admin -->|GetAccount| Accounts[account-manager]
    Admin -->|Persist datasource, hashes, schemas| Postgres[(PostgreSQL)]
    Admin -->|Merged DataSourceConfig| Intake

    classDef service fill:#e8f1ff,stroke:#2f5fb3,stroke-width:1px,color:#0f244d
    classDef data fill:#eef8ee,stroke:#3f8f46,stroke-width:1px,color:#123d16
    classDef actor fill:#fff7e6,stroke:#b7791f,stroke-width:1px,color:#4a2a00

    class Admin,Intake,Accounts service
    class Postgres data
    class Operator,Connector actor
```

The service has two public gRPC surfaces:

- `DataSourceAdminService` manages datasource lifecycle, API-key validation, API-key rotation, datasource status, connector discovery, and soft-delete.
- `ConnectorRegistrationService` manages connector type registration, connector default metadata, and JSON Schema versions for connector-specific custom configuration.

Generated Mutiny stubs may exist because the protobuf toolchain can produce them, but hand-written production code uses standard grpc-java service implementations and Hibernate ORM/Panache.

## Datasource Contract

```mermaid
sequenceDiagram
    autonumber
    participant Client as Admin client
    participant Admin as connector-admin
    participant Accounts as account-manager
    participant DB as PostgreSQL
    participant Intake as connector-intake-service

    Client->>Admin: CreateDataSource(account_id, connector_id)
    Admin->>Accounts: GetAccount(account_id)
    Accounts-->>Admin: active account
    Admin->>DB: Store datasource and Argon2id API-key hash
    Admin-->>Client: datasource + one-time plaintext API key

    Intake->>Admin: ValidateApiKey(datasource_id, api_key)
    Admin->>DB: Load datasource, connector defaults, datasource overrides
    Admin-->>Intake: valid=true + merged DataSourceConfig

    Client->>Admin: RotateApiKey(datasource_id)
    Admin->>DB: Replace stored API-key hash
    Admin-->>Client: one-time plaintext replacement key
```

Production guarantees:

- Datasource creation requires an account that exists and is active in account-manager.
- Plaintext API keys are returned only by `CreateDataSource` and `RotateApiKey`.
- `GetDataSource` and `ListDataSources` never expose plaintext API keys.
- `ValidateApiKey` accepts only the current key for an active datasource.
- `RotateApiKey` invalidates the previous key immediately.
- `DeleteDataSource` is a soft-delete: the datasource remains auditable, but validation is blocked.

## Status Reconciliation

```mermaid
stateDiagram-v2
    [*] --> Active: CreateDataSource
    Active --> Disabled: SetDataSourceStatus(active=false)
    Disabled --> Active: SetDataSourceStatus(active=true)
    Active --> AccountDisabled: account-manager Inactivated event
    AccountDisabled --> Active: account-manager Reactivated event
    Active --> SoftDeleted: DeleteDataSource
    Disabled --> SoftDeleted: DeleteDataSource
    AccountDisabled --> SoftDeleted: DeleteDataSource
    SoftDeleted --> [*]

    note right of AccountDisabled
      status_reason=account_inactive
      lets reactivation distinguish account-driven holds
      from manual or operational disables.
    end note

    note right of SoftDeleted
      Historical records remain.
      API-key validation returns invalid.
    end note
```

The account-events consumer runs on a worker thread and inside a transaction. That is important because Kafka consumers do not necessarily run inside a request context; Hibernate ORM access requires a transaction or active request context.

## Configuration Returned To Intake

```mermaid
flowchart TD
    SystemDefaults[System defaults] --> Merge[ConfigMergingService]
    ConnectorDefaults[Connector type defaults] --> Merge
    DataSourceColumns[Datasource column overrides] --> Merge
    DataSourceProto[Datasource serialized proto overrides] --> Merge
    Merge --> Config[DataSourceConfig returned by ValidateApiKey]
    Config --> Intake[connector-intake routing and persistence decisions]

    classDef source fill:#eef8ee,stroke:#3f8f46,color:#123d16
    classDef service fill:#e8f1ff,stroke:#2f5fb3,color:#0f244d
    classDef output fill:#fff7e6,stroke:#b7791f,color:#4a2a00

    class SystemDefaults,ConnectorDefaults,DataSourceColumns,DataSourceProto source
    class Merge,Intake service
    class Config output
```

`ValidateApiKey` is therefore both an authentication check and a configuration handoff. A valid response tells connector-intake that the caller can submit documents for that datasource and provides the configuration needed to process those documents consistently.

## Automated Validation

```mermaid
flowchart LR
    Compile[compileJava + compileIntegrationTestJava] --> Unit[Quarkus unit tests]
    Unit --> Packaged[quarkusIntTest packaged JVM]
    Packaged --> WireMock[pipestream-wiremock-server account-manager mock]
    Packaged --> Postgres[(PostgreSQL DevServices)]
    Packaged --> Contract[Gateway behavior assertions]

    Contract --> Credential[Credential boundary]
    Contract --> Accounts[Account validation]
    Contract --> Integrity[Connector referential integrity]
    Contract --> Events[Account event reconciliation]
```

The packaged tests intentionally avoid repository mocks. They call the service over gRPC, use PostgreSQL for persistence, and use pipestream-wiremock-server only at the account-manager boundary.
