# connector-admin

Administrative service for managing external connectors and their configurations in the Pipestream platform.

## Overview

The connector-admin service provides centralized administration for external data source connectors. It manages the lifecycle of:

- **Connector Types**: Pre-seeded connector definitions (e.g., "s3", "file-crawler", "confluence")
- **Connector Config Schemas**: JSON Schema definitions for custom connector configuration
- **DataSources**: Account-specific bindings to connector types with API keys and configuration
- **Two-Tier Configuration**: Global defaults + per-node overrides for flexible deployment

## Architecture

### Two-Tier Configuration Model

The service implements a hierarchical configuration system:

#### Tier 1: Global/Default Configuration (Stored in connector-admin)
- Connector-level defaults (persistence policies, retention settings, etc.)
- Strongly typed protobuf fields for universal settings
- JSON Schema-validated custom configuration per connector type
- Applied to all DataSources using a connector

#### Tier 2: Per-Node/Instance Configuration (Stored in pipestream-engine)
- Graph-specific overrides for individual datasource instances
- Node-level routing hints and processing directives
- Managed through the engine's graph configuration

### Services

#### DataSourceAdminService
Administrative operations for datasource lifecycle management:

- **DataSource CRUD**: Create, read, update, delete datasources
- **API Key Management**: Generate, validate, rotate API keys
- **Status Control**: Enable/disable datasources with reason tracking
- **Connector Discovery**: List available connector types

#### ConnectorRegistrationService
Registration and management of connector types and schemas:

- **Schema Management**: Create, read, update, delete configuration schemas
- **Connector Registration**: Manage connector type metadata and defaults
- **Versioning**: Support schema versioning for backward compatibility
- **Validation**: JSON Schema validation for custom configuration fields

## API Reference

### DataSourceAdminService

| RPC Method | Description |
|------------|-------------|
| `CreateDataSource` | Create new datasource with generated API key |
| `UpdateDataSource` | Update datasource configuration |
| `GetDataSource` | Retrieve datasource details |
| `ValidateApiKey` | Validate API key and return merged configuration |
| `ListDataSources` | List datasources with optional filtering |
| `SetDataSourceStatus` | Enable/disable datasource |
| `DeleteDataSource` | Soft delete datasource |
| `RotateApiKey` | Generate new API key |
| `GetCrawlHistory` | Get datasource crawl history (unimplemented) |
| `ListConnectorTypes` | List available connector types |
| `GetConnectorType` | Get connector type details |

### ConnectorRegistrationService

| RPC Method | Description |
|------------|-------------|
| `CreateConnectorConfigSchema` | Create new configuration schema |
| `GetConnectorConfigSchema` | Retrieve schema by ID |
| `ListConnectorConfigSchemas` | List schemas with pagination |
| `DeleteConnectorConfigSchema` | Remove schema (with dependency checks) |
| `SetConnectorCustomConfigSchema` | Link/unlink schema to connector |
| `UpdateConnectorTypeDefaults` | Update connector-level defaults |

## Database Schema

### Tables

#### connectors
Pre-seeded connector type definitions.

| Column | Type | Description |
|--------|------|-------------|
| connector_id | VARCHAR(255) | Primary key (deterministic hash) |
| connector_type | VARCHAR(255) | Type name (unique) |
| name | VARCHAR(255) | Human-readable name |
| description | TEXT | Connector description |
| management_type | VARCHAR(50) | "MANAGED" or "UNMANAGED" |
| custom_config_schema_id | VARCHAR(255) | FK to connector_config_schemas |
| default_persist_pipedoc | BOOLEAN | Default persistence policy |
| default_max_inline_size_bytes | INTEGER | Default max inline size |
| default_custom_config | JSONB | Default custom configuration |
| display_name | VARCHAR(255) | UI display name |
| owner | VARCHAR(255) | Connector owner |
| documentation_url | TEXT | Documentation URL |
| tags | TEXT[] | Categorization tags |

#### connector_config_schemas
JSON Schema definitions for custom connector configuration.

| Column | Type | Description |
|--------|------|-------------|
| schema_id | VARCHAR(255) | Primary key |
| connector_id | VARCHAR(255) | FK to connectors |
| schema_version | VARCHAR(100) | Version identifier |
| custom_config_schema | JSONB | Tier 1 custom config schema |
| node_custom_config_schema | JSONB | Tier 2 node custom config schema |
| created_at | TIMESTAMP | Creation timestamp |
| created_by | VARCHAR(255) | Creator |
| apicurio_artifact_id | VARCHAR(255) | Apicurio Registry ID |
| apicurio_global_id | BIGINT | Apicurio global ID |
| sync_status | VARCHAR(20) | Sync status with Apicurio |
| last_sync_attempt | TIMESTAMP | Last sync attempt |
| sync_error | VARCHAR(255) | Last sync error |

#### datasources
Account-specific datasource bindings.

| Column | Type | Description |
|--------|------|-------------|
| datasource_id | VARCHAR(255) | Primary key (deterministic hash) |
| account_id | VARCHAR(255) | Owning account |
| connector_id | VARCHAR(255) | FK to connectors |
| name | VARCHAR(255) | Human-readable name |
| api_key_hash | VARCHAR(255) | Argon2id hash of API key |
| drive_name | VARCHAR(255) | Storage drive reference |
| metadata | JSONB | Additional metadata |
| max_file_size | BIGINT | File size limit |
| rate_limit_per_minute | BIGINT | Rate limit |
| active | BOOLEAN | Active status |
| status_reason | VARCHAR(255) | Status change reason |
| global_config_proto | BYTEA | Serialized protobuf config |
| custom_config | JSONB | Custom configuration |
| custom_config_schema_id | VARCHAR(255) | FK to connector_config_schemas |
| created_at | TIMESTAMP | Creation timestamp |
| updated_at | TIMESTAMP | Last update timestamp |
| last_rotated_at | TIMESTAMP | Last API key rotation |

## Configuration Merging

The service includes a `ConfigMergingService` that resolves Tier 1 configuration from multiple sources:

1. **System Defaults**: Hardcoded fallback values
2. **Connector Defaults**: Service-level defaults for all datasources
3. **DataSource Overrides**: Instance-specific configuration
4. **Proto Overrides**: Serialized protobuf configuration (highest priority)

Configuration resolution order: System → Connector → DataSource Column → DataSource Proto

## Development

### Prerequisites
- Java 21+
- PostgreSQL
- Quarkus 3.x
- Gradle 8+

### Building
```bash
./gradlew build
```

### Testing
```bash
./gradlew test
```

### Running Locally
```bash
./gradlew quarkus:dev
```

## Dependencies

### Core Dependencies
- **Quarkus**: Framework for microservices
- **Hibernate ORM**: Database persistence
- **PostgreSQL**: Database
- **gRPC**: Service communication
- **Protobuf**: Message serialization

### Key Libraries
- **password4j**: API key hashing
- **pipestream-wiremock-server**: Testing infrastructure
- **pipestream-service-registration**: Service discovery
- **quarkus-dynamic-grpc**: Dynamic gRPC client creation

## Integration Points

- **pipestream-engine**: Receives merged Tier 1 config via `ValidateApiKey`
- **connector-intake-service**: Validates API keys and retrieves configuration
- **repository-service**: Manages storage drives referenced by datasources
- **account-service**: Validates account existence and status
- **pipestream-wiremock-server**: Provides mock services for testing

## Error Handling

The service implements comprehensive error handling:

- **Validation**: Input validation with clear error messages
- **Constraints**: Database constraint violation handling
- **Dependencies**: Foreign key relationship management
- **Authentication**: API key validation with proper error responses

## Security

- **API Key Management**: Secure key generation and rotation
- **Hashing**: Argon2id password hashing for API keys
- **Validation**: Strict input validation and sanitization
- **Access Control**: Account-based datasource isolation