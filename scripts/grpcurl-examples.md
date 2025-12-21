# grpcurl Examples for DataSourceAdminService

All commands assume connector-admin-service is running on `localhost:38107`.

## Pre-seeded Connector IDs
- **S3**: `a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11`
- **File Crawler**: `b1ffc0aa-0d1c-5f09-cc7e-7cc0ce491b22`

## List Services
```bash
grpcurl -plaintext localhost:38107 list
```

## Describe Service
```bash
grpcurl -plaintext localhost:38107 describe ai.pipestream.connector.intake.v1.DataSourceAdminService
```

---

## Connector Type Operations

### List All Connector Types
```bash
grpcurl -plaintext localhost:38107 \
  ai.pipestream.connector.intake.v1.DataSourceAdminService/ListConnectorTypes
```

### Get Connector Type by ID
```bash
grpcurl -plaintext -d '{"connector_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}' \
  localhost:38107 \
  ai.pipestream.connector.intake.v1.DataSourceAdminService/GetConnectorType
```

---

## DataSource CRUD Operations

### Create DataSource
```bash
grpcurl -plaintext -d '{
  "account_id": "my-account-123",
  "connector_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
  "name": "My S3 DataSource",
  "drive_name": "my-drive",
  "metadata": {"env": "dev", "team": "engineering"}
}' localhost:38107 \
  ai.pipestream.connector.intake.v1.DataSourceAdminService/CreateDataSource
```

### Get DataSource
```bash
grpcurl -plaintext -d '{"datasource_id": "YOUR_DATASOURCE_ID"}' \
  localhost:38107 \
  ai.pipestream.connector.intake.v1.DataSourceAdminService/GetDataSource
```

### List DataSources for Account
```bash
grpcurl -plaintext -d '{"account_id": "my-account-123"}' \
  localhost:38107 \
  ai.pipestream.connector.intake.v1.DataSourceAdminService/ListDataSources
```

### Update DataSource
```bash
grpcurl -plaintext -d '{
  "datasource_id": "YOUR_DATASOURCE_ID",
  "name": "Updated Name",
  "drive_name": "new-drive",
  "metadata": {"env": "staging"}
}' localhost:38107 \
  ai.pipestream.connector.intake.v1.DataSourceAdminService/UpdateDataSource
```

### Delete DataSource (soft delete)
```bash
grpcurl -plaintext -d '{"datasource_id": "YOUR_DATASOURCE_ID"}' \
  localhost:38107 \
  ai.pipestream.connector.intake.v1.DataSourceAdminService/DeleteDataSource
```

---

## API Key Operations

### Validate API Key
```bash
grpcurl -plaintext -d '{
  "datasource_id": "YOUR_DATASOURCE_ID",
  "api_key": "YOUR_API_KEY"
}' localhost:38107 \
  ai.pipestream.connector.intake.v1.DataSourceAdminService/ValidateApiKey
```

### Rotate API Key
```bash
grpcurl -plaintext -d '{"datasource_id": "YOUR_DATASOURCE_ID"}' \
  localhost:38107 \
  ai.pipestream.connector.intake.v1.DataSourceAdminService/RotateApiKey
```

---

## Status Operations

### Disable DataSource
```bash
grpcurl -plaintext -d '{
  "datasource_id": "YOUR_DATASOURCE_ID",
  "active": false,
  "reason": "maintenance"
}' localhost:38107 \
  ai.pipestream.connector.intake.v1.DataSourceAdminService/SetDataSourceStatus
```

### Enable DataSource
```bash
grpcurl -plaintext -d '{
  "datasource_id": "YOUR_DATASOURCE_ID",
  "active": true
}' localhost:38107 \
  ai.pipestream.connector.intake.v1.DataSourceAdminService/SetDataSourceStatus
```

---

## Health Check
```bash
grpcurl -plaintext localhost:38107 grpc.health.v1.Health/Check
```
