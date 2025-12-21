#!/bin/bash
# grpcurl test scripts for DataSourceAdminService
# Run these against a running connector-admin-service (default: localhost:38107)

HOST="${GRPC_HOST:-localhost:38107}"
SERVICE="ai.pipestream.connector.intake.v1.DataSourceAdminService"

# Pre-seeded connector IDs
S3_CONNECTOR_ID="a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
FILE_CRAWLER_CONNECTOR_ID="b1ffc0aa-0d1c-5f09-cc7e-7cc0ce491b22"

echo "=========================================="
echo "DataSourceAdminService gRPC Tests"
echo "Host: $HOST"
echo "=========================================="

# =========================================
# Connector Type Operations
# =========================================

echo -e "\n--- ListConnectorTypes ---"
grpcurl -plaintext $HOST $SERVICE/ListConnectorTypes

echo -e "\n--- GetConnectorType (S3) ---"
grpcurl -plaintext -d "{\"connector_id\": \"$S3_CONNECTOR_ID\"}" $HOST $SERVICE/GetConnectorType

# =========================================
# DataSource Lifecycle
# =========================================

ACCOUNT_ID="test-account-$(date +%s)"
echo -e "\n--- CreateDataSource ---"
echo "Account ID: $ACCOUNT_ID"
CREATE_RESPONSE=$(grpcurl -plaintext -d "{
  \"account_id\": \"$ACCOUNT_ID\",
  \"connector_id\": \"$S3_CONNECTOR_ID\",
  \"name\": \"My S3 DataSource\",
  \"drive_name\": \"test-drive\",
  \"metadata\": {\"env\": \"dev\", \"team\": \"engineering\"}
}" $HOST $SERVICE/CreateDataSource)

echo "$CREATE_RESPONSE"

# Extract datasource_id and api_key from response
DATASOURCE_ID=$(echo "$CREATE_RESPONSE" | jq -r '.datasource.datasourceId')
API_KEY=$(echo "$CREATE_RESPONSE" | jq -r '.datasource.apiKey')

echo -e "\nExtracted: datasource_id=$DATASOURCE_ID, api_key=$API_KEY"

echo -e "\n--- GetDataSource ---"
grpcurl -plaintext -d "{\"datasource_id\": \"$DATASOURCE_ID\"}" $HOST $SERVICE/GetDataSource

echo -e "\n--- ValidateApiKey (valid) ---"
grpcurl -plaintext -d "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"api_key\": \"$API_KEY\"
}" $HOST $SERVICE/ValidateApiKey

echo -e "\n--- ValidateApiKey (invalid) ---"
grpcurl -plaintext -d "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"api_key\": \"wrong-api-key\"
}" $HOST $SERVICE/ValidateApiKey

echo -e "\n--- ListDataSources ---"
grpcurl -plaintext -d "{\"account_id\": \"$ACCOUNT_ID\"}" $HOST $SERVICE/ListDataSources

echo -e "\n--- UpdateDataSource ---"
grpcurl -plaintext -d "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"name\": \"Updated S3 DataSource\",
  \"metadata\": {\"env\": \"staging\", \"team\": \"engineering\", \"updated\": \"true\"}
}" $HOST $SERVICE/UpdateDataSource

echo -e "\n--- RotateApiKey ---"
ROTATE_RESPONSE=$(grpcurl -plaintext -d "{\"datasource_id\": \"$DATASOURCE_ID\"}" $HOST $SERVICE/RotateApiKey)
echo "$ROTATE_RESPONSE"
NEW_API_KEY=$(echo "$ROTATE_RESPONSE" | jq -r '.newApiKey')
echo "New API Key: $NEW_API_KEY"

echo -e "\n--- ValidateApiKey (old key - should fail) ---"
grpcurl -plaintext -d "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"api_key\": \"$API_KEY\"
}" $HOST $SERVICE/ValidateApiKey

echo -e "\n--- ValidateApiKey (new key - should succeed) ---"
grpcurl -plaintext -d "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"api_key\": \"$NEW_API_KEY\"
}" $HOST $SERVICE/ValidateApiKey

echo -e "\n--- SetDataSourceStatus (disable) ---"
grpcurl -plaintext -d "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"active\": false,
  \"reason\": \"manual_test\"
}" $HOST $SERVICE/SetDataSourceStatus

echo -e "\n--- ValidateApiKey (disabled - should fail) ---"
grpcurl -plaintext -d "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"api_key\": \"$NEW_API_KEY\"
}" $HOST $SERVICE/ValidateApiKey

echo -e "\n--- SetDataSourceStatus (re-enable) ---"
grpcurl -plaintext -d "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"active\": true
}" $HOST $SERVICE/SetDataSourceStatus

echo -e "\n--- DeleteDataSource (soft delete) ---"
grpcurl -plaintext -d "{\"datasource_id\": \"$DATASOURCE_ID\"}" $HOST $SERVICE/DeleteDataSource

echo -e "\n--- GetDataSource (after delete - should show inactive) ---"
grpcurl -plaintext -d "{\"datasource_id\": \"$DATASOURCE_ID\"}" $HOST $SERVICE/GetDataSource

echo -e "\n=========================================="
echo "All tests completed!"
echo "=========================================="
