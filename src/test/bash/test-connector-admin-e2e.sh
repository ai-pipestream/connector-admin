#!/bin/bash

# End-to-end test script for DataSource Admin Service
# Tests the full CRUD flow via grpcurl

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
GRPC_HOST="localhost:38107"
SERVICE="ai.pipestream.connector.intake.v1.DataSourceAdminService"
TEST_PREFIX="e2e-test-$(date +%s)"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}DataSource Admin Service E2E Test${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Step 1: Create account for testing (requires account-manager running on 38105)
echo -e "${YELLOW}Step 1: Creating test account...${NC}"
ACCOUNT_ID="${TEST_PREFIX}-account"
grpcurl -plaintext -d "{\"account_id\":\"${ACCOUNT_ID}\",\"name\":\"Test Account\",\"description\":\"E2E test account\"}" \
  localhost:38105 io.pipeline.repository.account.AccountService/CreateAccount | jq .
echo -e "${GREEN}✓ Account created${NC}"
echo ""

# Step 2: List available connector types
echo -e "${YELLOW}Step 2: Listing available connector types...${NC}"
CONNECTOR_TYPES=$(grpcurl -plaintext -d '{}' $GRPC_HOST $SERVICE/ListConnectorTypes)
echo "$CONNECTOR_TYPES" | jq .
# Extract first connector ID (assuming at least one exists)
CONNECTOR_TYPE_ID=$(echo "$CONNECTOR_TYPES" | jq -r '.connectors[0].connectorId // empty')
if [ -z "$CONNECTOR_TYPE_ID" ]; then
  echo -e "${RED}✗ No connector types available. Please seed connector types first.${NC}"
  exit 1
fi
echo -e "${GREEN}✓ Using connector type: ${CONNECTOR_TYPE_ID}${NC}"
echo ""

# Step 3: Create first datasource
echo -e "${YELLOW}Step 3: Creating datasource 'filesystem-datasource'...${NC}"
DATASOURCE1_NAME="${TEST_PREFIX}-filesystem"
RESPONSE1=$(grpcurl -plaintext -d "{\"account_id\":\"${ACCOUNT_ID}\",\"connector_id\":\"${CONNECTOR_TYPE_ID}\",\"name\":\"${DATASOURCE1_NAME}\",\"drive_name\":\"test-drive\"}" \
  $GRPC_HOST $SERVICE/CreateDataSource)
echo "$RESPONSE1" | jq .
DATASOURCE1_ID=$(echo "$RESPONSE1" | jq -r '.datasource.datasourceId')
API_KEY1=$(echo "$RESPONSE1" | jq -r '.datasource.apiKey')
echo -e "${GREEN}✓ DataSource created with ID: ${DATASOURCE1_ID}${NC}"
echo -e "${GREEN}✓ API Key: ${API_KEY1:0:20}...${NC}"
echo ""

# Step 4: Create second datasource
echo -e "${YELLOW}Step 4: Creating datasource 'database-datasource'...${NC}"
DATASOURCE2_NAME="${TEST_PREFIX}-database"
RESPONSE2=$(grpcurl -plaintext -d "{\"account_id\":\"${ACCOUNT_ID}\",\"connector_id\":\"${CONNECTOR_TYPE_ID}\",\"name\":\"${DATASOURCE2_NAME}\",\"drive_name\":\"test-drive\"}" \
  $GRPC_HOST $SERVICE/CreateDataSource)
echo "$RESPONSE2" | jq .
DATASOURCE2_ID=$(echo "$RESPONSE2" | jq -r '.datasource.datasourceId')
echo -e "${GREEN}✓ DataSource created with ID: ${DATASOURCE2_ID}${NC}"
echo ""

# Step 5: Get datasource details
echo -e "${YELLOW}Step 5: Getting datasource details...${NC}"
grpcurl -plaintext -d "{\"datasource_id\":\"${DATASOURCE1_ID}\"}" \
  $GRPC_HOST $SERVICE/GetDataSource | jq .
echo -e "${GREEN}✓ DataSource retrieved${NC}"
echo ""

# Step 6: List all datasources
echo -e "${YELLOW}Step 6: Listing all datasources...${NC}"
ALL_DATASOURCES=$(grpcurl -plaintext -d '{}' \
  $GRPC_HOST $SERVICE/ListDataSources)
echo "$ALL_DATASOURCES" | jq .
TOTAL=$(echo "$ALL_DATASOURCES" | jq -r '.totalCount')
echo -e "${GREEN}✓ Found ${TOTAL} total datasources${NC}"
echo ""

# Step 7: List datasources for specific account
echo -e "${YELLOW}Step 7: Listing datasources for account ${ACCOUNT_ID}...${NC}"
ACCOUNT_DATASOURCES=$(grpcurl -plaintext -d "{\"account_id\":\"${ACCOUNT_ID}\"}" \
  $GRPC_HOST $SERVICE/ListDataSources)
echo "$ACCOUNT_DATASOURCES" | jq .
ACCOUNT_COUNT=$(echo "$ACCOUNT_DATASOURCES" | jq -r '.totalCount')
echo -e "${GREEN}✓ Found ${ACCOUNT_COUNT} datasources for this account${NC}"
echo ""

# Step 8: Update datasource name
echo -e "${YELLOW}Step 8: Updating datasource name...${NC}"
NEW_NAME="${TEST_PREFIX}-filesystem-updated"
grpcurl -plaintext -d "{\"datasource_id\":\"${DATASOURCE1_ID}\",\"name\":\"${NEW_NAME}\"}" \
  $GRPC_HOST $SERVICE/UpdateDataSource | jq .
echo -e "${GREEN}✓ DataSource updated${NC}"
echo ""

# Step 9: Verify update
echo -e "${YELLOW}Step 9: Verifying datasource was updated...${NC}"
UPDATED=$(grpcurl -plaintext -d "{\"datasource_id\":\"${DATASOURCE1_ID}\"}" \
  $GRPC_HOST $SERVICE/GetDataSource)
echo "$UPDATED" | jq .
UPDATED_NAME=$(echo "$UPDATED" | jq -r '.datasource.name')
if [ "$UPDATED_NAME" = "$NEW_NAME" ]; then
  echo -e "${GREEN}✓ Name successfully updated to: ${UPDATED_NAME}${NC}"
else
  echo -e "${RED}✗ Name update failed. Expected: ${NEW_NAME}, Got: ${UPDATED_NAME}${NC}"
  exit 1
fi
echo ""

# Step 10: Rotate API key
echo -e "${YELLOW}Step 10: Rotating API key for datasource...${NC}"
ROTATE_RESPONSE=$(grpcurl -plaintext -d "{\"datasource_id\":\"${DATASOURCE2_ID}\",\"invalidate_old_immediately\":true}" \
  $GRPC_HOST $SERVICE/RotateApiKey)
echo "$ROTATE_RESPONSE" | jq .
NEW_API_KEY=$(echo "$ROTATE_RESPONSE" | jq -r '.newApiKey')
echo -e "${GREEN}✓ API key rotated. New key: ${NEW_API_KEY:0:20}...${NC}"
echo ""

# Step 11: Set datasource inactive
echo -e "${YELLOW}Step 11: Inactivating datasource...${NC}"
grpcurl -plaintext -d "{\"datasource_id\":\"${DATASOURCE2_ID}\",\"active\":false,\"reason\":\"E2E test cleanup\"}" \
  $GRPC_HOST $SERVICE/SetDataSourceStatus | jq .
echo -e "${GREEN}✓ DataSource set to inactive${NC}"
echo ""

# Step 12: List active datasources only (should not include inactivated one)
echo -e "${YELLOW}Step 12: Listing active datasources only...${NC}"
ACTIVE_DATASOURCES=$(grpcurl -plaintext -d "{\"account_id\":\"${ACCOUNT_ID}\",\"include_inactive\":false}" \
  $GRPC_HOST $SERVICE/ListDataSources)
echo "$ACTIVE_DATASOURCES" | jq .
ACTIVE_COUNT=$(echo "$ACTIVE_DATASOURCES" | jq -r '.datasources | length')
echo -e "${GREEN}✓ ${ACTIVE_COUNT} active datasources (should not include inactivated one)${NC}"
echo ""

# Step 13: Delete datasource
echo -e "${YELLOW}Step 13: Deleting datasource (soft delete)...${NC}"
grpcurl -plaintext -d "{\"datasource_id\":\"${DATASOURCE1_ID}\",\"hard_delete\":false}" \
  $GRPC_HOST $SERVICE/DeleteDataSource | jq .
echo -e "${GREEN}✓ DataSource deleted${NC}"
echo ""

# Step 14: Verify deletion (should exist but be inactive)
echo -e "${YELLOW}Step 14: Verifying soft delete...${NC}"
DELETED=$(grpcurl -plaintext -d "{\"datasource_id\":\"${DATASOURCE1_ID}\"}" \
  $GRPC_HOST $SERVICE/GetDataSource)
echo "$DELETED" | jq .
IS_ACTIVE=$(echo "$DELETED" | jq -r '.datasource.active // false')
# Note: protobuf omits fields with default values, so missing active = false
if [ "$IS_ACTIVE" = "false" ] || [ "$IS_ACTIVE" = "null" ]; then
  echo -e "${GREEN}✓ DataSource soft deleted (active=false or omitted)${NC}"
else
  echo -e "${RED}✗ Soft delete failed. DataSource is still active: ${IS_ACTIVE}${NC}"
  exit 1
fi
echo ""

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}All E2E tests passed! ✓${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Test artifacts created:"
echo "  Account ID: ${ACCOUNT_ID}"
echo "  DataSource 1: ${DATASOURCE1_ID} (deleted)"
echo "  DataSource 2: ${DATASOURCE2_ID} (inactive)"
