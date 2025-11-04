#!/bin/bash

# End-to-end test script for Connector Admin Service
# Tests the full CRUD flow via grpcurl

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
GRPC_HOST="localhost:38107"
TEST_PREFIX="e2e-test-$(date +%s)"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Connector Admin Service E2E Test${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Step 1: Create account for testing (requires account-manager running on 38105)
echo -e "${YELLOW}Step 1: Creating test account...${NC}"
ACCOUNT_ID="${TEST_PREFIX}-account"
grpcurl -plaintext -d "{\"account_id\":\"${ACCOUNT_ID}\",\"name\":\"Test Account\",\"description\":\"E2E test account\"}" \
  localhost:38105 io.pipeline.repository.account.AccountService/CreateAccount | jq .
echo -e "${GREEN}✓ Account created${NC}"
echo ""

# Step 2: Register first connector
echo -e "${YELLOW}Step 2: Registering connector 'filesystem-connector'...${NC}"
CONNECTOR1_NAME="${TEST_PREFIX}-filesystem"
RESPONSE1=$(grpcurl -plaintext -d "{\"connector_name\":\"${CONNECTOR1_NAME}\",\"connector_type\":\"filesystem\",\"account_id\":\"${ACCOUNT_ID}\"}" \
  $GRPC_HOST io.pipeline.connector.intake.ConnectorAdminService/RegisterConnector)
echo "$RESPONSE1" | jq .
CONNECTOR1_ID=$(echo "$RESPONSE1" | jq -r '.connector_id')
API_KEY1=$(echo "$RESPONSE1" | jq -r '.api_key')
echo -e "${GREEN}✓ Connector registered with ID: ${CONNECTOR1_ID}${NC}"
echo -e "${GREEN}✓ API Key: ${API_KEY1:0:20}...${NC}"
echo ""

# Step 3: Register second connector
echo -e "${YELLOW}Step 3: Registering connector 'database-connector'...${NC}"
CONNECTOR2_NAME="${TEST_PREFIX}-database"
RESPONSE2=$(grpcurl -plaintext -d "{\"connector_name\":\"${CONNECTOR2_NAME}\",\"connector_type\":\"database\",\"account_id\":\"${ACCOUNT_ID}\"}" \
  $GRPC_HOST io.pipeline.connector.intake.ConnectorAdminService/RegisterConnector)
echo "$RESPONSE2" | jq .
CONNECTOR2_ID=$(echo "$RESPONSE2" | jq -r '.connector_id')
echo -e "${GREEN}✓ Connector registered with ID: ${CONNECTOR2_ID}${NC}"
echo ""

# Step 4: Get connector details
echo -e "${YELLOW}Step 4: Getting connector details...${NC}"
grpcurl -plaintext -d "{\"connector_id\":\"${CONNECTOR1_ID}\"}" \
  $GRPC_HOST io.pipeline.connector.intake.ConnectorAdminService/GetConnector | jq .
echo -e "${GREEN}✓ Connector retrieved${NC}"
echo ""

# Step 5: List all connectors
echo -e "${YELLOW}Step 5: Listing all connectors...${NC}"
ALL_CONNECTORS=$(grpcurl -plaintext -d '{}' \
  $GRPC_HOST io.pipeline.connector.intake.ConnectorAdminService/ListConnectors)
echo "$ALL_CONNECTORS" | jq .
TOTAL=$(echo "$ALL_CONNECTORS" | jq -r '.total_count')
echo -e "${GREEN}✓ Found ${TOTAL} total connectors${NC}"
echo ""

# Step 6: List connectors for specific account
echo -e "${YELLOW}Step 6: Listing connectors for account ${ACCOUNT_ID}...${NC}"
ACCOUNT_CONNECTORS=$(grpcurl -plaintext -d "{\"account_id\":\"${ACCOUNT_ID}\"}" \
  $GRPC_HOST io.pipeline.connector.intake.ConnectorAdminService/ListConnectors)
echo "$ACCOUNT_CONNECTORS" | jq .
ACCOUNT_COUNT=$(echo "$ACCOUNT_CONNECTORS" | jq -r '.total_count')
echo -e "${GREEN}✓ Found ${ACCOUNT_COUNT} connectors for this account${NC}"
echo ""

# Step 7: Update connector name
echo -e "${YELLOW}Step 7: Updating connector name...${NC}"
NEW_NAME="${TEST_PREFIX}-filesystem-updated"
grpcurl -plaintext -d "{\"connector_id\":\"${CONNECTOR1_ID}\",\"connector_name\":\"${NEW_NAME}\"}" \
  $GRPC_HOST io.pipeline.connector.intake.ConnectorAdminService/UpdateConnector | jq .
echo -e "${GREEN}✓ Connector updated${NC}"
echo ""

# Step 8: Verify update
echo -e "${YELLOW}Step 8: Verifying connector was updated...${NC}"
UPDATED=$(grpcurl -plaintext -d "{\"connector_id\":\"${CONNECTOR1_ID}\"}" \
  $GRPC_HOST io.pipeline.connector.intake.ConnectorAdminService/GetConnector)
echo "$UPDATED" | jq .
UPDATED_NAME=$(echo "$UPDATED" | jq -r '.connector_name')
if [ "$UPDATED_NAME" = "$NEW_NAME" ]; then
  echo -e "${GREEN}✓ Name successfully updated to: ${UPDATED_NAME}${NC}"
else
  echo -e "${RED}✗ Name update failed. Expected: ${NEW_NAME}, Got: ${UPDATED_NAME}${NC}"
  exit 1
fi
echo ""

# Step 9: Rotate API key
echo -e "${YELLOW}Step 9: Rotating API key for connector...${NC}"
ROTATE_RESPONSE=$(grpcurl -plaintext -d "{\"connector_id\":\"${CONNECTOR2_ID}\",\"invalidate_old_immediately\":true}" \
  $GRPC_HOST io.pipeline.connector.intake.ConnectorAdminService/RotateApiKey)
echo "$ROTATE_RESPONSE" | jq .
NEW_API_KEY=$(echo "$ROTATE_RESPONSE" | jq -r '.new_api_key')
echo -e "${GREEN}✓ API key rotated. New key: ${NEW_API_KEY:0:20}...${NC}"
echo ""

# Step 10: Set connector inactive
echo -e "${YELLOW}Step 10: Inactivating connector...${NC}"
grpcurl -plaintext -d "{\"connector_id\":\"${CONNECTOR2_ID}\",\"active\":false,\"reason\":\"E2E test cleanup\"}" \
  $GRPC_HOST io.pipeline.connector.intake.ConnectorAdminService/SetConnectorStatus | jq .
echo -e "${GREEN}✓ Connector set to inactive${NC}"
echo ""

# Step 11: List active connectors only (should not include inactivated one)
echo -e "${YELLOW}Step 11: Listing active connectors only...${NC}"
ACTIVE_CONNECTORS=$(grpcurl -plaintext -d "{\"account_id\":\"${ACCOUNT_ID}\",\"include_inactive\":false}" \
  $GRPC_HOST io.pipeline.connector.intake.ConnectorAdminService/ListConnectors)
echo "$ACTIVE_CONNECTORS" | jq .
ACTIVE_COUNT=$(echo "$ACTIVE_CONNECTORS" | jq -r '.connectors | length')
echo -e "${GREEN}✓ ${ACTIVE_COUNT} active connectors (should not include inactivated one)${NC}"
echo ""

# Step 12: Delete connector
echo -e "${YELLOW}Step 12: Deleting connector (soft delete)...${NC}"
grpcurl -plaintext -d "{\"connector_id\":\"${CONNECTOR1_ID}\",\"hard_delete\":false}" \
  $GRPC_HOST io.pipeline.connector.intake.ConnectorAdminService/DeleteConnector | jq .
echo -e "${GREEN}✓ Connector deleted${NC}"
echo ""

# Step 13: Verify deletion (should exist but be inactive)
echo -e "${YELLOW}Step 13: Verifying soft delete...${NC}"
DELETED=$(grpcurl -plaintext -d "{\"connector_id\":\"${CONNECTOR1_ID}\"}" \
  $GRPC_HOST io.pipeline.connector.intake.ConnectorAdminService/GetConnector)
echo "$DELETED" | jq .
IS_ACTIVE=$(echo "$DELETED" | jq -r '.active // false')
# Note: protobuf omits fields with default values, so missing active = false
if [ "$IS_ACTIVE" = "false" ] || [ "$IS_ACTIVE" = "null" ]; then
  echo -e "${GREEN}✓ Connector soft deleted (active=false or omitted)${NC}"
else
  echo -e "${RED}✗ Soft delete failed. Connector is still active: ${IS_ACTIVE}${NC}"
  exit 1
fi
echo ""

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}All E2E tests passed! ✓${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Test artifacts created:"
echo "  Account ID: ${ACCOUNT_ID}"
echo "  Connector 1: ${CONNECTOR1_ID} (deleted)"
echo "  Connector 2: ${CONNECTOR2_ID} (inactive)"
