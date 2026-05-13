#!/usr/bin/env bash
# =============================================================================
# upload-document-e2e.sh
#
# End-to-end script demonstrating the full document-upload workflow via the
# connector-admin gRPC API.
#
# What this script does
# ---------------------
# 1.  Lists available connector types so you can pick one.
# 2.  Creates a new DataSource for a test account, capturing the one-time
#     plaintext API key.
# 3.  Validates the API key (this is exactly what the intake service calls
#     before accepting a document upload).
# 4.  Simulates updating the datasource metadata (e.g., adding upload limits).
# 5.  Rotates the API key and confirms the old key is rejected and the new
#     key is accepted.
# 6.  Disables the datasource (simulating a maintenance window) and confirms
#     that API-key validation is rejected while disabled.
# 7.  Re-enables the datasource.
# 8.  (Optional) Cleans up test data when DRY_RUN=false.
#
# Usage
# -----
#   # Default — uses localhost:18107, runs all steps, skips cleanup
#   ./scripts/upload-document-e2e.sh
#
#   # Point at a remote host
#   GRPC_HOST=admin.example.com:18107 ./scripts/upload-document-e2e.sh
#
#   # Run cleanup at the end (hard-deletes the test datasource)
#   DRY_RUN=false ./scripts/upload-document-e2e.sh
#
# Requirements
# ------------
#   grpcurl  — https://github.com/fullstorydev/grpcurl
#   jq       — https://stedolan.github.io/jq/
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HOST="${GRPC_HOST:-localhost:18107}"
ADMIN_SVC="ai.pipestream.connector.intake.v1.DataSourceAdminService"
# Pre-seeded connector IDs (populated by Flyway V1 migration + seed loader)
S3_CONNECTOR_ID="a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
FILE_CRAWLER_CONNECTOR_ID="b1ffc0aa-0d1c-5f09-cc7e-7cc0ce491b22"
# Unique account ID for this test run (uses timestamp to avoid collisions)
ACCOUNT_ID="test-upload-e2e-$(date +%s)"
DRY_RUN="${DRY_RUN:-true}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
grpc() {
    local method="$1"; shift
    grpcurl -plaintext "$@" "$HOST" "$ADMIN_SVC/$method"
}

grpc_d() {
    local method="$1"
    local data="$2"
    grpcurl -plaintext -d "$data" "$HOST" "$ADMIN_SVC/$method"
}

step() { echo -e "\n\033[1;34m=== $* ===\033[0m"; }
ok()   { echo -e "\033[0;32m✔  $*\033[0m"; }
fail() { echo -e "\033[0;31m✘  $*\033[0m"; exit 1; }
info() { echo -e "\033[0;36m   $*\033[0m"; }

check_deps() {
    for cmd in grpcurl jq; do
        command -v "$cmd" &>/dev/null || fail "Required tool not found: $cmd"
    done
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
check_deps

echo ""
echo "┌─────────────────────────────────────────────────────┐"
echo "│         Connector Admin — Document Upload E2E        │"
echo "├─────────────────────────────────────────────────────┤"
echo "│  Host       : $HOST"
echo "│  Account ID : $ACCOUNT_ID"
echo "│  Cleanup    : $([ "$DRY_RUN" = "true" ] && echo "skipped (DRY_RUN=true)" || echo "enabled")"
echo "└─────────────────────────────────────────────────────┘"

# ---------------------------------------------------------------------------
# Step 1 — List connector types
# ---------------------------------------------------------------------------
step "1 / 7  List available connector types"
grpc ListConnectorTypes | jq -r '.connectors[] | "  \(.connectorId)  \(.connectorType)  (\(.name))"'
ok "Connector types listed"

# ---------------------------------------------------------------------------
# Step 2 — Create datasource
# ---------------------------------------------------------------------------
step "2 / 7  Create a DataSource for account '$ACCOUNT_ID'"

CREATE_RESPONSE=$(grpc_d CreateDataSource "{
  \"account_id\": \"$ACCOUNT_ID\",
  \"connector_id\": \"$S3_CONNECTOR_ID\",
  \"name\": \"E2E Test DataSource — S3\",
  \"drive_name\": \"e2e-test-drive\",
  \"metadata\": {\"purpose\": \"e2e-test\", \"script\": \"upload-document-e2e.sh\"}
}")

echo "$CREATE_RESPONSE" | jq .

DATASOURCE_ID=$(echo "$CREATE_RESPONSE" | jq -r '.datasource.datasourceId')
API_KEY=$(echo "$CREATE_RESPONSE" | jq -r '.datasource.apiKey')

[ -z "$DATASOURCE_ID" ] || [ "$DATASOURCE_ID" = "null" ] && fail "Failed to extract datasource_id"
[ -z "$API_KEY"       ] || [ "$API_KEY"       = "null" ] && fail "Failed to extract api_key"

info "Datasource ID : $DATASOURCE_ID"
info "API Key       : $API_KEY"
ok "DataSource created"

# ---------------------------------------------------------------------------
# Step 3 — Validate API key (simulates what the intake service calls)
# ---------------------------------------------------------------------------
step "3 / 7  Validate API key (intake-service simulation)"

VALIDATE_RESPONSE=$(grpc_d ValidateApiKey "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"api_key\": \"$API_KEY\"
}")

echo "$VALIDATE_RESPONSE" | jq .

VALID=$(echo "$VALIDATE_RESPONSE" | jq -r '.valid')
[ "$VALID" = "true" ] || fail "API key should be valid but got: $VALID"
ok "API key validated — document upload would be authorised"

info ""
info "The 'config' block above is the merged DataSourceConfig that the intake"
info "service uses to route the document to the correct storage drive and apply"
info "ingestion policies (persistence, retention, encryption, hydration)."

# ---------------------------------------------------------------------------
# Step 4 — Update datasource metadata (e.g., add upload limits)
# ---------------------------------------------------------------------------
step "4 / 7  Update datasource (add upload limits)"

grpc_d UpdateDataSource "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"name\": \"E2E Test DataSource — S3 (updated)\",
  \"metadata\": {\"purpose\": \"e2e-test\", \"updated\": \"true\"}
}" | jq .
ok "DataSource updated"

# ---------------------------------------------------------------------------
# Step 5 — Rotate API key
# ---------------------------------------------------------------------------
step "5 / 7  Rotate API key"

ROTATE_RESPONSE=$(grpc_d RotateApiKey "{\"datasource_id\": \"$DATASOURCE_ID\"}")
echo "$ROTATE_RESPONSE" | jq .
NEW_API_KEY=$(echo "$ROTATE_RESPONSE" | jq -r '.newApiKey')
[ -z "$NEW_API_KEY" ] || [ "$NEW_API_KEY" = "null" ] && fail "Failed to extract new api_key"
info "New API Key : $NEW_API_KEY"

info "  (a) Verifying OLD key is now rejected ..."
OLD_VALID=$(grpc_d ValidateApiKey "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"api_key\": \"$API_KEY\"
}" | jq -r '.valid')
[ "$OLD_VALID" = "false" ] || fail "Old API key should be invalid after rotation but got: $OLD_VALID"
ok "  OLD key correctly rejected"

info "  (b) Verifying NEW key is accepted ..."
NEW_VALID=$(grpc_d ValidateApiKey "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"api_key\": \"$NEW_API_KEY\"
}" | jq -r '.valid')
[ "$NEW_VALID" = "true" ] || fail "New API key should be valid but got: $NEW_VALID"
ok "  NEW key accepted — update your crawler configuration with the new key"

API_KEY="$NEW_API_KEY"   # carry forward

# ---------------------------------------------------------------------------
# Step 6 — Disable datasource (maintenance window)
# ---------------------------------------------------------------------------
step "6 / 7  Disable datasource and verify uploads are rejected"

grpc_d SetDataSourceStatus "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"active\": false,
  \"reason\": \"e2e-maintenance-test\"
}" | jq .

DISABLED_VALID=$(grpc_d ValidateApiKey "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"api_key\": \"$API_KEY\"
}" | jq -r '.valid')
[ "$DISABLED_VALID" = "false" ] || fail "Disabled datasource should reject API key validation"
ok "API key correctly rejected while datasource is disabled"

# Re-enable
grpc_d SetDataSourceStatus "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"active\": true
}" | jq .
ok "DataSource re-enabled"

# ---------------------------------------------------------------------------
# Step 7 — Final validation / cleanup
# ---------------------------------------------------------------------------
step "7 / 7  Final validation"

FINAL_VALID=$(grpc_d ValidateApiKey "{
  \"datasource_id\": \"$DATASOURCE_ID\",
  \"api_key\": \"$API_KEY\"
}" | jq -r '.valid')
[ "$FINAL_VALID" = "true" ] || fail "Final API key validation failed"
ok "DataSource is healthy and ready to receive document uploads"

if [ "$DRY_RUN" = "false" ]; then
    info "  Cleaning up test datasource $DATASOURCE_ID ..."
    grpc_d DeleteDataSource "{\"datasource_id\": \"$DATASOURCE_ID\", \"hard_delete\": true}" | jq .
    ok "Test datasource deleted"
else
    info "  Skipping cleanup (DRY_RUN=true). To remove:"
    info "  grpcurl -plaintext -d '{\"datasource_id\": \"$DATASOURCE_ID\", \"hard_delete\": true}' \\"
    info "    $HOST $ADMIN_SVC/DeleteDataSource"
fi

echo ""
echo "┌─────────────────────────────────────────────────────┐"
echo "│                  All steps passed ✔                  │"
echo "│                                                      │"
echo "│  Summary                                             │"
echo "│  Account ID   : $ACCOUNT_ID"
echo "│  Datasource ID: $DATASOURCE_ID"
echo "│  Active Key   : $API_KEY"
echo "└─────────────────────────────────────────────────────┘"
echo ""
info "Next step: configure your S3 connector with this API key and point it at"
info "the connector-intake service.  The intake service will call ValidateApiKey"
info "on connector-admin to authorise each document submission."
