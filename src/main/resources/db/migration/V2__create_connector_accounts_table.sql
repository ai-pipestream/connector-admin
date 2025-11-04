-- Create connector_accounts junction table for many-to-many relationship
-- Note: account_id has NO foreign key constraint - accounts are managed by account-service
-- Connector-service validates accounts exist via gRPC before creating links
CREATE TABLE IF NOT EXISTS connector_accounts (
    connector_id VARCHAR(255) NOT NULL,
    account_id VARCHAR(255) NOT NULL,
    PRIMARY KEY (connector_id, account_id),
    FOREIGN KEY (connector_id) REFERENCES connectors(connector_id) ON DELETE CASCADE
);

-- Create indexes for lookup performance
CREATE INDEX idx_connector_accounts_connector ON connector_accounts(connector_id);
CREATE INDEX idx_connector_accounts_account ON connector_accounts(account_id);
