-- Add deletion tracking fields to connectors table
ALTER TABLE connectors
    ADD COLUMN deleted_reason VARCHAR(500),
    ADD COLUMN deleted_at TIMESTAMP NULL;

-- Create index for querying deleted connectors
CREATE INDEX idx_connectors_deleted ON connectors(deleted_at);
