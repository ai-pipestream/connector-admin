-- Add status_reason column to connectors table
ALTER TABLE connectors ADD COLUMN status_reason VARCHAR(255) NULL;

-- Create index for status_reason queries
CREATE INDEX idx_connectors_status_reason ON connectors(status_reason);
