-- 001_initial_dbos_schema.down.sql

-- Drop tables in reverse order to respect foreign key constraints
DROP TABLE IF EXISTS dbos.workflow_events;
DROP TABLE IF EXISTS dbos.notifications;
DROP TABLE IF EXISTS dbos.operation_outputs;
DROP TABLE IF EXISTS dbos.workflow_status;

-- Drop the schema (only if empty)
DROP SCHEMA IF EXISTS dbos;