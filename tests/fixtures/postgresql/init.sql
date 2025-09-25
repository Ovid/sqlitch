-- PostgreSQL initialization script for sqlitch testing
-- This script sets up the test database with proper permissions

-- Create additional test databases if needed
CREATE DATABASE sqlitch_test_2;
CREATE DATABASE sqlitch_test_temp;

-- Grant permissions to sqlitch user
GRANT ALL PRIVILEGES ON DATABASE sqlitch_test TO sqlitch;
GRANT ALL PRIVILEGES ON DATABASE sqlitch_test_2 TO sqlitch;
GRANT ALL PRIVILEGES ON DATABASE sqlitch_test_temp TO sqlitch;

-- Connect to test database and set up schemas
\c sqlitch_test;

-- Ensure sqlitch user can create schemas
GRANT CREATE ON DATABASE sqlitch_test TO sqlitch;

-- Create a test schema for validation
CREATE SCHEMA IF NOT EXISTS test_schema;
GRANT ALL ON SCHEMA test_schema TO sqlitch;