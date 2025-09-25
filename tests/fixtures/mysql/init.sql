-- MySQL initialization script for sqlitch testing
-- This script sets up the test database with proper permissions

-- Create additional test databases if needed
CREATE DATABASE IF NOT EXISTS sqlitch_test_2;
CREATE DATABASE IF NOT EXISTS sqlitch_test_temp;

-- Grant permissions to sqlitch user
GRANT ALL PRIVILEGES ON sqlitch_test.* TO 'sqlitch'@'%';
GRANT ALL PRIVILEGES ON sqlitch_test_2.* TO 'sqlitch'@'%';
GRANT ALL PRIVILEGES ON sqlitch_test_temp.* TO 'sqlitch'@'%';

-- Ensure user can create databases and schemas
GRANT CREATE ON *.* TO 'sqlitch'@'%';

-- Use the main test database
USE sqlitch_test;

-- Create a test table for validation
CREATE TABLE IF NOT EXISTS test_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Flush privileges to ensure changes take effect
FLUSH PRIVILEGES;