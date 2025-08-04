-- Create a custom database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'test_db')
BEGIN
    CREATE DATABASE test_db;
END
GO

-- Switch to the new database
USE test_db;
GO

-- Create a schema for organization
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'app')
BEGIN
    EXEC('CREATE SCHEMA app');
END
GO
