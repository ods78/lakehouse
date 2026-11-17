-- SQL Server source setup for Debezium CDC.
-- Applied by ./lakehouse.sh seed-mssql (sqlcmd with -v DEBEZIUM_PASSWORD=...).
-- Tables/DML below are idempotent.

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'lakehouse_src')
BEGIN
    CREATE DATABASE lakehouse_src;
END
GO

USE lakehouse_src;
GO

-- 1) Enable CDC on the database
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'lakehouse_src' AND is_cdc_enabled = 1)
    EXEC sys.sp_cdc_enable_db;
GO

-- 2) Operational table: customers
IF OBJECT_ID('dbo.customers', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.customers (
        id          INT IDENTITY(1,1) PRIMARY KEY,
        name        NVARCHAR(100)  NOT NULL,
        email       NVARCHAR(200)  NOT NULL,
        city        NVARCHAR(100)  NULL,
        updated_at  DATETIME2      DEFAULT SYSDATETIME()
    );
END
GO

-- 3) Enable CDC for the table
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'dbo' AND t.name = 'customers' AND t.is_tracked_by_cdc = 1)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'customers',
        @role_name            = N'cdc_role',
        @supports_net_changes = 0;
END
GO

-- 4) Debezium login (password injected via sqlcmd -v)
IF NOT EXISTS (SELECT name FROM sys.sql_logins WHERE name = 'debezium')
    CREATE LOGIN debezium WITH PASSWORD = '$(DEBEZIUM_PASSWORD)', CHECK_POLICY = OFF;
GO
IF NOT EXISTS (SELECT name FROM sys.database_principals WHERE name = 'debezium' AND type = 'S')
    CREATE USER debezium FOR LOGIN debezium;
GO
-- Homelab-grade grant (least-privilege checklist in docs/runbook.md)
ALTER ROLE db_datareader ADD MEMBER debezium;
GRANT VIEW DEFINITION TO debezium;
GO
-- Debezium must be a member of the CDC gating role to read change data
IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'cdc_role' AND type = 'R')
   AND NOT EXISTS (SELECT 1
                   FROM sys.database_role_members rm
                   JOIN sys.database_principals r ON r.principal_id = rm.role_principal_id
                   JOIN sys.database_principals m ON m.principal_id = rm.member_principal_id
                   WHERE r.name = 'cdc_role' AND m.name = 'debezium')
BEGIN
    ALTER ROLE cdc_role ADD MEMBER debezium;
END
GO
-- 5) Seed rows (only if empty)
IF NOT EXISTS (SELECT 1 FROM dbo.customers)
BEGIN
    INSERT INTO dbo.customers (name, email, city) VALUES
        ('Ava Müller',      'ava@example.com',   'Berlin'),
        ('Liam Chen',       'liam@example.com',  'Singapore'),
        ('Sofia Rossi',     'sofia@example.com', 'Milano'),
        ('Noah Dupont',     'noah@example.com',  'Paris'),
        ('Olivia Novak',    'olivia@example.com','Praha'),
        ('Emma Johansson',  'emma@example.com',  'Stockholm'),
        ('Lucas Silva',     'lucas@example.com', 'São Paulo'),
        ('Mia Kowalski',    'mia@example.com',   'Warszawa');
END
GO
