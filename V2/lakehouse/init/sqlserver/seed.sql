IF DB_ID('shop') IS NULL CREATE DATABASE shop;
GO
USE shop;
GO
IF OBJECT_ID('dbo.customers') IS NULL
  CREATE TABLE dbo.customers (id INT IDENTITY PRIMARY KEY, name NVARCHAR(100) NOT NULL, country NVARCHAR(50), created_at DATETIME2 DEFAULT SYSUTCDATETIME());
IF OBJECT_ID('dbo.orders') IS NULL
  CREATE TABLE dbo.orders (id INT IDENTITY PRIMARY KEY, customer_id INT NOT NULL, status NVARCHAR(20) NOT NULL, amount DECIMAL(12,2) NOT NULL, updated_at DATETIME2 DEFAULT SYSUTCDATETIME());
GO
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name='shop' AND is_cdc_enabled=1) EXEC sys.sp_cdc_enable_db;
GO
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id=OBJECT_ID('dbo.customers')) EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='customers', @role_name=NULL;
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id=OBJECT_ID('dbo.orders')) EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='orders', @role_name=NULL;
GO
IF NOT EXISTS (SELECT 1 FROM dbo.customers) INSERT INTO dbo.customers(name,country) VALUES ('Alice','DE'),('Bob','IR'),('Carla','US');
IF NOT EXISTS (SELECT 1 FROM dbo.orders) INSERT INTO dbo.orders(customer_id,status,amount) VALUES (1,'new',120.50),(2,'paid',75.00),(3,'new',310.25);
GO
