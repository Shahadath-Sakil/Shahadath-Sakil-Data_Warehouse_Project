/*
=============================================================
Create Database and Schemas
=============================================================
*/
USE master;
GO
-- Drop and recreate the 'Data_Warehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Data_Warehouse')
BEGIN
	ALTER DATABASE Data_Warehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE Data_Warehouse;
END;
GO
CREATE DATABASE Data_Warehouse;
GO

-- Create the 'DataWarehouse' database
USE Data_Warehouse;
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
