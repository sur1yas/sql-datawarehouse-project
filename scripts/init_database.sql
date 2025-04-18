/*
----------------------------------------------------------
CREATE DATABASE and SCHEMA
----------------------------------------------------------
Purpose:
This script creates a 'DataWarehouse' named DATABASE after checking its existence.
If the named databse exists, it is dropped and recreated.
The script also creates 'bronze', 'silver', 'gold' named SCHEMAS in the DATABASE.

CAUTION NOTE: It is programmed in a way to delete the named DATABASE everytime it is runned.
*/

USE master;
GO

-- Checking any pre-existence and dropping if any
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET single_user WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Creating a DATAWAREHOUSE database
CREATE DATABASE DataWarehouse;
GO

-- Creating Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
