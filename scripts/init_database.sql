-- ============================================================================
-- Create Database and Schemas in PostgreSQL
-- ============================================================================
-- Script Purpose:
--     This script creates a new database named 'DataWarehouse' after checking 
--     if it already exists. If the database exists, it is dropped and recreated. 
--     Additionally, the script sets up three schemas: 'bronze', 'silver', and 'gold'.
--
-- WARNING:
--     Running this script will drop the entire 'DataWarehouse' database if it exists. 
--     All data in the database will be permanently deleted. Proceed with caution 
--     and ensure you have proper backups before running this script.
-- ============================================================================

-- Drop database if it exists
DROP DATABASE IF EXISTS DataWarehouse;

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

-- Connect to the 'DataWarehouse' database
\c DataWarehouse;

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

