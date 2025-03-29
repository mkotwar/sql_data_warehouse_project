/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' also the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	

*/



use master;
go

-- Create the 'DataWarehouse' database
create database DataWarehouse;
go


use DataWarehouse;
go

--Create Schemas

create schema bronze;
go

create schema silver;
go

create schema gold;
go
