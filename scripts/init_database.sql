/*

=================================================================================
Create Database and Schemas
===============================================================================
Script Purpose :
    This script creates a new database named 'Datawarehouse' after checking if it already exists.
    If the database exists, it is dropped and recreated. Additionally, the script sets up for 3 schemas 
    within the database : 'bronze','silver','gold'.

Warning:
    Running this script will drop the entire 'Datawarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution and
    ensure you have proper backups before running this script.

*/


    --create database 'datawarehouse'

use master;

----------------------------------------------drop and recreate 'datawarehouse database'
if exists(select 1 from sys.databases where name = 'Datawarehouse')
begin
	alter database Datawarehouse 
	set single_user 
	with rollback immediate;
	drop database Datawarehouse;
end;
------------------------------------------check if the databse exists or not
select 
case when exists
(
select 1 from sys.databases
where name = 'Datawarehouse'
) then 'exists'
else 'not exists'
end as databasestatus;

create database Datawarehouse;
use datawarehouse;
-----------------------------------------create schema
create schema bronze;
go
create  schema silver;
go
create schema gold;
go

drop schema silver;
go
drop schema gold;
go
