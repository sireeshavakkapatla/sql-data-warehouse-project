/*
============================================================================
DDL Script : Create silver tables
===========================================================================
Script Purpose:
 THis script creates tables in the 'silver' schema, dropping existing tables if they already exist
Run this script to redefine the DDL structure of 'bronze' Tables.
=============================================================================================
*/

use Datawarehouse;

if OBJECT_ID('silver.crm_cust_info','u') is not null
drop table silver.crm_cust_info;

create table silver.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_created_date date,
dwh_create_date datetime2 default getdate()
);
if OBJECT_ID('silver.crm_prd_info','u') is not null
drop table silver.crm_prd_info;
create table silver.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime,
dwh_create_date datetime2 default getdate()
);
if OBJECT_ID('silver.crm_sales_details','u') is not null
drop table silver.crm_sales_details;
create table silver.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date datetime2 default getdate()
);
if OBJECT_ID('silver.erp_cust_az12','u') is not null
drop table silver.erp_cust_az12;
create table silver.erp_cust_az12(
CID nvarchar(50),
BDATE date, 
GEN nvarchar(20),
dwh_create_date datetime2 default getdate()
);
if OBJECT_ID('silver.erp_loc_a101','u') is not null
drop table silver.erp_loc_a101;

create table silver.erp_loc_a101(
CID nvarchar(50),
CNTRY nvarchar(50),
dwh_create_date datetime2 default getdate()
);
if OBJECT_ID('silver.erp_PX_CAT_G1V2','u') is not null
drop table silver.erp_PX_CAT_G1V2;

create table silver.erp_PX_CAT_G1V2(
ID nvarchar(50),
CAT nvarchar(50),
SUBCAT nvarchar(50),
MAINTENANCE nvarchar(10),
dwh_create_date datetime2 default getdate()
);
-----------------check for nulls or duplicates in primary key
----------expectation: No result
select cst_id,count(*) as dupid from bronze.crm_cust_info  group by cst_id having count(*)>1 or cst_id is null;

insert into silver.crm_cust_info(
cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_created_date
)
select cst_id ,
cst_key,
trim(cst_firstname) cst_firstname,
trim(cst_lastname) cst_lastname,---------------------------remove unwanted spaces

case when upper(trim(cst_marital_status))='S' then 'Single'
when upper(trim(cst_marital_status))='M' then 'Married'
else 'n/a'
end as cst_marital_status,----------------------------------Normalize marital status to readable format(Data Normalization)
case when upper(trim(cst_gndr))='F' then 'Female'
when  upper(trim(cst_gndr))='M' then 'Male'
else 'unknown'  ----------------------------------------------Handling missing data
end  as cst_gndr,---------------------------------------------(Data Normalization) & data standardization
cst_created_date
from
(select *,
ROW_NUMBER() over(partition by cst_id order by cst_created_date desc) flag_last  ----------------------removing duplicates
from bronze.crm_cust_info)t
where flag_last = 1 and cst_id is not null;

-------------------------check for unwanted spaces
select cst_firstname from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname);

select cst_lastname from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname);

select distinct cst_gndr from bronze.crm_cust_info;
select distinct cst_marital_status from bronze.crm_cust_info;

select * from bronze.crm_cust_info where cst_id is null;

-----------------------------------------------------------------check for clean data in silver table
-----------------check for nulls or duplicates in primary key

select cst_id,count(*) as dupid from silver.crm_cust_info  group by cst_id having count(*)>1 or cst_id is null;


select cst_firstname from silver.crm_cust_info
where cst_firstname != trim(cst_firstname);

select cst_lastname from silver.crm_cust_info
where cst_lastname != trim(cst_lastname);

select distinct cst_gndr from silver.crm_cust_info;
select distinct cst_marital_status from silver.crm_cust_info;

select * from silver.crm_cust_info where cst_id is null;
select * from silver.crm_cust_info;

if object_id('silver.crm_prd_info','u') is not null
drop table silver.crm_prd_info;

create table silver.crm_prd_info(
	prd_id int,
	prd_key nvarchar(50),
	cat_id nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date,
	dwh_create_date datetime2 default getdate()
);
insert into silver.crm_prd_info(
	prd_id ,
	prd_key ,
	cat_id ,
	prd_nm ,
	prd_cost ,
	prd_line ,
	prd_start_dt ,
	prd_end_dt 
	
	)
SELECT  [prd_id],
      
	  replace(SUBSTRING(prd_key,1,5),'-','_') cat_id, --------------------derived new columns
	  SUBSTRING(prd_key,7,len(prd_key)) prd_key
      ,[prd_nm]
      ,coalesce([prd_cost],0) [prd_cost], ------------------------------------instead of null 0
      
	  case  UPPER(trim(prd_line)) 
	  when 'M' then 'Mountain'
	  when 'R' then 'Road'
	  when 'S' then 'Other sales'
	  when  'T' then 'Touring'
	  else 'n/a'
	  end [prd_line] ------------------------------------------------data normalization
      ,cast([prd_start_dt] as date) prd_start_dt ----------------------data type casting
      , cast( lead(prd_start_dt) over(partition by prd_key order by prd_start_dt )-1  as date) prd_end_dt --------------data enrichment
		
  FROM [Datawarehouse].[bronze].[crm_prd_info];
  
  -------------------------------------------------------------------------
  where SUBSTRING(prd_key,7,len(prd_key)) 
  not in 
  (  select distinct sls_prd_key from bronze.crm_sales_details);
  ----------------------------------------------------------------check what ids not avail in erp cat table
  where replace(SUBSTRING(prd_key,1,5),'-','_') 
  not in(
  select distinct id from bronze.erp_PX_CAT_G1V2);
  --------------------------------------check nulls or duplicates in pk
  select prd_id,count(*) as dup from silver.crm_prd_info  group by prd_id having count (*) !=1 or prd_id is null;

  select distinct id from bronze.erp_PX_CAT_G1V2;
  select * from bronze.crm_sales_details;
  select count(*) from bronze.crm_prd_info;
  select distinct prd_line from silver.crm_prd_info;
  -----------------------------------------------------check invalid dates
  select * ,
   cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt )-1 as date) prd_end_test
  from silver.crm_prd_info
  where prd_end_dt < prd_start_dt;

  select * from silver.crm_prd_info where prd_end_dt<prd_start_dt;
-----------------------------------------------------------------------------------------------------------------------------------
insert into silver.crm_sales_details(
	   [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
)
SELECT  [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      
	  ,case when sls_order_dt = 0 or len(sls_order_dt) != 8 or sls_order_dt < 19000101 then null 
	  else cast(cast(sls_order_dt as varchar)as date) end sls_order_dt
      ,case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 or sls_order_dt < 19000101 then null 
	  else cast(cast(sls_ship_dt as varchar)as date) end sls_ship_dt
      ,case when sls_due_dt = 0 or len(sls_due_dt) != 8 or sls_due_dt < 19000101 then null 
	  else cast(cast(sls_due_dt as varchar)as date) end sls_due_dt
      ,case when sls_sales <=0 or sls_sales is null or sls_sales != (sls_quantity* abs(sls_price)) then (sls_quantity* abs(sls_price)) 
  else sls_sales
  end sls_sales,
      [sls_quantity]
      ,case when sls_price is null or sls_price <=0 then sls_sales/nullif(sls_quantity,0)
	else sls_price
	end as sls_price
  FROM [Datawarehouse].[bronze].[crm_sales_details]
  -------------------------------------------------------------------------------------
  select nullif(sls_order_dt,0) sls_order_dt from bronze.crm_sales_details
  where  sls_order_dt <=0 or
  len(sls_order_dt)>8 or
  sls_order_dt > 20500101 or
  sls_order_dt < 19000101;

  select sls_sales,sls_quantity,sls_price
  from silver.crm_sales_details
  where sls_sales ! = sls_quantity* sls_price or
  sls_sales is null or sls_quantity is null or sls_price is null
  or sls_sales <=0 or sls_quantity <=0 or sls_price <=0;

  select sls_sales as old_sls_sales,sls_quantity,sls_price as old_price,
  case when sls_sales <=0 or sls_sales is null or sls_sales != (sls_quantity* abs(sls_price)) then (sls_quantity* abs(sls_price)) 
  else sls_sales
  end sls_sales,----------------------------------Recalculate sales if original value is missing or incorrect
	case when sls_price is null or sls_price <=0 then sls_sales/nullif(sls_quantity,0)
	else sls_price
	end as sls_price--------------------------------------derive price if original value is invalid
  from bronze.crm_sales_details  
    where sls_sales ! = sls_quantity* sls_price or
  sls_sales is null or sls_quantity is null or sls_price is null
  or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
  order by sls_sales;

  select * from silver.crm_sales_details;
