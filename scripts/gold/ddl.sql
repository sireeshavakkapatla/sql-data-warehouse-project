
/*
======================================================================================
DDL Script: Create Gold views
=====================================================================================
Script Purpose:
This Script created views for the Gold layer in the data warehouse.
The Gold layer represents the final dimension and fact tables (star schema)

Each view performs transformations and combines data from the silver layer
to produce a clean, enriched and business ready dataset.

Usage:
-These views can be queried directly for analytics and reporting.
===================================================================================
*/
--====================================================================================
-- Create Dimension: gold.dim_customets
==================================================================================
use Datawarehouse;
if OBJECT_ID('gold.dim_customers','v') is not null
drop view gold.dim_customers;
go

create view gold.dim_customers as
SELECT  
	   row_number() over(order by cst_id) as customer_key,
	   ci.[cst_id] as customer_id
      ,ci.[cst_key] as customer_number
      ,ci.[cst_firstname] as customer_firstname
      ,ci.[cst_lastname] as customer_lastname
      ,ci.[cst_marital_status] as marital_status
      
      ,ci.[cst_created_date] as created_date ,
	  ca.bdate as birthdate,
	 case when ci.cst_gndr != 'unknown' then ci.cst_gndr----------------------crm is the master gender info
	else coalesce(ca.gen,'n/a')
	end as gender,
	  la.CNTRY as country
    
  FROM [Datawarehouse].[silver].[crm_cust_info] ci
  left join silver.erp_cust_az12 ca
  on ci.cst_key = ca.CID
  left join silver.erp_loc_a101 la
  on ci.cst_key=la.CID ;


----------------------------------------------------------------------------------------------------
if OBJECT_ID('gold.dim_products','v') is not null
drop view gold.dim_products;
go
create view gold.dim_products as
SELECT  
       ROW_NUMBER() over(order by pn.prd_start_dt,pn.cat_id) as product_key,
	   pn.[prd_id] as product_id,
		pn.[cat_id] as product_number,
       pn.[prd_nm] as product_name,
	   pn.[prd_key] as category_id,
	   pc.CAT as category,
	  pc.SUBCAT as subcategory,
      pn.[prd_cost] as cost
      ,pn.[prd_line] as product_line,
	  pc.MAINTENANCE
      ,pn.[prd_start_dt] as startdate
	  
FROM [Datawarehouse].[silver].[crm_prd_info] pn
  left join silver.erp_PX_CAT_G1V2 pc
  on pn.prd_key = pc.ID 
where prd_end_dt is null;---------------------------------filter out historical data

  select * from silver.erp_PX_CAT_G1V2;
  select * from gold.dim_products;
  ---------------------------------------------------------------------------------------------------------------gold facts
 if OBJECT_ID('gold.fact_sales','v') is not null
drop view gold.fact_sales;
go
  create view gold.fact_sales as 
  SELECT 
 
	   sd.[sls_ord_num] as order_number
	   ,cst.customer_key,
	   pr.product_key
      ,sd.[sls_order_dt] as order_date
      ,sd.[sls_ship_dt] as shipping_date
      ,sd.[sls_due_dt] as due_date
      ,sd.[sls_sales] as sales_amount
      ,sd.[sls_quantity] as sales_quantity
      ,sd.[sls_price] as sales_price
     
  FROM [Datawarehouse].[silver].[crm_sales_details] sd
  left join gold.dim_customers cst
  on sd.sls_cust_id = cst.customer_id
  left join gold.dim_products pr
  on sd.sls_prd_key = pr.product_number;


