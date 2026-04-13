/*
==============================================================================
Quality Checks
==============================================================================
Script Purpose:
This script performs quality checks to validate the integrity, consistency,
and accuracy of the gold layer. Thses checks ensure:
-Uniqueness of surrogate keys in dimension tables
-Referential integrity between fact and dimension tables.
-Validation of relationships in the data model for analytical purposes

Usage Notes:
- Run these checks after data loading silver layer
- Investigate and resolve any discrepancies found during the checks.
============================================================================================
*/
select customer_key,
count(*) as dup from gold.dim_customers
group by customer_key having count(*)>1;
==========================================================================

select distinct ci.[cst_gndr],ca.GEN ,
  case when ci.cst_gndr != 'unknown' then ci.cst_gndr
  else coalesce(ca.gen,'n/a')
  end as new_gen
  FROM [Datawarehouse].[silver].[crm_cust_info] ci
  left join silver.erp_cust_az12 ca
  on ci.cst_key = ca.CID
  left join silver.erp_loc_a101 la
  on ci.cst_key=la.CID order by 1,2;
select * from silver.erp_cust_az12; 
select * from silver.erp_loc_a101;
delete from [silver].[crm_cust_info] where cst_id is null;
select distinct gender from gold.dim_customers;
======================================================================================

  select * from silver.erp_PX_CAT_G1V2;
  select * from gold.dim_products;

select * from gold.fact_sales s
  left join gold.dim_customers c
  on s.customer_key = c.customer_key 
  left join gold.dim_products p
  on s.product_key = p.product_key where p.product_key is null;
