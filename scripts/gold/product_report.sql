/*
 ====================================
 product report
 ====================================
 purpose:
 - this report consolidates key product metrics and behaviours.

 highlights:
 1. Gather essential fields such as product name,category, subcategory,cost
 2.segment products by revenue to identofy high performers,mid -range, or low performers
 3.aggregate product level metrics
 -total orders
 -total sales
 -total quantity sold
 -total customer(unique)
 -lifespan (in months)
 4. calculate valuable KPIs:
 recency (months since last sale)
 -avg order revenue(AOR)
 -avg monthly revenue
 ============================================================
 */
 create or alter view gold.report_products as 
 with product_base_query as (
 select p.product_name,p.product_key,p.category,p.subcategory,p.cost,
 f.order_number,f.customer_key,f.order_date,f.sales_amount,f.sales_quantity
 from 
 gold.fact_sales f
 left join 
 gold.dim_products p
 on f.product_key=p.product_key
 where order_date is not null),
 --------------------------------------------------------------------------------------
-- ---product aggregations
------------------------------------------------------------------------------------------------------------------------------------
 product_aggregations as (
 select product_name,product_key,category,subcategory,cost,count(distinct order_number) total_orders,count(distinct customer_key) as customers,
 sum(sales_amount) as total_sales,sum(sales_quantity) as total_quantity,
 max(order_date) as last_order_date,
 DATEDIFF(month,min(order_date), max(order_date)) as lifespan,
round(avg(cast(sales_amount as float)/nullif(sales_quantity,0)),1) as avg_selling_price

 from
 product_base_query
 group by 
 product_name,product_key,category,subcategory,cost
 )
 -------------------------------------------------------------------------------
 ---final query
 -------------------------------------------------
 select 
 product_name,product_key,category,subcategory,last_order_date,
 DATEDIFF(month,last_order_date,getdate()) as recency_in_months,
 case when total_sales>50000 then 'High performers'
	  when	total_sales>=10000 then 'Mid-Range'
	  else 'Low performer'
	  end as product_segment,
	  lifespan,total_orders,total_sales,total_quantity,customers,avg_selling_price,
	  ---avg order revenue
	  case when total_orders = 0 then 0
	  else
	  total_sales/total_orders
	  end as avg_order_revenue,
	  ------------------avg monthly revenue
	  case when lifespan = 0 then total_sales
	  else total_sales/lifespan
	  end as avg_monthly_revenue
	  from product_aggregations;


select * from gold.report_products;
