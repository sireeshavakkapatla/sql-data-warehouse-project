/*
===========================================
customer report
===============================================
purpose:
-This report consolidates key customer metrics and behaviours

Highlights:
 1. Gathers essential fiels such as names,ages, and transaction details
 2.Segment customers into categories(VIP,Regular,New) and age groups.
 3.Aggregates customer level metrice
 -total sales
 -total orders
 -total quantity produced
 -total products
 -lifespan(in months)
 4.calculates valuable KPIs:
 -recency (months since last order)
 -average order value
 -avg monthly spend
 ==========================================
 */
 create or alter view gold.report_customers as
 with base_query as
 (
 select f.order_number,f.product_key,
 f.order_date,f.sales_amount,f.sales_quantity,c.customer_number,
 c.customer_key,concat(c.customer_firstname,' ',customer_lastname) as customer_name,c.birthdate,
 DATEDIFF(month,c.birthdate,GETDATE()) as age
 from 
 gold.fact_sales f
 left join
 gold.dim_customers c
 on f.customer_key =c.customer_key
 where order_date is not null )
 , customer_aggregations as (
 -------------------------------customer aggregations: summarizes key metrics at customer_level
 select customer_key,customer_name,customer_number,age,
 COUNT(distinct(order_number)) as total_orders,
 sum(sales_amount) as total_sales,
 sum((sales_quantity)) as total_quantity,
 count(distinct product_key) as total_products,
 max(order_date) as last_order_date,
 DATEDIFF(month,min(order_date), max(order_date)) as lifespan
 from base_query
 group by customer_key,customer_number,customer_name,age)
 select customer_key,customer_name,customer_number,age,
 case when age <20 then 'under 20'
 when age between 20 and 29 then '20-29'
  when age between 30 and 39 then '30-39'
   when age between 40 and 49 then '40-49'
   else '50 and above'
   end as age_grp,
 total_sales,total_orders,total_quantity,total_products,last_order_date,DATEDIFF(YEAR,last_order_date,getdate()) as recency,lifespan,
 case when lifespan >= 12 and total_sales > 5000 then 'VIP'
	when lifespan >= 12 and total_sales<= 5000 then 'Regular'
	else 'New' end as customer_seg,
	total_sales/total_orders as average_order_value,---------avg order value
	case when lifespan = 0 then total_sales
	else
	total_sales/lifespan end as monthly_spend ---------avg monthly spend
 from customer_aggregations
 ;

 select * from gold.report_customers;
