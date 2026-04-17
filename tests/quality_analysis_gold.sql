use datawarehouse;
select * from gold.fact_sales;

select MONTH(order_date) as emonth, sum(sales_amount) as totalsales
from gold.fact_sales
group by MONTH(order_date) order by emonth;

--------------------------running total(cummulative analysis)
select mon,totalsales,
sum(totalsales) over (partition by mon order by mon ) as runningtotal
from (
select DATETRUNC(month,order_date) as mon, sum(sales_amount) as totalsales
from gold.fact_sales
where order_date is not null
group by DATETRUNC(month,order_date)
)t;
-------------------------------year
select order_date,totalsales,
sum(totalsales) over ( order by order_date ) as runningtotal,
avg(avg_price) over(order by order_date) as runningavg
from (
select DATETRUNC(YEAR,order_date) as order_date, sum(sales_amount) as totalsales,
avg(sales_price) as avg_price
from gold.fact_sales
where order_date is not null
group by DATETRUNC(YEAR,order_date)
)t;

----------------------------------------performance analysis
---analyze the yearly performance  of products by comparing their sales to 
--both the avg sales performance of the product and the prev years sales--yoy analysis
with yearly_prds_sales as(

select p.product_name,
year(order_date) as order_year,
sum(s.sales_amount) as totalsales
from
gold.fact_sales s
left join
gold.dim_products p
on s.product_key=p.product_key
where order_date is not null
group by p.product_name,year(order_date)

)
select product_name,order_year,totalsales,
avg(totalsales) over(partition by product_name) avg_sales,
totalsales-avg(totalsales) over(partition by product_name)  as difference,
case when totalsales-avg(totalsales) over(partition by product_name) >0 then 'Above avg'
 when totalsales-avg(totalsales) over(partition by product_name) <0 then 'below avg'
else 'avg'
end as pv_status,
lag(totalsales) over (partition by product_name order by order_year) as prev_sales,
totalsales-lag(totalsales) over (partition by product_name order by order_year) as current_vs_prev_sales,
case when totalsales-lag(totalsales) over (partition by product_name order by order_year) >0 then 'Increase'
     when totalsales-lag(totalsales) over (partition by product_name order by order_year)<0 then 'Decrease'
else 'No change'
end as sales_status
from yearly_prds_sales
;

------------------part to whole analysis
-----which categories contribute the most to overall sales
with cte as(
select p.category,sum(f.sales_amount) as totalsales
from gold.fact_sales f
left join
gold.dim_products p
on f.product_key = p.product_key
group by p.category
)
select category,totalsales,
sum(totalsales) over() as overallsales,
concat(round((cast(totalsales as float)/sum(totalsales) over())*100,2),'%') as percent_sales
from cte;

-------------------------Data segmentation
--------measure by measure--segment products into cost ranges and count how many prod fall into that each segment
with product_range as 
(
select product_key,product_name,cost,
case when cost <100 then 'below 100'
     when cost between 100 and 500 then '100-500'
	 when cost between 500 and 1000 then '500-1000'
	 else 'above 100'
	 end as cost_range
from gold.dim_products)
select count(product_name) as product_list,cost_range 

from product_range
group by cost_range
order by cost_range;
------------------------------------------------------------------------------
--group cust into 3 segments based on their spending behaviour
----VIP: cust with atleast 12 months of history and spending more than 5000.
---Regular: customers atleast 12 months of his but spending 5000 or less
--New: customers with lifespan less than 12 months
with customer_segment as
(
select c.customer_key,sum(f.sales_amount)as total_spending ,
min(order_date) as first_order,
max(order_date) as last_order,
DATEDIFF(month,min(order_date), max(order_date)) as life_span
from gold.dim_customers c
left join gold.fact_sales f
on c.customer_key=f.customer_key
group by c.customer_key
)
select customer_seg,count(customer_key) as total_cust
from (
	select customer_key,
	case when life_span >= 12 and total_spending > 5000 then 'VIP'
	when life_span >= 12 and total_spending <= 5000 then 'Regular'
	else 'New' end as customer_seg

	from customer_segment
)t
group by customer_seg
order by count(customer_key) desc;

