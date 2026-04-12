/*
==================================================================================
stored procedure : Load silver Layer (Bronze -> silver)
===================================================================================
Script Purpose :
  This stored procedure performs the ETL(Extract,Transform,Load) process to 
populate the silver schema tables from the 'bronze' schema.
Actions Performed:
-Truncates silver tables.
-Inserts transformed and cleansed data from bronze into silver tables.

Parameters :
None.
This stored procedure does not accept any parameters or return any values.

Usage Example :
 Exec silver.load_silver;
======================================================================================
*/

create or alter procedure silver.load_silver as
begin
	declare @start_time datetime, @end_time datetime,@batch_start_time datetime,@batch_end_time datetime;
		begin try
		set @batch_start_time = GETDATE();
		print '=====================================================================';
		print 'Loading silver layer'
		print '====================================================================';

		print '--------------------------------------------------------------------';
		print 'Loading CRM Tables';
		print '-------------------------------------------------------------------';

		--------------loading silver.crm_cust_info
		set @start_time = getdate();
		print '>>Truncating table : silver.crm_cust_info'------------add these three lines in every table before insert
		Truncate table silver.crm_cust_info;
		print '>>Inserting table : silver.crm_cust_info'
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
		where flag_last = 1 ;
		set @end_time = getdate();
		print '>> load duration:' +cast (datediff(second, @start_time, @end_time) as nvarchar)+'seconds';
		print'>>----';
		--------------------------------------------------------------------------------------------------------------------------------------------
		set @start_time = getdate();
		print '>>Truncating table : crm_prd_info'------------add these three lines in every table before insert
		Truncate table silver.crm_prd_info;
		print '>>Inserting table :crm_prd_info'
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
		  set @end_time = getdate();
		print '>> load duration:' +cast (datediff(second, @start_time, @end_time) as nvarchar)+'seconds';
		print'>>----';
		  --------------------------------------------------------------------------------------------------------------------------------------------------------
		  set @start_time = getdate();
		  print '>>Truncating table : crm_sales_details'------------add these three lines in every table before insert
		Truncate table silver.crm_sales_details;
		print '>>Inserting table :crm_sales_details'
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
		  end sls_sales,----------------------------------Recalculate sales if original value is missing or incorrect
			  [sls_quantity]
			  ,case when sls_price is null or sls_price <=0 then sls_sales/nullif(sls_quantity,0)
			else sls_price
			end as sls_price--------------------------------------derive price if original value is invalid
		  FROM [Datawarehouse].[bronze].[crm_sales_details];
		  set @end_time = getdate();
		print '>> load duration:' +cast (datediff(second, @start_time, @end_time) as nvarchar)+'seconds';
		print'>>----';
		  --------------------------------------------------------------------------------------------------------------------------------------------------------------
		  set @start_time = getdate();
		  print '>>Truncating table : erp_cust_az12'------------add these three lines in every table before insert
		Truncate table silver.erp_cust_az12;
		print '>>Inserting table : erp_cust_az12'

		insert into silver.erp_cust_az12 (cid,bdate,gen)
		SELECT  
				case when cid like 'NAS%' then substring(cid,4,len(cid))
				else cid
				end cid,
			case when bdate> getdate() then null 
			  else bdate end bdate,
			case when upper(trim(gen)) in ('F','Female') then 'Female'
				when upper(trim(gen)) in ('M','Male') then 'Male'
				else 'n/a'
				end gen
		FROM bronze.erp_cust_az12;
		set @end_time = getdate();
		print '>> load duration:' +cast (datediff(second, @start_time, @end_time) as nvarchar)+'seconds';
		print'>>----';
		--------------------------------------------------------------------------------------------------------------------------------------
		set @start_time = getdate();
		  print '>>Truncating table : erp_loc_a101'------------add these three lines in every table before insert
		Truncate table silver.erp_loc_a101;
		print '>>Inserting table : erp_loc_a101'
		  insert into silver.erp_loc_a101(cid,CNTRY)
		SELECT  
				replace(CID,'-','')cid,
			  case when trim(CNTRY) in ('US','USA') then 'United States'
				   when trim(cntry) = 'DE' then 'Germany' 
				   when trim(cntry) = '' or cntry is null then 'n/a'
				   else cntry
					end cntry
		  FROM [Datawarehouse].[bronze].[erp_loc_a101];
		  set @end_time = getdate();
		print '>> load duration:' +cast (datediff(second, @start_time, @end_time) as nvarchar)+'seconds';
		print'>>----';
		  ----------------------------------------------------------------------------------------------------------------------------
		  set @start_time = getdate();
		  print '>>Truncating table :erp_PX_CAT_G1V2'------------add these three lines in every table before insert
		Truncate table silver.erp_PX_CAT_G1V2;
		print '>>Inserting table : erp_PX_CAT_G1V2'
		  insert into silver.erp_PX_CAT_G1V2(id,cat,subcat,maintenance)
		  SELECT  [ID]
			  ,[CAT]
			  ,[SUBCAT]
			  ,[MAINTENANCE]
      
		  FROM [Datawarehouse].[bronze].[erp_PX_CAT_G1V2];
		  set @end_time = getdate();
		print '>> load duration:' +cast (datediff(second, @start_time, @end_time) as nvarchar)+'seconds';
		print'>>----';
	  set @batch_end_time = getdate();
	  print '=================================================='
	  print 'Loading silver layer is completed';
	  print '-Total Load Duration: '+ cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar)+'seconds';
	  print '==============================================='

	  end try
	  begin catch
	  print '=================================================='
	  print 'error occured during loading bronze layer';
	  print 'Error Message' + Error_message();
	  print 'Error Message' + cast(error_number() as nvarchar);
	  print 'Error Message' + cast(error_state() as nvarchar);
	  print '==============================================='
	  end catch
end

exec silver.load_silver;
exec bronze.load_procedure;
