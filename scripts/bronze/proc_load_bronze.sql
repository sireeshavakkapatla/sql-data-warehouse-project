/*
===========================================================================================================
Stored Procedure: Load Bronze Layer (source--> Bronze)
======================================================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external csv files.
  It performs the following actions:
  -Truncates the bronze tables before loading data.
  -Uses the 'Bulk Insert' command to load data from CSV files to bronze tables.

parameters:
  None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC bronze.load_bronze;
=========================================================================================
*/

-------------------------------------data ingestion bulk insert from csv to bronze layer
create or alter procedure bronze.load_procedure as
begin
	declare @starttime datetime, @endtime datetime,@batch_start_time datetime,@batch_end_time datetime
	begin try
		set @batch_start_time=getdate();
		print '==================================================';
		print 'Loading Bronze layer';
		print '==================================================';

		print '--------------------------------------------------';
		print 'Loading CRM Tables';
		print '--------------------------------------------------';
		set @starttime = GETDATE();
		print '>> Truncating Table : bronze.crm_cust_info';

			truncate table bronze.crm_cust_info;

		print '>> Inserting data into : bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\siree\OneDrive\Documents\SQL_BARRA\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @endtime = GETDATE();
		print '.. Load duration: '+ cast(datediff(second,@starttime,@endtime) as nvarchar)+ ' Seconds';
		print'-----------------------';

		set @starttime = getdate();
		print '>> Truncating Table : bronze.crm_prd_info';
			truncate table bronze.crm_prd_info;

		print '>> Inserting data into : bronze.crm_prd_info';
		bulk insert  bronze.crm_prd_info
		from 'C:\Users\siree\OneDrive\Documents\SQL_BARRA\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @endtime = getdate();
		print 'Load duration: '+ cast( datediff(second,@starttime,@endtime) as nvarchar)+' seconds';
		print'------------------------------------';

		set @starttime = GETDATE();
		print '>> Truncating Table :  bronze.crm_sales_details';
			truncate table bronze.crm_sales_details;

		print '>> Inserting data into : bronze.crm_sales_details';
		bulk insert  bronze.crm_sales_details
		from 'C:\Users\siree\OneDrive\Documents\SQL_BARRA\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @endtime = getdate();
		print 'Load duration: '+ cast( datediff(second,@starttime,@endtime) as nvarchar)+' seconds';
		print'------------------------------------';

		print '--------------------------------------------------';
		print 'Loading ERP Tables';
		print '--------------------------------------------------';

		set @starttime=GETDATE();
		print '>> Truncating Table : bronze.erp_cust_az12';
			truncate table bronze.erp_cust_az12;

		print '>> Inserting data into : bronze.erp_cust_az12';
		bulk insert  bronze.erp_cust_az12
		from 'C:\Users\siree\OneDrive\Documents\SQL_BARRA\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @endtime = getdate();
		print 'Load duration: '+ cast( datediff(second,@starttime,@endtime) as nvarchar)+' seconds';
		print'------------------------------------';

		set @starttime=getdate();
		print '>> Truncating Table : bronze.erp_loc_a101';
			truncate table bronze.erp_loc_a101;
		print '>> Inserting data into : bronze.erp_loc_a101';
		bulk insert  bronze.erp_loc_a101
		from 'C:\Users\siree\OneDrive\Documents\SQL_BARRA\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @endtime = getdate();
		print 'Load duration: '+ cast( datediff(second,@starttime,@endtime) as nvarchar)+' seconds';
		print'------------------------------------';

		set @starttime = getdate();
		print '>> Truncating Table : bronze.erp_PX_CAT_G1V2';
			truncate table bronze.erp_PX_CAT_G1V2;
		print '>> Inserting data into : bronze.erp_PX_CAT_G1V2';
		bulk insert  bronze.erp_PX_CAT_G1V2
		from 'C:\Users\siree\OneDrive\Documents\SQL_BARRA\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @endtime = getdate();
		print 'Load duration: '+ cast( datediff(second,@starttime,@endtime) as nvarchar)+' seconds';
		print'------------------------------------';

		set @batch_end_time = GETDATE();
		print '====================================================================================='
		print 'Loading Bronze layer is completed';
		print 'Total Load duration: '+ cast( datediff(second,@batch_start_time,@batch_end_time) as nvarchar)+' seconds';
		print'=====================================================================================';

	end try
	begin catch
		print '==============================================================='
		print 'Error occured during loading Bronze Layer'
		print 'Error Message' + Error_Message();
		print 'Error Number' + cast(Error_number() as nvarchar);
		print 'Error State' + cast(Error_State() as nvarchar);
		print '================================================================'

	end catch
end

select count(*) from bronze.erp_PX_CAT_G1V2;

exec bronze.load_procedure;

