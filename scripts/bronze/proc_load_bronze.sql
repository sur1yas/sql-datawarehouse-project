/*
===============================================================================
StoredProcedure: Load Bronze Layer (DataSource -> Bronze)
===============================================================================
- Truncates the bronze tables before loading data.
- Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

 EXEC bronze.load_bronze;
===============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	
	DECLARE @start_time DATETIME, @end_time DATETIME,
			@batch_start DATETIME, @batch_end DATETIME;

	BEGIN TRY
		
		SET @batch_start = GETDATE();

		PRINT '======================================';
		PRINT '		LOADING BRONZE LAYER	     ';
		PRINT '======================================';
		PRINT '';
  		PRINT '************ CRM TABLES **************';
  		PRINT '';

		SET @start_time = GETDATE();

		PRINT '>>TruncatingTable: bronze.crm_cust_info'; 
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>>InsertingTable : bronze.crm_cust_info'; 
		
		-- Bulk insertion with full load method of truncating & inserting
		-- Loading crm_cust_info
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\sur1y\Desktop\DWH Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();

		PRINT 'LoadTime: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';

----------------------------------------------------------------------------------------------------------------------

		SET @start_time = GETDATE();

		PRINT '';
		PRINT '>>TruncatingTable: bronze.crm_prd_info'; 
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>>InsertingTable : bronze.crm_prd_info'; 

		-- Bulk insertion with full load method of truncating & inserting
		-- Loading crm_prd_info
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\sur1y\Desktop\DWH Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();

		PRINT 'LoadTime: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';

----------------------------------------------------------------------------------------------------------------------
		
		SET @start_time = GETDATE();

		PRINT '';
		PRINT '>>TruncatingTable: bronze.crm_sales_details'; 
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>>InsertingTable : bronze.crm_sales_details'; 

		-- Bulk insertion with full load method of truncating & inserting
		-- Loading crm_sales_details
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\sur1y\Desktop\DWH Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();

		PRINT 'LoadTime: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';

----------------------------------------------------------------------------------------------------------------------
		
		SET @start_time = GETDATE();

		PRINT '************ ERP TABLES **************';
		PRINT '';
		PRINT '>>TruncatingTable: bronze.erp_cust_az12'; 
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>>InsertingTable : bronze.erp_cust_az12'; 

		-- Bulk insertion with full load method of truncating & inserting
		-- Loading erp_cust_az12
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\sur1y\Desktop\DWH Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();

		PRINT 'LoadTime: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';

----------------------------------------------------------------------------------------------------------------------
		
		SET @start_time = GETDATE();

		PRINT '';
		PRINT '>>TruncatingTable: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>>InsertingTable : bronze.erp_loc_a101'; 

		-- Bulk insertion with full load method of truncating & inserting
		-- Loading erp_loc_a101
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\sur1y\Desktop\DWH Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();

		PRINT 'LoadTime: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';

----------------------------------------------------------------------------------------------------------------------
		
		SET @start_time = GETDATE();

		PRINT '';
		PRINT '>>TruncatingTable: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>>InsertingTable : bronze.erp_px_cat_g1v2'; 

		-- Bulk insertion with full load method of truncating & inserting
		-- Loading erp_px_cat_g1v2
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\sur1y\Desktop\DWH Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();

		PRINT 'LoadTime: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';

		SET @batch_end = GETDATE();

		PRINT 'BronzeLayerLoaded';
		PRINT 'BatchLoadTime: ' + CAST(DATEDIFF(second,@batch_start,@batch_end) AS NVARCHAR) + ' seconds';

	END TRY						-- SQL runs TRY block exclusively
	BEGIN CATCH					-- On failure, runs the CATCH block to handle error
		PRINT '=========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ErrorMessage:' + ERROR_MESSAGE();
		PRINT 'ErrorNumber:' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '=========================================';
	END CATCH	
END

