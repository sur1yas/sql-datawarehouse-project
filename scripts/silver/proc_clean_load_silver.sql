/*
===============================================================================
StoredProcedure: Clean & Load Silver Layer (Bronze -> Silver)
===============================================================================
- Truncate Silver tables.
- Insert transformed and cleansed data from Bronze into Silver tables.

 Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	
	  DECLARE @start_time DATETIME, @end_time DATETIME, 
			  @batch_start_time DATETIME, @batch_end_time DATETIME; 

	  BEGIN TRY

		  SET @batch_start_time = GETDATE();
		
  		PRINT '======================================';
  		PRINT '	       LOADING SILVER LAYER 		 ';
  		PRINT '======================================';
  		PRINT '';
  		PRINT '************ CRM TABLES **************';
  		PRINT '';
  
  		SET @start_time = GETDATE();
  
  		PRINT '>>TruncatingTable: silver.crm_cust_info'; 
  		TRUNCATE TABLE silver.crm_cust_info;
  		PRINT '>>InsertingTable : silver.crm_cust_info'; 
  		
  		-- CLEAN & LOAD crm_cust_info
  		INSERT INTO silver.crm_cust_info(
  				 cst_id,
  				 cst_key,
  				 cst_firstname,
  				 cst_lastname,
  				 cst_marital_status,
  				 cst_gndr,
  				 cst_create_date
  			)
  		SELECT 
  			cst_id,
  			cst_key,
  			TRIM(cst_firstname) as cst_firstname,			-- Data consistency
  			TRIM(cst_lastname) as cst_lastname,
  			CASE UPPER(TRIM(cst_marital_status))			-- DATA CLEANING
  				WHEN 'S' THEN 'Single'
  				WHEN 'M' THEN 'Married'
  				ELSE 'N/A'
  			END cst_marital_status,							-- Normalisation & Standardisation
  			CASE UPPER(TRIM(cst_gndr))						-- Handling missing data
  				WHEN 'M' THEN 'Male'
  				WHEN 'F' THEN 'Female'
  				ELSE 'N/A'
  			END cst_gndr,									-- Normalisation & Standardisation (Friendly format)
  			cst_create_date
  		FROM(
  			SELECT *,											-- Eliminate duplicates 
  				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
  			FROM bronze.crm_cust_info
  			WHERE cst_id IS NOT NULL
  		)t
  		WHERE flag_last = 1;									-- Filtering			
  
  		SET @end_time = GETDATE();
  
  		PRINT 'LoadTime: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
  		PRINT '---------------------------------------';
		
----------------------------------------------------------------------------------------------------------------------
		
  		SET @start_time = GETDATE();
  
  		PRINT '';
  		PRINT '>>TruncatingTable: silver.crm_prd_info'; 
  		TRUNCATE TABLE silver.crm_prd_info;
  		PRINT '>>InsertingTable : silver.crm_prd_info'; 
  		
  		-- CLEAN & LOAD crm_prd_info
  		INSERT INTO silver.crm_prd_info(
  				prd_id,
  				cat_id,
  				prd_key,
  				prd_nm,
  				prd_cost,
  				prd_line,
  				prd_start_dt,
  				prd_end_dt
  		)
  		SELECT prd_id,
  			REPLACE(SUBSTRING(TRIM(prd_key),1,5),'-','_') AS cat_id,			-- Extract category ID
  			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,						-- Extract product key 
  			prd_nm,
  			COALESCE(prd_cost,0) AS prd_cost,									-- Handling missing data
  			CASE UPPER(TRIM(prd_line))											-- Data Normalization
  				WHEN 'M' THEN 'Mountain'
  				WHEN 'R' THEN 'Roads'
  				WHEN 'T' THEN 'Touring'
  				WHEN 'S' THEN 'Others'
  				ELSE 'N/A'
  			END AS prd_line,
  			CAST(prd_start_dt AS DATE) AS prd_start_dt,
  			CASE WHEN prd_start_dt > prd_end_dt									-- Data Enrichment
  				THEN DATEADD(day,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) 
  				ELSE prd_end_dt
  			END AS prd_end_dt -- Calculating end date as 1day prior to next start date
  		FROM (
  			SELECT *,
  			ROW_NUMBER() OVER(PARTITION BY prd_id ORDER BY prd_id) nFlag
  			FROM bronze.crm_prd_info
  			WHERE prd_id IS NOT NULL
  		)t 
  		WHERE nFlag = 1;
  
  		SET @end_time = GETDATE();
  
  		PRINT 'LoadTime: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
  		PRINT '---------------------------------------';
  		
----------------------------------------------------------------------------------------------------------------
		
  		SET @start_time = GETDATE();
  
  		PRINT '';
  		PRINT '>>TruncatingTable: silver.crm_sales_details'; 
  		TRUNCATE TABLE silver.crm_sales_details;
  		PRINT '>>InsertingTable : silver.crm_sales_details'; 
  
  		-- CLEAN crm_sales_details
  		INSERT INTO silver.crm_sales_details(
  				sls_ord_num,
  				sls_prd_key,
  				sls_cust_id,
  				sls_order_dt,
  				sls_ship_dt,
  				sls_due_dt,
  				sls_sales,
  				sls_quantity,
  				sls_price
  		)
  		SELECT sls_ord_num,
  			sls_prd_key,
  			sls_cust_id,
  			sls_order_dt,
  			sls_ship_dt,
  			sls_due_dt,
  			CASE																 
  				WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
  					THEN sls_quantity * ABS(sls_price)
  				ELSE sls_sales
  			END AS sls_sales,													-- Valid data validation				
  			sls_quantity,
  			CASE 
  				WHEN sls_price IS NULL OR sls_price <=0
  					THEN sls_sales / NULLIF(sls_quantity,0)
  				ELSE sls_price
  			END AS sls_price													-- Valid data validation
  		FROM bronze.crm_sales_details;
  
  		SET @end_time = GETDATE();
  
  		PRINT 'LoadTime: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
  		PRINT '---------------------------------------';
  		
---------------------------------------------------------------------------------------------------------
		
  		SET @start_time = GETDATE();
  
  		PRINT '************ ERP TABLES **************';
  		PRINT '';
  		PRINT '>>TruncatingTable: silver.erp_cust_az12'; 
  		TRUNCATE TABLE silver.erp_cust_az12;
  		PRINT '>>InsertingTable : silver.erp_cust_az12'; 
  
  		-- CLEAN & LOAD erp_cust_az12
  		INSERT INTO silver.erp_cust_az12(
  				cid,
  				bdate,
  				gen
  		)
  		SELECT
  			CASE LEN(cid)														-- Data Matching
  				WHEN 13 THEN SUBSTRING(TRIM(cid), 4, LEN(cid))
  				ELSE cid
  			END AS cid,
  			CASE																-- Eliminate future birthdates
  				WHEN bdate > GETDATE() THEN NULL
  				ELSE bdate														
  			END AS bdate,															
  			CASE																-- Data Normalization & handling unknown values
  				WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
  				WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
  				ELSE 'N/A'
  			END AS gen
  		FROM bronze.erp_cust_az12;
  
  		SET @end_time = GETDATE();
  
  		PRINT 'LoadTime: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
  		PRINT '---------------------------------------';
  		
---------------------------------------------------------------------------------------------------------------
		
  		SET @start_time = GETDATE();
  
  		PRINT '';
  		PRINT '>>TruncatingTable: silver.erp_loc_a101'; 
  		TRUNCATE TABLE silver.erp_loc_a101;
  		PRINT '>>InsertingTable : silver.erp_loc_a101'; 
  
  		-- CLEAN & LOAD erp_loc_a101
  		INSERT INTO silver.erp_loc_a101(
  				cid,
  				cntry
  		)
  		SELECT
  			REPLACE(cid, '-', '') AS cid,										-- Data matching over Keys
  			CASE 
  				WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'
  				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
  				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
  				ELSE TRIM(cntry)
  			END AS cntry														-- Data Normalizationand handling missing data
  		FROM bronze.erp_loc_a101;
  
  		SET @end_time = GETDATE();
  
  		PRINT 'LoadTime: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
  		PRINT '---------------------------------------';
  		
--------------------------------------------------------------------------------------------------------------
		
  		SET @start_time = GETDATE();
  
  		PRINT '';
  		PRINT '>>TruncatingTable: silver.erp_px_cat_g1v2'; 
  		TRUNCATE TABLE silver.erp_px_cat_g1v2;
  		PRINT '>>InsertingTable : silver.erp_px_cat_g1v2'; 
  
  		-- CLEAN & LOAD erp_px_cat_g1v2
  		INSERT INTO silver.erp_px_cat_g1v2 (
  				id,
  				cat,
  				subcat,
  				maintenance
  		)
  		SELECT id,
  			cat,
  			subcat,
  			maintenance
  		FROM bronze.erp_px_cat_g1v2;
  
  		SET @end_time = GETDATE();
  
  		PRINT 'LoadTime: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
  		PRINT '---------------------------------------';
  
  	END TRY
  	BEGIN CATCH
  		PRINT '=========================================='
  		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
  		PRINT 'Error Message' + ERROR_MESSAGE();
  		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
  		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
  		PRINT '=========================================='
  	END CATCH
  END
