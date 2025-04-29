-- Naming convention: schema.source_filename

-- Create crm_cust_info
IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
		DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info(
		cst_id				INT,
		cst_key				NVARCHAR(50),
		cst_firstname			NVARCHAR(50),
		cst_lastname			NVARCHAR(50),
		cst_marital_status 	 	NVARCHAR(50),
		cst_gndr			NVARCHAR(50),
		cst_create_date			DATETIME
);
GO

-- Create crm_prd_info
IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
		DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info(
		prd_id			INT,
		prd_key			NVARCHAR(50),
		prd_nm			NVARCHAR(50),
		prd_cost		INT,
		prd_line		NVARCHAR(50),
		prd_start_dt		DATETIME,
		prd_end_dt		DATETIME
);
GO

-- Create crm_sales_details
IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
		DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details(
		sls_ord_num		NVARCHAR(50),
		sls_prd_key		NVARCHAR(50),
		sls_cust_id		INT,
		sls_order_dt		DATETIME,
		sls_ship_dt		DATETIME,
		sls_due_dt		DATETIME,
		sls_sales		INT,
		sls_quantity		INT,
		sls_price		INT
);
GO

-- Create erp_cust_az12
IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
		DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12(
		cid			NVARCHAR(50),
		bdate			DATETIME,
		gen			NVARCHAR(50)
);
GO

-- Create erp_loc_a101
IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
		DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101(
		cid			NVARCHAR(50),
		cntry			NVARCHAR(50)
);
GO

-- Create erp_px_cat_g1v2
IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
		DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2(
		id			NVARCHAR(50),
		cat			NVARCHAR(50),
		subcat			NVARCHAR(50),
		maintenance		NVARCHAR(50)
);
