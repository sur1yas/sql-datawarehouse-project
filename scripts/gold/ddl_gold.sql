/*
===============================================================================
DDL Script: Create Gold View
===============================================================================
- Creates views for the Gold layer in the data warehouse representing the final 
  dimension and fact tables (Star Schema).
- Transformed and combined data from the Silver layer to produce a clean, 
  enriched, and business-ready dataset.
===============================================================================
*/

-- Create Dimension table: gold.dim_customers
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,     -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr           -- Primary source for gender = CRM
        ELSE COALESCE(ca.gen, 'n/a')  			                 -- Atcase refer ERP
    END                                AS gender,
    ca.bdate                           AS birth_date,
    ci.cst_create_date                 AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

----------------------------------------------------------------------------------------------
  
-- Create Dimension table: gold.dim_products
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;                                 -- Current/present/relevant date data 
GO                                                           -- ie filtering historical data

----------------------------------------------------------------------------------------------------

-- Create Fact Table: gold.fact_sales
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,                          -- Foreign Key(dim_products -> surrogate key)(connecting the Dim w Fact)
    cu.customer_key AS customer_key,                         -- Foreign Key(dim_customers -> surrogate key)(connecting the Dim w Fact)
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr                               -- Data Lookup 
    ON sd.sls_prd_key = pr.product_number                    
LEFT JOIN gold.dim_customers cu                              -- Data Lookup
    ON sd.sls_cust_id = cu.customer_id;
GO
