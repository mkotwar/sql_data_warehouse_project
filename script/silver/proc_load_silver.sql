/*
===============================================================================
 Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
 Purpose:
    Perform ETL (Extract, Transform, Load) operations to populate 'silver' 
    schema tables from the 'bronze' schema.

 Actions:
    - Truncate existing Silver tables
    - Insert cleaned and transformed data from Bronze

 Parameters:
    None

 Usage:
    EXEC silver.load_silver;
===============================================================================
*/

-- Execute Procedure
EXEC silver.load_silver;

-- Create or Alter the Procedure
CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        
        PRINT '=============================================';
        PRINT 'STARTING: LOADING THE SILVER LAYER';
        PRINT '=============================================';

        ---------------------------------------------
        -- Load CRM Customer Information
        ---------------------------------------------
        PRINT '---------------------------------------------';
        PRINT 'LOADING CRM: crm_cust_info';
        PRINT '---------------------------------------------';

        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT 'INSERTING INTO: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
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
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
                ELSE 'N/A'
            END,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
                ELSE 'N/A'
            END,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_name
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_name = 1;

        SET @end_time = GETDATE();
        PRINT 'DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        ---------------------------------------------
        -- Load CRM Product Information
        ---------------------------------------------
        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT 'INSERTING INTO: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            SUBSTRING(prd_key, 7, LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost, 0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'MOUNTAIN'
                WHEN 'R' THEN 'ROAD'
                WHEN 'S' THEN 'OTHER SALES'
                WHEN 'T' THEN 'TOURING'
                ELSE 'N/A'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT 'DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        ---------------------------------------------
        -- Load CRM Sales Details
        ---------------------------------------------
        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT 'INSERTING INTO: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sale,
            sls_quantity,
            sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL 
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END,
            CASE WHEN sls_sale IS NULL OR sls_sale <= 0 OR sls_sale != sls_quantity * ABS(sls_price)
                 THEN sls_quantity * ABS(sls_price)
                 ELSE sls_sale
            END,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price <= 0
                 THEN sls_sale / NULLIF(sls_quantity, 0)
                 ELSE sls_price
            END
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT 'DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        ---------------------------------------------
        -- Load ERP Customer
        ---------------------------------------------
        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT 'INSERTING INTO: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT 
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT 'DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        ---------------------------------------------
        -- Load ERP Location
        ---------------------------------------------
        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT 'INSERTING INTO: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT 
            REPLACE(cid, '-', ''),
            CASE 
                WHEN TRIM(cntry) = 'DE
