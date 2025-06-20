/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date)

		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) cst_firstname,
		TRIM(cst_lastname) cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 ELSE 'n/a' 
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'n/a' 
		END cst_gndr,
		cst_create_date
		FROM (SELECT *,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
					FROM bronze.crm_cust_info
					WHERE cst_id IS NOT NULL
				) t
				WHERE flag_last = 1;
		SET @end_time = GETDATE();
		PRINT '>> crm_cust_info load duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			   prd_id,
			   cat_id,
			   prd_key,
			   prd_nm,
			   prd_cost,
			   prd_line,
			   prd_start_dt,
			   prd_end_dt)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') cat_id,
			SUBSTRING(prd_key,7,LEN(prd_key)) prd_key,
			prd_nm,
			ISNULL(prd_cost,0) prd_cost,
			CASE UPPER(TRIM(prd_line)) 
				 WHEN  'M' THEN 'Mountain'
				 WHEN  'R' THEN 'Road'
				 WHEN  'T' THEN 'Tourong'
				 WHEN  'S' THEN 'Other Sales'
				 ELSE 'n/a'
			END prd_line,
			CAST(prd_start_dt as DATE) as prd_start_dt,
			DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) as prd_end_dt
			/*CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) AS prd_end_dt */
		FROM Data_Warehouse.bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> crm_prd_info load duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			   sls_ord_num,
			   sls_prd_key,
			   sls_cust_id,
			   sls_order_dt,
			   sls_ship_dt,
			   sls_due_dt,
			   sls_sales,
			   sls_quantity,
			   sls_price)
		SELECT sls_ord_num
			  ,sls_prd_key
			  ,sls_cust_id
			  ,CASE WHEN sls_order_dt <=0 or LEN(sls_order_dt) < 8 THEN NULL
					ELSE CAST(CAST(sls_order_dt as VARCHAR) as DATE)
			   END sls_order_dt
			  ,CASE WHEN sls_ship_dt <=0 or LEN(sls_ship_dt) < 8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt as VARCHAR) as DATE)
			   END sls_ship_dt
			  ,CASE WHEN sls_due_dt <=0 or LEN(sls_due_dt) < 8 THEN NULL
					ELSE CAST(CAST(sls_due_dt as VARCHAR) as DATE)
			   END sls_due_dt
			  ,CASE WHEN sls_sales is null or sls_sales<0 or sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
			   END sls_sales
			  ,sls_quantity
			  ,CASE WHEN sls_price<0 or sls_price is null
					THEN sls_sales / NULLIF(sls_quantity,0)
					ELSE sls_price
			   END sls_price
		FROM Data_Warehouse.bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> crm_sales_details load duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
			   cid,
			   bdate,
			   gen)
		SELECT 
			   CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
					ELSE cid
			   END as cid,
			   CASE WHEN bdate > GETDATE() THEN NULL
					ELSE bdate
			   END as bdate,
			   CASE WHEN UPPER(TRIM(gen)) in ('F','Female') THEN 'Female'
					WHEN UPPER(TRIM(gen)) in ('M','Male') THEN 'Male'
					ELSE 'n/a'
			   END gen
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> erp_cust_az12 load duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
			   cid,
			   cntry)
		SELECT 
			   REPLACE (cid ,'-','') cid,
			   CASE WHEN UPPER(TRIM(cntry)) in ('United States','US','USA') THEN 'United States'
					WHEN UPPER(TRIM(cntry)) in ('DE','Germany') THEN 'Germany'
					WHEN cntry = '' or cntry IS NULL THEN 'n/a'
					ELSE cntry
			   END cntry
		FROM Data_Warehouse.bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> erp_loc_a101 load duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
			   id,
			   cat,
			   subcat,
			   maintenance)
		SELECT id,
			   cat,
			   subcat,
			   maintenance
		FROM Data_Warehouse.bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> erp_px_cat_g1v2 load duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------';
		SET @batch_end_time = GETDATE();
		PRINT 'Loading Silver Layer Completed';
		PRINT '>> Total load duration:' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------';

	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END