-- Data_Warehouse.bronze.crm_custinfo & crm_prd_info
select *
from (SELECT *,
row_number() over (partition by cst_id order by cst_create_date desc) as flag
FROM bronze.crm_cust_info) as t 
where flag = 1

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT cst_marital_status
FROM bronze.crm_cust_info
WHERE  cst_marital_status != TRIM(cst_marital_status)

-- Check distinct
-- Expectation: No Results
SELECT *
FROM bronze.crm_cust_info


SELECT 
    cst_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


SELECT TOP(200)*
  FROM Data_Warehouse.bronze.crm_sales_details

SELECT DISTINCT id 
FROM Data_Warehouse.bronze.erp_px_cat_g1v2

SELECT  sls_prd_key
FROM Data_Warehouse.bronze.crm_sales_details
-------------
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
FROM Data_Warehouse.bronze.crm_prd_info
------------------------
WHERE SUBSTRING(prd_key,7,LEN(prd_key)) NOT IN 
	(SELECT sls_prd_key
FROM Data_Warehouse.bronze.crm_sales_details)
--------------
SELECT 
	prd_id,
	count(*)
FROM Data_Warehouse.bronze.crm_prd_info
GROUP BY prd_id
HAVING count(*)>1 or prd_id is null

SELECT MAX(len(prd_key)) AS max_prd_key_length
FROM Data_Warehouse.bronze.crm_prd_info;

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE TRIM(prd_nm) != prd_nm

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 or prd_cost is null


-- Data_Warehouse.bronze.crm_sales_details
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

SELECT sls_ord_num
      ,sls_prd_key
      ,sls_cust_id
      ,sls_order_dt
      ,sls_ship_dt
      ,sls_due_dt
      ,sls_sales
      ,sls_quantity
      ,sls_price
FROM Data_Warehouse.bronze.crm_sales_details
WHERE sls_prd_key NOT IN ( SELECT prd_key FROM Data_Warehouse.silver.crm_prd_info);

SELECT sls_ord_num
      ,sls_prd_key
      ,sls_cust_id
      ,sls_order_dt
      ,sls_ship_dt
      ,sls_due_dt
      ,sls_sales
      ,sls_quantity
      ,sls_price
FROM Data_Warehouse.bronze.crm_sales_details
WHERE sls_cust_id NOT IN ( SELECT cst_id FROM Data_Warehouse.silver.crm_cust_info);

SELECT NULLIF(sls_order_dt,0) sls_order_dt 
      
FROM Data_Warehouse.bronze.crm_sales_details
WHERE sls_order_dt <=0 or LEN(sls_order_dt) < 8 or sls_order_dt > 20500000 or sls_order_dt < 19000000;

--Argument data type int is invalid for argument 1 of Trim function.
SELECT sls_cust_id
FROM Data_Warehouse.bronze.crm_sales_details
WHERE sls_cust_id != TRIM(sls_cust_id)
-------

SELECT distinct sls_sales,sls_quantity,sls_price
FROM Data_Warehouse.silver.crm_sales_details
WHERE sls_sales != (sls_quantity * sls_price) or
	  sls_price is null or sls_quantity is null or sls_sales is null or
	  sls_price<0  or sls_quantity<0 or sls_sales <0
ORDER BY sls_sales,sls_quantity,sls_price;

---------------------------------------
SELECT sls_sales  sls_sales_old,
       
       sls_price sls_price_old,
	   CASE WHEN sls_sales is null or sls_sales<0 or sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
	   END sls_sales,
	   sls_quantity,
	   CASE WHEN sls_price<0 or sls_price is null
			THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
	   END sls_price
FROM Data_Warehouse.bronze.crm_sales_details



CREATE TABLE silver.crm_sales_details (
	sls_ord_num		NVARCHAR(50),
	sls_prd_key		NVARCHAR(50),
	sls_cust_id		INT,
	sls_order_dt	DATE,
	sls_ship_dt		DATE,
	sls_due_dt		DATE,
	sls_sales		INT,
	sls_quantity	INT,
	sls_price		INT,
	dwh_crate_date		DATETIME2 DEFAULT GETDATE()
);

-- Data_Warehouse.bronze.erp_cust_az12
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
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
	   END not in (select DISTINCT cst_key from silver.crm_cust_info)

SELECT *
FROM silver.crm_cust_info;

SELECT DISTINCT 
    bdate 
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();


SELECT DISTINCT gen, 
    CASE WHEN UPPER(TRIM(gen)) in ('F','Female') THEN 'Female'
			WHEN UPPER(TRIM(gen)) in ('M','Male') THEN 'Male'
			ELSE 'n/a'
	   END gen
FROM bronze.erp_cust_az12

-- Data_Warehouse.bronze.erp_loc_a101
SELECT 
	   REPLACE (cid ,'-','') cid,
       CASE WHEN UPPER(TRIM(cntry)) in ('United States','US','USA') THEN 'United States'
			WHEN UPPER(TRIM(cntry)) in ('DE','Germany') THEN 'Germany'
			WHEN cntry = '' or cntry IS NULL THEN 'n/a'
			ELSE cntry
	   END cntry
FROM Data_Warehouse.bronze.erp_loc_a101
WHERE REPLACE (cid ,'-','') not in (SELECT cst_key FROM silver.crm_cust_info) ;

SELECT distinct CASE WHEN UPPER(TRIM(cntry)) in ('United States','US','USA') THEN 'United States'
			WHEN UPPER(TRIM(cntry)) in ('DE','Germany') THEN 'Germany'
			WHEN cntry = '' or cntry IS NULL THEN 'n/a'
			ELSE cntry
	   END cntry
FROM Data_Warehouse.bronze.erp_loc_a101;