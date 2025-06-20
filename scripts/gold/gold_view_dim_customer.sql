/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the Data_warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    -- Generates a unique surrogate key for each customer, ordered by customer ID.
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id                              AS customer_id,
    ci.cst_key                             AS customer_number,
    ci.cst_firstname                       AS firstname,
    ci.cst_lastname                        AS lastname,
    loc.cntry                              AS country,
    ci.cst_marital_status                  AS marital_status,
    -- Derives gender, preferring CRM data, falling back to ERP if CRM is 'n/a'.
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END                                    AS gender,
    ca.bdate                               AS birth_date,
    ci.cst_create_date                     AS create_date
FROM
    Data_Warehouse.silver.crm_cust_info AS ci
LEFT JOIN
    Data_Warehouse.silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN
    Data_Warehouse.silver.erp_loc_a101 AS loc
    ON ci.cst_key = loc.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    -- Generates a unique surrogate key for each product, ensuring uniqueness
    -- even for products with the same number but different start dates.
    ROW_NUMBER() OVER (ORDER BY start_date, product_number) AS product_key,
    t.* -- Selects all columns from the subquery 't'
FROM
    (
        SELECT DISTINCT
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
        FROM
            silver.crm_prd_info AS pn
        LEFT JOIN
            silver.erp_px_cat_g1v2 AS pc
            ON pn.cat_id = pc.id
        WHERE
            pn.prd_end_dt IS NULL
    ) AS t;

GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key,
    cs.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM
    silver.crm_sales_details AS sd
LEFT JOIN
    gold.dim_products AS pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN
    gold.dim_customers AS cs
    ON sd.sls_cust_id = cs.customer_id;
