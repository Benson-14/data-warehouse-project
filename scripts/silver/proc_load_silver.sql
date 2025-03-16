SELECT * FROM bronze.crm_cust_info
SELECT * FROM bronze.crm_prd_info
SELECT * FROM bronze.crm_sales_details
SELECT * FROM silver.crm_cust_info

--------------------------------- crm_cust_info -------------------------------
	
-- Removing NULL Values 
SELECT
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Check for unwanted spaces 
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


-- Data Standardization & Consistency 
SELECT 
	DISTINCT cst_gndr
FROM bronze.crm_cust_info

-- Loading Data
INSERT INTO silver.crm_cust_info (
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
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname, 
	CASE
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'N/A'
		END AS cst_marital_status,
	CASE
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'N/A'
		END AS cst_gndr,
	cst_create_date
FROM (
SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
) 
WHERE flag_last = 1


--------------------------------- crm_prd_info -------------------------------
	
-- Removing NULL Values 
SELECT
	prd_id,
	COUNT(*)
FROM bronze.prd_cust_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted spaces
SELECT prd_nm FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for negative numbers or NULLs 
SELECT prd_cost FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency 
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- Check for invalid DATE Orders 
SELECT * FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt



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
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract Category ID
	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key, -- Extract Product ID
	prd_nm,
	COALESCE(prd_cost, 0) AS prod_cost,
	CASE 
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line, --Descriptive values
	CAST(prd_start_dt AS DATE),
	CAST(
		LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' AS DATE
	) AS prd_end_dt -- Calc end date as one day before the next start date
FROM bronze.crm_prd_info


--------------------------------- crm_sales_details -------------------------------
	
-- Removing NULL Values 
SELECT
	prd_id,
	COUNT(*)
FROM bronze.prd_cust_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted spaces
SELECT prd_nm FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for negative numbers or NULLs 
SELECT prd_cost FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency 
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- Check for invalid DATE Orders 
SELECT sls_order_dt FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8

SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check Data Consistency: Between Sales, Quantity and Price
-- SALES = QUANTITY * PRICE 
-- Values must not be NULL, zero or Negative 
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
GROUP BY sls_sales, sls_quantity, sls_price

SELECT
	sls_ord_num,
	sls_sales AS old_sls_sales,
	sls_quantity,
	sls_price AS old_sls_price,
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
    sls_quantity,
    CASE 
    	WHEN sls_price IS NULL OR sls_price <= 0 
        	THEN sls_sales / NULLIF(sls_quantity, 0)  -- Avoid division by zero
    	ELSE sls_price  
	END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
GROUP BY sls_sales, sls_quantity, sls_price

INSERT INTO silver.crm_sales_details (
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
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
		WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
		ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
	END AS sls_order_dt,
    CASE 
		WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
		ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
	END AS sls_ship_dt,
    CASE 
		WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
		ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
	END AS sls_due_dt,
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
    sls_quantity,
    CASE 
    	WHEN sls_price IS NULL OR sls_price <= 0 
        	THEN sls_sales / NULLIF(sls_quantity, 0)  -- Avoid division by zero
    	ELSE sls_price  
	END AS sls_price
FROM bronze.crm_sales_details
