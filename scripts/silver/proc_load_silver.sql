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
