/*=====================================================================
  TASK     : CRM & ERP Data Quality Check and Cleaning
  Author      : Omar
  Date        : 6-6-2025
  Description : 
    This SQL script performs data profiling, cleansing, and transformation 
    tasks on CRM and ERP tables in the bronze layer to prepare clean and 
    consistent data for further processing. The main operations include:

    - Ensuring uniqueness and handling nulls in key identifiers
    - Removing unwanted spaces and standardizing categorical values
    - Handling invalid or missing numerical and date values
    - Normalizing and transforming fields for join compatibility
    - Recalculating derived fields like sales and price if inconsistencies exist

  Sections:
    1. CRM Tables: crm_cust_info, crm_prd_info, crm_sales_details
    2. ERP Tables: erp_cust_az12

=====================================================================*/


/* ---------------------------------------------------------------
----------------------       CRM TABLES       --------------------
-----------------------------------------------------------------*/

/* ----------------- Table: crm_cust_info ------------------ */

/* 1. Check if cst_id is unique and not null */
SELECT COUNT(*) AS count_id, cst_id
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

/* Identify duplicates */
SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

/* Keep only latest record per cst_id */
WITH ranked_custs AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
)
SELECT *
FROM ranked_custs
WHERE rn = 1;

/* 2. Trim unwanted spaces */
-- cst_key is clean
SELECT cst_firstname FROM bronze.crm_cust_info WHERE TRIM(cst_firstname) != cst_firstname;
SELECT TRIM(cst_firstname), TRIM(cst_lastname) FROM bronze.crm_cust_info;

/* 3. Normalize categorical values */
SELECT DISTINCT cst_marital_status FROM bronze.crm_cust_info;
SELECT 
    cst_marital_status,
    CASE UPPER(TRIM(cst_marital_status))
        WHEN 'S' THEN 'Single'
        WHEN 'M' THEN 'Married'
        ELSE 'n/a'
    END AS standardized_marital_status
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info;
SELECT 
    cst_gndr,
    CASE UPPER(TRIM(cst_gndr))
        WHEN 'M' THEN 'Male'
        WHEN 'F' THEN 'Female'
        ELSE 'n/a'
    END AS standardized_gender
FROM bronze.crm_cust_info;

/* Final cleaned crm_cust_info */
WITH cleaned AS (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE UPPER(TRIM(cst_marital_status))
        WHEN 'S' THEN 'Single'
        WHEN 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE UPPER(TRIM(cst_gndr))
        WHEN 'M' THEN 'Male'
        WHEN 'F' THEN 'Female'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM cleaned
WHERE rn = 1;


/* ----------------- Table: crm_prd_info ------------------ */

/* 1. Check prd_id is unique and not null */
SELECT COUNT(*) AS count_id, prd_id
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

/* 2. Parse prd_key */
SELECT 
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS join_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS join_key
FROM bronze.crm_prd_info;

/* Check joins */
SELECT * 
FROM bronze.crm_prd_info A
JOIN bronze.erp_px_cat_g1v2 B ON REPLACE(SUBSTRING(A.prd_key, 1, 5), '-', '_') = B.id;

SELECT * 
FROM bronze.crm_prd_info A
JOIN bronze.crm_sales_details B ON SUBSTRING(A.prd_key, 7, LEN(A.prd_key)) = B.sls_prd_key;

/* 3. Clean prd_nm */
SELECT prd_nm FROM bronze.crm_prd_info WHERE TRIM(prd_nm) != prd_nm;

/* 4. Fix prd_cost */
SELECT ISNULL(prd_cost, 0) AS fixed_cost
FROM bronze.crm_prd_info
WHERE prd_cost <= 0 OR prd_cost IS NULL;

/* 5. Normalize prd_line */
SELECT DISTINCT prd_line FROM bronze.crm_prd_info;
SELECT 
    *,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS normalized_line
FROM bronze.crm_prd_info;

/* 6. Fix invalid date ranges */
SELECT  
    prd_key, 
    prd_start_dt,
    prd_end_dt AS old_end_dt,
    DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS new_end_dt
FROM bronze.crm_prd_info
WHERE prd_key = 'AC-HE-HL-U509-R';

/* Final cleaned crm_prd_info */
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    TRIM(prd_nm) AS prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    prd_start_dt,
    DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM bronze.crm_prd_info;


/* ----------------- Table: crm_sales_details ------------------ */

SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE 
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE 
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;

------------------------------------------------------------------
----------------------   ERP TABLES   ----------------------------
------------------------------------------------------------------

-- =========================
-- Table: erp_cust_az12
-- =========================
-- View all records
SELECT * FROM bronze.erp_cust_az12;

-- 1. Column: cid
-- Validate if `cid` is unique and not null
SELECT COUNT(*) AS count_id, cid
FROM bronze.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1 OR cid IS NULL;

-- Check for `cid` values starting with 'NAS'
SELECT cid
FROM bronze.erp_cust_az12
WHERE cid LIKE 'NAS%';

-- Clean `cid` by removing 'NAS' prefix
SELECT 
	CASE 
		WHEN TRIM(cid) LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid
FROM bronze.erp_cust_az12;

-- 2. Column: bdate
-- Identify any future birthdates
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE();

-- Replace future dates with NULL
SELECT 
	CASE 
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate
FROM bronze.erp_cust_az12;

-- 3. Column: gen
-- Check for gender value inconsistencies
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

-- Normalize gender values
SELECT 
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;

-- Final cleaned output from erp_cust_az12
SELECT
	CASE 
		WHEN TRIM(cid) LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid,
	CASE 
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;

-- View original data
SELECT * FROM bronze.erp_cust_az12;


-- =========================
-- Table: erp_loc_a101
-- =========================
-- View all records
SELECT * FROM bronze.erp_loc_a101;
SELECT * FROM bronze.crm_cust_info;

-- 1. Column: cid
-- Remove hyphens from `cid` to match `cst_key` in crm_cust_info
SELECT REPLACE(cid, '-', '') AS cid
FROM bronze.erp_loc_a101;

-- 2. Column: cntry
-- Normalize country values
SELECT 
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;

-- Final cleaned output from erp_loc_a101
SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;

-- View original data
SELECT * FROM bronze.erp_loc_a101;


-- =========================
-- Table: erp_px_cat_g1v2
-- =========================
-- View all records
SELECT * FROM bronze.erp_px_cat_g1v2;

-- No data issues identified; select required columns
SELECT
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2;



/*
---------------------------------------------------------------
---------------           CRM TABLES         ------------------
---------------------------------------------------------------
-- Table crm_cust_info
SELECT * 
FROM bronze.crm_cust_info

-- 1.
--- Check on id is unique and not null
SELECT COUNT(*) AS count_id, cst_id
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

---- There are ids must be corrected 
SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466;
------ There are 3 created from that id but every one has date 
------ so we will get the only what has last created date
------ That can be done Using Ranking Function

SELECT * 
FROM (
SELECT * , ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS num
FROM bronze.crm_cust_info) AS t
WHERE num = 1 AND cst_id IS NOT NULL;

----- Now This query will get only ids that unique and not null ids
SELECT * 
FROM (
SELECT * , ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS num
FROM bronze.crm_cust_info) AS t
WHERE num = 1 AND cst_id IS NOT NULL;


-- 2. check on unwanted spaces
SELECT cst_key 
FROM bronze.crm_cust_info
WHERE TRIM(cst_key) != cst_key;
----- cst_key is good

SELECT cst_firstname 
FROM bronze.crm_cust_info
WHERE TRIM(cst_firstname) != cst_firstname;
----- cst_firstname needs to be solved for unwanted spaces

SELECT TRIM(cst_firstname)
FROM bronze.crm_cust_info

SELECT TRIM(cst_lastname)
FROM bronze.crm_cust_info
---- This solve unwanted spaces for firstname and lastname

-- 3. check on consistency of data 
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info

---- Solve low cardinality of cst-marital col
SELECT cst_marital_status AS cst_marital_status_old,
		CASE UPPER(TRIM(cst_marital_status))
			WHEN 'S' THEN 'Single'
			WHEN 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_marital_status_new
FROM bronze.crm_cust_info


--- Also in gender
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

---- Solve low cardinality of it
SELECT cst_gndr AS cst_gndr_old,
		CASE UPPER(TRIM(cst_gndr))
			WHEN 'M' THEN 'Male'
			WHEN 'F' THEN 'Female'
			ELSE 'n/a'
		END AS cst_gndr_new
FROM bronze.crm_cust_info

---- The Whole Query of that table with solved of all
SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname),
	TRIM(cst_lastname),
	CASE UPPER(TRIM(cst_marital_status))
			WHEN 'S' THEN 'Single'
			WHEN 'M' THEN 'Married'
			ELSE 'n/a'
	END AS cst_marital_status,
	CASE UPPER(TRIM(cst_gndr))
			WHEN 'M' THEN 'Male'
			WHEN 'F' THEN 'Female'
			ELSE 'n/a'
	END AS cst_gndr_new,
	cst_create_date
FROM (
		SELECT 
			* , 
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS num
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL) AS t
WHERE num = 1;





SELECT * FROM bronze.crm_cust_info


-- Table crm_prd_info

-- 1.
--- Check on id is unique and not null
SELECT COUNT(*) AS count_id, prd_id
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

--2. prd_key will be joined with another table in ERP by prd_key 
----- the first 4 values of it get the prd_id and the last get the prd_key that will be joined with sales by it  
SELECT 
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
	SUBSTRING(prd_key, 7, LEN(prd_key))
FROM bronze.crm_prd_info

-- check on id with other table 
SELECT * 
FROM bronze.crm_prd_info AS A,
	bronze.erp_px_cat_g1v2 AS B
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') = B.id

SELECT * 
FROM bronze.crm_prd_info AS A,
	bronze.crm_sales_details AS B
WHERE SUBSTRING(A.prd_key, 7, LEN(A.prd_key)) = B.sls_prd_key

--- 3. check on prd_num
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE TRIM(prd_nm) != prd_nm

--- 4. CHECK on prd_cost if there was null or negative and 0
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost <= 0 OR prd_cost IS NULL;

---- solve the null value
SELECT ISNULL(prd_cost, 0)
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

--- 5. cheeck on cardinality 
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Solve cardinality by normalization
SELECT 
	*, 
	CASE 
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line
FROM bronze.crm_prd_info;

--- 6. check on dates if there end date less than start_date
SELECT  prd_key, 
		prd_start_dt,
		prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_key = 'AC-HE-HL-U509-R';

---- There was a problem which is end date less than start 
----- Also, start date of second history not equal end date of previus - 1
SELECT  
    prd_key, 
    prd_start_dt,
    prd_end_dt AS prd_end_dt_old,
    DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_date_new
FROM bronze.crm_prd_info
WHERE prd_key = 'AC-HE-HL-U509-R';


--- Now the Whole Query that solved all problems of that table
SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	prd_start_dt,
	DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_date
FROM bronze.crm_prd_info;

SELECT * FROM bronze.crm_prd_info


-- Table crm_sales_details

SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
	sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price  -- Derive price if original value is invalid
	END AS sls_price
FROM bronze.crm_sales_details;

SELECT * FROM bronze.crm_sales_details

---------------------------------------------------------------
---------------           ERP TABLES         ------------------
---------------------------------------------------------------

-- Table erp_cust_az12
SELECT * FROM bronze.erp_cust_az12;


--- 1. cid col
----- check on if cid is unique and not null
SELECT COUNT(*) AS count_id, cid
FROM bronze.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1 OR cid IS NULL;

----- cid has a substring at the first which is 'NAS'
------ and that must be removed to be joined with crm_cust_info table
SELECT cid
FROM bronze.erp_cust_az12
WHERE cid LIKE 'NAS%';

---- solution
SELECT 
	CASE WHEN TRIM(cid) LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid
FROM bronze.erp_cust_az12;


--- 2. bdate col
---- check the boundries of the date [if there was a date in the future]
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE();

---- solution then the date should be NULL
SELECT 
	CASE 
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate
FROM bronze.erp_cust_az12;

--- 3. gen col
---- check on consistency 
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;


--- solution 
SELECT 
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;


---- Now The Whole Query 
SELECT
	CASE WHEN TRIM(cid) LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid,
	CASE 
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;

SELECT * FROM bronze.erp_cust_az12;


-- Table erp_loc_a101

SELECT * FROM bronze.erp_loc_a101;
SELECT * FROM bronze.crm_cust_info;

--- 1. col cid
---- The cid need to be remove '-' in it to be like cst_key in crm_cust_info
----- to make join betweeen tables
SELECT REPLACE(cid, '-', '') AS cid
FROM bronze.erp_loc_a101;


--- 2. Cntry col
----- The col needs to be normalized
SELECT 
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;


--- Now the whole code 
SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;

SELECT * FROM bronze.erp_loc_a101;


-- Table erp_px_cat
SELECT * FROM bronze.erp_px_cat_g1v2;

--- The table has no any problem 
SELECT
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2;*/


