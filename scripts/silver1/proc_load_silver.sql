# Creating a text file with the content from the second image
stored_procedure_description = """/*
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
*/"""

alter PROCEDURE silver.load_silver
AS
begin
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time Datetime,@batch_end_time datetime
	BEGIN TRY
		set @batch_start_time=getdate()
        PRINT '============================';
        PRINT 'Loading silver layer';
        PRINT '-----';
        PRINT 'Loading CRM tables';
        PRINT 'Truncating the table crm_cust_info';
		SET @start_time = GETDATE();
		print 'insering data into silver_cust_info'
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr
	)
	SELECT 
		cst_id,
		cst_key,
		LTRIM(RTRIM(cst_firstname)) AS cst_firstname,
		LTRIM(RTRIM(cst_lastname)) AS cst_lastname,
		CASE 
			WHEN UPPER(LTRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(LTRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'N/A'
		END AS cst_marital_status,
		CASE 
			WHEN UPPER(LTRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(LTRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'N/A'
		END AS cst_gndr
	FROM (
		SELECT 
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL 
	) t 
	WHERE flag_last = 1;
		SET @end_time = GETDATE();
        PRINT '>>Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	truncate table silver.crm_prd_id
	PRINT 'Truncating the table crm_prd_id';
		set @start_time=getdate()
       	PRINT '============================';
        PRINT 'Loading silver layer';
        PRINT '-----';
        PRINT 'Loading CRM tables';
       	print 'insering data into silver_prd_id'
	INSERT INTO silver.crm_prd_id (
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
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE 
			WHEN UPPER(LTRIM(RTRIM(prd_line))) = 'M' THEN 'Mountain'
			WHEN UPPER(LTRIM(RTRIM(prd_line))) = 'R' THEN 'Road'
			WHEN UPPER(LTRIM(RTRIM(prd_line))) = 'S' THEN 'other_sales'
			ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(
			DATEADD(
				DAY, 
				-1, 
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
			) AS DATE
		) AS prd_end_dt
	FROM [DataWarehouse].[bronze].[crm_prd_id];
		SET @end_time = GETDATE();
        PRINT '>>Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	truncate table silver.crm_sales_detail
	PRINT 'Truncating the table crm_sales_details';
	set @start_time=getdate()
        PRINT '============================';
   		print 'insering data into silver_crm-sales_details'
	INSERT INTO silver.crm_sales_detail(
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
			WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE 
			WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE 
			WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
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
	FROM bronze.crm_sales_detail;
		SET @end_time = GETDATE();
        PRINT '>>Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	truncate table silver.erp_CUST_AZ12
	PRINT 'Truncating the table erp_cust_az12';
	set @start_time=getdate()
     
		
	print'inserting the silver.erp_CUST_AZ12 '
	insert into silver.erp_CUST_AZ12(
	cid,
	bdate,
	gen)
	select
	case when cid like 'NAS%'THEN SUBSTRING(CID,4,len(cid))
		else cid
	end as cid,

	case when bdate >getdate() then null
		else bdate
	end as bdate,
	(SELECT DISTINCT
    
		CASE 
			WHEN UPPER(LTRIM(RTRIM(gen))) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(LTRIM(RTRIM(gen))) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen)
	gen
	FROM bronze.erp_CUST_AZ12;
		SET @end_time = GETDATE();
        PRINT '>>Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

 
	truncate table silver.erp_LOC_A101
	set @start_time=getdate()
	print 'inserting into silver.erp_LOC_A101'
	insert into silver.erp_LOC_A101(cid,CNTRY)
	select 
	replace(cid,'-','')cid,
	case when ltrim(rtrim(cntry))='DE' then 'Germany'
		 when ltrim(rtrim(cntry)) IN ('US','USA') then 'United states'
		 when ltrim(rtrim(cntry)) = '' or cntry is null then 'N/A'
		 else LTRIM(rtrim(cntry))
	end as cntry

	from bronze.erp_LOC_A101
		SET @end_time = GETDATE();
        PRINT '>>Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


	truncate table silver.erp_PX_CAT_G1V2
	set @start_time=getdate()
	print 'inserting into silver.erp_PX_CAT_G1V2'
	insert into silver.erp_PX_CAT_G1V2(ID,CAT,SUBCAT,MAINTENANCE)
	select 
	ID,
	CAT,
	SUBCAT,
	MAINTENANCE
	from bronze.erp_PX_CAT_G1V2
		SET @end_time = GETDATE();
        PRINT '>>Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	end try
	 BEGIN CATCH
        PRINT '❌ Error occurred: ' + ERROR_MESSAGE();
    END CATCH

	
end
