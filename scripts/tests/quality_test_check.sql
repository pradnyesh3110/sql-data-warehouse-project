/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schema. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
--check for null duplicates in primary key
select 
cst_id,
count(*)
from bronze.crm_cust_info
group by cst_id
having cst_id is not null or count(*)>1
--check for unwanted space
SELECT '[' + cst_firstname + ']' AS cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != LTRIM(RTRIM(cst_firstname))
--data standarization in cst_gndr
select distinct cst_gndr
from silver.crm_cust_info

select * from silver.crm_cust_info

--check duplicates in primary key
select 
prd_id,
count(*)
from bronze.crm_prd_id
group by prd_id
having prd_id is  null or count(*)>1
--
select
SUBSTRING(prd_key,1,5) 
from bronze.crm_prd_id
--check for unwanted space
select '[' +prd_nm + ']' as prd_nm
from bronze.crm_prd_id
where prd_nm!=ltrim(rtrim(prd_nm))
--check for negative values in cost
select prd_cost
from bronze.crm_prd_id
where prd_cost < 0 or prd_cost is null

select 
nullif(sls_due_dt,0)sls_ship_dt
from bronze.crm_sales_detail
where sls_due_dt<=0 or len(sls_due_dt)!=8 or sls_due_dt<sls_due_dt or sls_due_dt>20500101 or sls_due_dt<19000101
select *
from bronze.crm_sales_detail
where sls_order_dt>sls_due_dt or sls_order_dt>sls_ship_dt
select distinct
sls_sales,sls_quantity,sls_price
from bronze.crm_sales_detail
where sls_sales!= sls_quantity*sls_price
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
or sls_sales is null or sls_quantity is null or sls_price is null
order by sls_sales,sls_quantity,sls_price

