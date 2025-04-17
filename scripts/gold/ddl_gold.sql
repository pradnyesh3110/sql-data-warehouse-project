/*
DDL Script: Create Gold Views

Script Purpose: This script is designed to create views for the Gold layer in the data warehouse. The Gold layer represents the final dimension and fact tables (Star Schema).

Overview: Each view applies transformations and integrates data from the Silver layer to produce a clean, enriched, and business-ready dataset.

Usage: These views are intended for direct querying to support analytics and reporting.
*/
alter view gold.dim_customer as
SELECT  
		ROW_NUMBER() over (order by cst_id)as customer_key,
		ci.cst_id as customer_id,
		ci.cst_key AS cst_number,
		ci.cst_firstname AS firstname,
		ci.cst_lastname AS lastname,
		cl.CNTRY AS country,
		case when ci.cst_gndr!='n/a' then ci.cst_gndr
			 else coalesce(ca.gen,'n/a')
		end as gender,
		ci.cst_marital_status AS marital_status,
		ca.BDATE AS birthday,
		ci.cst_create_date AS create_date
		
		
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_CUST_AZ12 ca
		ON LTRIM(RTRIM(ci.cst_key)) = LTRIM(RTRIM(ca.CID))
	LEFT JOIN silver.erp_LOC_A101 cl
		ON LTRIM(RTRIM(ci.cst_key)) = LTRIM(RTRIM(cl.CID))
	

alter view gold.dim_products as
 select 
	ROW_NUMBER() over(order by pn.prd_start_dt,pn.prd_key) as proudct_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.CAT as category,
	pc.SUBCAT as subcategory,
	pc.MAINTENANCE as maintenance,
	pn.prd_cost as product_cost,
	pn.prd_line as product_line,
	
	pn.prd_start_dt as start_date
from silver.crm_prd_id pn
left join silver.erp_PX_CAT_G1V2 pc
on pn.cat_id=pc.ID
where prd_end_dt is null 

alter view gold.fact_sales as
select
sd.sls_ord_num as order_number,
pr. product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_detail sd
left join  gold.dim_products pr
on sd.sls_prd_key=pr.product_number
left join gold.dim_customer cu
on sd.sls_cust_id=cu.customer_id
