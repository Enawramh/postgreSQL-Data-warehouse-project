--TRUNCATE TABLE bronze.crm_cust_info;
--TRUNCATE TABLE bronze.crm_prd_info;
--TRUNCATE TABLE bronze.crm_sales_details;
--TRUNCATE TABLE bronze.erp_cat_g1v2;
--TRUNCATE TABLE bronze.erp_cust_az12;
--TRUNCATE TABLE bronze.erp_loc_a101;


COPY bronze.crm_cust_info 
FROM '/tmp/crm_cust_info.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.crm_prd_info
FROM '/tmp/crm_prd_info.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.crm_sales_details
FROM '/tmp/crm_sales_details.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.erp_cat_g1v2
FROM '/tmp/erp_px_cat_g1v2.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.erp_cust_az12
FROM '/tmp/erp_cust_az12.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.erp_loc_a101
FROM '/tmp/erp_loc_a101.csv'
DELIMITER ','
CSV HEADER;