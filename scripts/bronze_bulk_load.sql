CREATE OR REPLACE PROCEDURE bronze.import_bronze_data()
LANGUAGE plpgsql
AS $$
DECLARE
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  duration DOUBLE PRECISION;
BEGIN
  RAISE NOTICE '📥 Starting Bronze Layer data loading...';

  -- Block 1: Load crm_cust_info
  BEGIN
    start_time := clock_timestamp();
    RAISE NOTICE '→ Importing crm_cust_info...';

    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info 
    FROM '/tmp/crm_cust_info.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '✅ crm_cust_info loaded in % seconds', duration;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE NOTICE '❌ ERROR loading crm_cust_info: %', SQLERRM;
  END;

  -- Block 2: Load crm_prd_info
  BEGIN
    start_time := clock_timestamp();
    RAISE NOTICE '→ Importing crm_prd_info...';

    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info 
    FROM '/tmp/crm_prd_info.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '✅ crm_prd_info loaded in % seconds', duration;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE NOTICE '❌ ERROR loading crm_prd_info: %', SQLERRM;
  END;

  -- Block 3: Load crm_sales_details
  BEGIN
    start_time := clock_timestamp();
    RAISE NOTICE '→ Importing crm_sales_details...';

    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details 
    FROM '/tmp/crm_sales_details.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '✅ crm_sales_details loaded in % seconds', duration;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE NOTICE '❌ ERROR loading crm_sales_details: %', SQLERRM;
  END;

  -- Block 4: Load erp_cat_g1v2
  BEGIN
    start_time := clock_timestamp();
    RAISE NOTICE '→ Importing erp_cat_g1v2...';

    TRUNCATE TABLE bronze.erp_cat_g1v2;
    COPY bronze.erp_cat_g1v2 
    FROM '/tmp/erp_px_cat_g1v2.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '✅ erp_cat_g1v2 loaded in % seconds', duration;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE NOTICE '❌ ERROR loading erp_cat_g1v2: %', SQLERRM;
  END;

  -- Block 5: Load erp_cust_az12
  BEGIN
    start_time := clock_timestamp();
    RAISE NOTICE '→ Importing erp_cust_az12...';

    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12 
    FROM '/tmp/erp_cust_az12.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '✅ erp_cust_az12 loaded in % seconds', duration;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE NOTICE '❌ ERROR loading erp_cust_az12: %', SQLERRM;
  END;

  -- Block 6: Load erp_loc_a101
  BEGIN
    start_time := clock_timestamp();
    RAISE NOTICE '→ Importing erp_loc_a101...';

    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101 
    FROM '/tmp/erp_loc_a101.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '✅ erp_loc_a101 loaded in % seconds', duration;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE NOTICE '❌ ERROR loading erp_loc_a101: %', SQLERRM;
  END;

  RAISE NOTICE '🏁 Bronze Layer data loading complete.';
END;
$$;

-- Execute the procedure
CALL bronze.import_bronze_data();