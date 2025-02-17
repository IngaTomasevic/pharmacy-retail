--_____________________________ MASTER BL_CL LOAD PROCEDURE____________________________
/* Loads all data into bl_cl layers (11 map tables + 1 look up table) */


CREATE OR REPLACE PROCEDURE bl_cl.prc_master_load_blcl()
LANGUAGE plpgsql
AS $$
DECLARE 
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	run_time_sec NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN
	BEGIN
	-- put it into subblock do not loose actions if actions above will fail
	time_start := CLOCK_TIMESTAMP();
	END;
	
	CALL bl_cl.prc_load_lkp_cities();
	CALL bl_cl.prc_load_map_states();
	CALL bl_cl.prc_load_map_cities_by_states();
	CALL bl_cl.prc_load_map_addresses();
	CALL bl_cl.prc_load_map_brands();
	CALL bl_cl.prc_load_map_prod_categories();
	CALL bl_cl.prc_load_map_prod_subcategories();
	CALL bl_cl.prc_load_map_products();
	CALL bl_cl.prc_load_map_promo_categories();
	CALL bl_cl.prc_load_map_promo_chanels();
	CALL bl_cl.prc_load_map_promotions();
	CALL bl_cl.prc_load_map_suppliers();

	/* All loading procedures above are treated as a single block. 
	 * Either all procedures are completed, or no at all. 
	 * Master procedure will be added to log table only in case
	 * of an error inside current procedure. If no errors - 
	 * only loading procedures go into log table with their details. */
	EXCEPTION WHEN OTHERS THEN
		time_end := CLOCK_TIMESTAMP();
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
		run_time_sec := EXTRACT (EPOCH FROM (time_end - time_start));
	
		CALL bl_cl.prc_load_logs(
			'prc_master_load_blcl',
			'bl_cl',
			time_start,
			time_end,
			run_time_sec,
			0,
			0,
			er_flag,
			er_code,
			er_msg
			);
		RAISE NOTICE 'ERROR in master loading BLCL. %: %',  SQLSTATE, SQLERRM;
	COMMIT;
END; $$;

--TRUNCATE bl_cl.logs;
--CALL bl_cl.prc_master_load_blcl();
--SELECT * FROM bl_cl.logs;

--SELECT * from bl_cl.fn_log_stat();

COMMIT;
