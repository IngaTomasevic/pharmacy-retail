--____________________________________________________ MASTER BL_3NF LOAD PROCEDURE_____________________________________
/* Calls all bl_3nf dimension loading procedures */

CREATE OR REPLACE PROCEDURE bl_cl.prc_master_load_bl3nf()
LANGUAGE plpgsql
AS $$
DECLARE 
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	time_run NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);	
	er_msg VARCHAR(300);
BEGIN
	-- put this in subblock do not loose if actions above will fail
	-- because this is used for logging in case of an error of the current prc
	BEGIN
	time_start := CLOCK_TIMESTAMP();
	END; 

	CALL bl_cl.prc_load_ce_employees();
	CALL bl_cl.prc_load_ce_states();
	CALL bl_cl.prc_load_ce_cities();
	CALL bl_cl.prc_load_ce_addresses();
	CALL bl_cl.prc_load_ce_brands();
	CALL bl_cl.prc_load_ce_customers();
	CALL bl_cl.prc_load_ce_payment_methods();
	CALL bl_cl.prc_load_ce_prod_categories();
	CALL bl_cl.prc_load_ce_prod_subcategories();
	CALL bl_cl.prc_load_ce_products_scd();
	CALL bl_cl.prc_load_ce_promo_categories();
	CALL bl_cl.prc_load_ce_promo_channels();
	CALL bl_cl.prc_load_ce_promotions();
	CALL bl_cl.prc_load_ce_sales_channels();
	CALL bl_cl.prc_load_ce_stores();
	CALL bl_cl.prc_load_ce_suppliers();

	/* All loading procedures above are treated as a single block. 
	 * Either all procedures are completed, or no at all. 
	 * Master procedure will be added to log table only in case
	 * of an error inside current procedure. If no errors - 
	 * only loading procedures go into log table with their own details. */
	EXCEPTION WHEN OTHERS THEN
		time_end := CLOCK_TIMESTAMP();
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
		time_run := EXTRACT (EPOCH FROM (time_end - time_start));
	
		CALL bl_cl.prc_load_logs(
			'prc_master_load_bl3nf',
			'bl_cl',
			time_start,
			time_end,
			time_run,
			0,
			0,
			er_flag,
			er_code,
			er_msg
			);
		RAISE NOTICE 'ERROR in master loading BL3NF. %: %',  SQLSTATE, SQLERRM;
	COMMIT ;
END; $$;

--TRUNCATE bl_cl.logs;
--CALL bl_cl.prc_master_load_bl3nf();
--SELECT * FROM bl_cl.logs;

COMMIT; 
