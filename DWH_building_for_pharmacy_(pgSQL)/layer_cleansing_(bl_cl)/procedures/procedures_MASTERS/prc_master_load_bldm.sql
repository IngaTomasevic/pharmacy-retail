--____________________________________________________ MASTER BL_DM LOAD PROCEDURE _____________________________________
/* CALLs all dimensions bl_dm loading procedures. Without fact table. */

CREATE OR REPLACE PROCEDURE bl_cl.prc_master_load_bldm()
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
	-- save time_start in separate subblock, it's needed for logging in case of an error during actions below
	BEGIN 
	time_start := CLOCK_TIMESTAMP();
	END; 

	CALL bl_cl.prc_load_dim_customers();
	CALL bl_cl.prc_load_dim_employees();
	CALL bl_cl.prc_load_dim_payment_methods();
	CALL bl_cl.prc_load_dim_products_scd();
	CALL bl_cl.prc_load_dim_promotions();
	CALL bl_cl.prc_load_dim_sales_channels();
	CALL bl_cl.prc_load_dim_stores();
	CALL bl_cl.prc_load_dim_suppliers();

	
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
			'prc_master_load_bldm',
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
		RAISE NOTICE 'ERROR in master loading BL_DM. %: %',  SQLSTATE, SQLERRM;
	COMMIT;
END; $$;


--CALL bl_cl.prc_master_load_bldm();
--SELECT * FROM bl_cl.logs;

COMMIT; 
