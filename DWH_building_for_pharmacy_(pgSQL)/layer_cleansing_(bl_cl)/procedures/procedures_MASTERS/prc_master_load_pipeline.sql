--___________________________________________ MASTER PROCEDURE: PIPELINE all loads _____________________________________
/* The main procedure that CALLs all underlying master procedures of all layers. The whole pipeline */

CREATE OR REPLACE PROCEDURE bl_cl.prc_master_load_pipeline()
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
	time_start := CLOCK_TIMESTAMP();
	END;

	CALL bl_cl.prc_load_src_offline();
	CALL bl_cl.prc_load_src_online();

	CALL bl_cl.prc_master_load_blcl();

	CALL bl_cl.prc_master_load_bl3nf();
 	CALL bl_cl.prc_load_ce_sales();
 
	CALL bl_cl.prc_master_load_bldm();
	CALL bl_cl.prc_load_fct_sales_dd();

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
			'prc_master_load_pipeline',
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
		RAISE NOTICE 'ERROR in master loading PIPELINE. %: %',  SQLSTATE, SQLERRM;
	COMMIT;
END; $$;

--CALL bl_cl.prc_master_load_pipeline();
--SELECT * from bl_cl.fn_log_stat() order by start_time asc ;


COMMIT;