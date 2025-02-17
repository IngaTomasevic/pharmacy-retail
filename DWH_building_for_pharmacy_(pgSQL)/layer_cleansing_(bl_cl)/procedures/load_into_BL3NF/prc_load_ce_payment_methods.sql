-- _____________________________________________ CE_PAYMENT_METHODS LOAD PROCEDURE ____________________________________
/* CE_PAYMENT_METHODS are implemented as SCD1.
 * Are explicitly defined only in one(offline) source, 
 * all online sales are by default made by card paymnet method. */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_payment_methods()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_3nf';
	prcd VARCHAR(50) := 'prc_load_ce_payment_methods';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	time_run NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN
	time_start := CLOCK_TIMESTAMP();

	SELECT f.count_ins, f.count_upd
	INTO rows_ins_before, rows_upd_before
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_payment_methods') f;

	BEGIN
	MERGE INTO bl_3nf.ce_payment_methods ce
	USING (
		SELECT
			src.payment_type AS pm_id,
			'sa_offline' AS syst,
			'src_pharm_offline_sales' AS tab,
			LOWER(src.payment_type) AS pm_name
		FROM (
				SELECT payment_type
				FROM sa_offline.src_pharm_offline_sales
				WHERE payment_type IS NOT NULL
				GROUP BY payment_type
				) src
		) upd
				
	ON ce.payment_method_src_id = upd.pm_id
	AND ce.source_system = upd.syst
	AND ce.source_table = upd.tab
	
	WHEN MATCHED AND ce.payment_method_name != upd.pm_name
		THEN UPDATE SET 
		payment_method_name = upd.pm_name, 
		ta_update_dt = CURRENT_DATE
		
	WHEN NOT MATCHED 
		THEN INSERT VALUES(
		NEXTVAL('bl_3nf.bl_3nf_seq_payment_method_id'),
		upd.pm_id, 
		upd.syst, 
		upd.tab, 
		upd.pm_name, 
		CURRENT_DATE,
		CURRENT_DATE
		);
	
	-- exception during load that should be logged (if any)
	EXCEPTION WHEN OTHERS THEN
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
	END ;

	time_end := CLOCK_TIMESTAMP();
	time_run := EXTRACT (EPOCH FROM (time_end - time_start));

	/* Since new inserted rows have the same update_dt as current_date,
	 * to extract count of only updated rows we must substract difference
	 * of inserted rows (before-after) from count of updated rows after. */
	SELECT f.count_ins, f.count_upd - (f.count_ins - rows_ins_before)
	INTO rows_ins_after, rows_upd_after
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_payment_methods') f;

	CALL bl_cl.prc_load_logs(
			prcd,
			schema_n,
			time_start,
			time_end,
			time_run,
			(rows_ins_after - rows_ins_before),
			(rows_upd_after - rows_upd_before),
			er_flag,
			er_code,
			er_msg
			);
		
	-- exception that can occur during last 4 actions
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Error in %, %: %', prcd, SQLSTATE, SQLERRM;
	COMMIT;
END; $$;


--DELETE FROM bl_3nf.ce_payment_methods WHERE payment_method_id != -1;
--ALTER SEQUENCE bl_3nf.bl_3nf_seq_payment_method_id RESTART WITH 1
--SELECT * FROM bl_3nf.ce_payment_methods;
--CALL bl_cl.prc_load_ce_payment_methods();
--SELECT * FROM bl_cl.logs;

COMMIT; 
