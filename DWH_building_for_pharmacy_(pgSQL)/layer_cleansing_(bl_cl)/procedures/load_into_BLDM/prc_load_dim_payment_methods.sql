-- _____________________________________________ DIM_PAYMENT_METHODS LOAD PROCEDURE ____________________________________
/* DIM_PAYMENT_METHODS are inplemented as SCD1 (overwrite) */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_payment_methods()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_dm';
	prcd VARCHAR(50) := 'prc_load_dim_payment_methods';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	run_time_sec NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN
	time_start := CLOCK_TIMESTAMP();

	SELECT f.count_ins, f.count_upd
	INTO rows_ins_before, rows_upd_before
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_dm.dim_payment_methods') f;

	BEGIN
		MERGE INTO bl_dm.dim_payment_methods dim
		USING (
		SELECT
			ce_pm.payment_method_id							AS payment_method_id,
			COALESCE(ce_pm.payment_method_name, 'n.a.')		AS payment_method_name
		FROM bl_3nf.ce_payment_methods ce_pm
		WHERE payment_method_id != -1
			) ce

		ON dim.payment_method_src_id = ce.payment_method_id::VARCHAR
		AND dim.source_table = 'ce_payment_methods'
		AND dim.source_system = 'bl_3nf'

		WHEN MATCHED AND dim.payment_method_name != ce.payment_method_name
			THEN UPDATE SET
			payment_method_name = ce.payment_method_name,
			ta_update_dt = CURRENT_DATE

		WHEN NOT MATCHED
			THEN INSERT VALUES (
				NEXTVAL('bl_dm.bl_dm_seq_payment_method_surr_id'),
				ce.payment_method_id,
				'bl_3nf',
				'ce_payment_methods',
				ce.payment_method_name,
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
	run_time_sec := EXTRACT (EPOCH FROM (time_end - time_start));

	SELECT f.count_ins, f.count_upd - (f.count_ins - rows_ins_before)
	INTO rows_ins_after, rows_upd_after
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_dm.dim_payment_methods') f;

	CALL bl_cl.prc_load_logs(
			prcd,
			schema_n,
			time_start,
			time_end,
			run_time_sec,
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


--DELETE FROM bl_dm.dim_payment_methods WHERE payment_method_surr_id != -1;
--CALL bl_cl.prc_load_dim_payment_methods();
--SELECT * FROM bl_dm.dim_payment_methods ORDER BY payment_method_surr_id;
--SELECT * FROM bl_cl.logs;

COMMIT;