-- _____________________________________________ DIM_SUPPLIERS LOAD PROCEDURE ____________________________________
/* DIM_SUPPLIERS are inplemented as SCD1 (overwrite) */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_suppliers()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_dm';
	prcd VARCHAR(50) := 'prc_load_dim_suppliers';
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_dm.dim_suppliers') f;

	BEGIN
		MERGE INTO bl_dm.dim_suppliers dim
		USING (
		SELECT
			ce_supl.supplier_id								AS supplier_id,
			COALESCE(ce_supl.supplier_name, 'n.a.')			AS supplier_name,
			COALESCE(ce_supl.supplier_phone_num, 'n.a.')	AS supplier_phone_num,
			COALESCE(ce_supl.supplier_email, 'n.a.')		AS supplier_email
		FROM bl_3nf.ce_suppliers ce_supl
		WHERE supplier_id != -1
			) ce

		ON dim.supplier_src_id = ce.supplier_id::VARCHAR
		AND dim.source_table = 'ce_suppliers'
		AND dim.source_system = 'bl_3nf'

		WHEN MATCHED AND (
		dim.supplier_name != ce.supplier_name OR
		dim.supplier_phone_num != ce.supplier_phone_num OR
		dim.supplier_email != ce.supplier_email
		)	THEN UPDATE SET
			supplier_name = ce.supplier_name,
			supplier_phone_num = ce.supplier_phone_num,
			supplier_email = ce.supplier_email,
			ta_update_dt = CURRENT_DATE

		WHEN NOT MATCHED
			THEN INSERT VALUES (
				NEXTVAL('bl_dm.bl_dm_seq_supplier_surr_id'),
				ce.supplier_id,
				'bl_3nf',
				'ce_suppliers',
				ce.supplier_name,
				ce.supplier_phone_num,
				ce.supplier_email,
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_dm.dim_suppliers') f;

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


--DELETE FROM bl_dm.dim_suppliers WHERE supplier_surr_id != -1;
--CALL bl_cl.prc_load_dim_suppliers();
--SELECT * FROM bl_dm.dim_suppliers ORDER BY supplier_surr_id;
--SELECT * FROM bl_cl.logs;

COMMIT;