-- _____________________________________________ DIM_EMPLOYEES LOAD PROCEDURE ____________________________________
/* DIM_EMPLOYEES are inplemented as SCD1 (owerwrite) */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_employees()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_dm';
	prcd VARCHAR(50) := 'prc_load_dim_employees';
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_dm.dim_employees') f;

	BEGIN
		MERGE INTO bl_dm.dim_employees dim
		USING (
		SELECT
			employee_id									AS employee_id,
			COALESCE(empl_first_name, 'n.a.')			AS empl_first_name,
			COALESCE(empl_last_name, 'n.a.')			AS empl_last_name,
			COALESCE(empl_birth_dt, '1900-01-01'::DATE) AS empl_birth_dt,
			COALESCE(empl_phone_num, 'n.a.')			AS empl_phone_num,
			COALESCE(empl_gender, 'n.a.')				AS empl_gender,
			COALESCE(empl_email, 'n.a.')				AS empl_email,
			COALESCE(empl_position, 'n.a.')				AS empl_position
		FROM bl_3nf.ce_employees
		WHERE employee_id != -1
			) AS ce

		ON dim.employee_src_id = ce.employee_id::VARCHAR
		AND dim.source_table = 'ce_employees'
		AND dim.source_system = 'bl_3nf'

		WHEN MATCHED AND (
		dim.empl_phone_num != ce.empl_phone_num OR
		dim.empl_email != ce.empl_email OR
		dim.empl_position != ce.empl_position
		)	THEN UPDATE SET
				empl_phone_num = ce.empl_phone_num,
				empl_email = ce.empl_email,
				empl_position = ce.empl_position,
				ta_update_dt = CURRENT_DATE

		WHEN NOT MATCHED
			THEN INSERT VALUES (
				NEXTVAL('bl_dm.bl_dm_seq_employee_surr_id'),
				ce.employee_id,
				'bl_3nf',
				'ce_employees',
				ce.empl_first_name,
				ce.empl_last_name,
				ce.empl_birth_dt,
				ce.empl_phone_num,
				ce.empl_gender,
				ce.empl_email,
				ce.empl_position,
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

	/* Since new inserted rows have the same update_dt as current_date,
	 * to extract count of only updated rows we must substract difference
	 * of inserted rows (before-after) from count of updated rows after. */
	SELECT f.count_ins, f.count_upd - (f.count_ins - rows_ins_before)
	INTO rows_ins_after, rows_upd_after
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_dm.dim_employees') f;

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


--DELETE FROM bl_dm.dim_employees WHERE employee_surr_id != -1;
--CALL bl_cl.prc_load_dim_employees();
--SELECT * FROM bl_dm.dim_employees ORDER BY employee_surr_id;
--SELECT * FROM bl_cl.logs;

COMMIT; 
