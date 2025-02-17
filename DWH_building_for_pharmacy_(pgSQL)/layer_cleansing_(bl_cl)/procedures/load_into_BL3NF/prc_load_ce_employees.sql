-- _____________________________________________ CE_EMPLOYESS LOAD PROCEDURE ____________________________________
/* CE_EMPLOYESS are inplemented as SCD1 (owerwrite). Implemented using MERGE. 
 * Employees exists only in one sales channel - offline. */


CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_employees()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_3nf';
	prcd VARCHAR(50) := 'prc_load_ce_employees';
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_employees') f;

	BEGIN
		WITH src AS (
		SELECT
			empl_id,
			empl_full_name,
			empl_date_of_birth,
			empl_phone,
			empl_gender,
			empl_email,
			"role" AS empl_role
		FROM sa_offline.src_pharm_offline_sales
		/* Filter entities for which at least one unique attribute is given
		 otherwise it will be default entity when loading into fact */
		WHERE COALESCE(empl_id, empl_phone,	empl_email) IS NOT NULL
		GROUP BY
			empl_id,
			empl_full_name,
			empl_date_of_birth,
			empl_phone,
			empl_gender,
			empl_email,
			"role"
			)
		MERGE INTO bl_3nf.ce_employees ce
		USING (
			SELECT
				/* Source ID(nk) becomes first unique attribute that is not NULL:
				 * entity ID from map table or from src, or email, or phone */
				COALESCE(src.empl_id, src.empl_phone, src.empl_email) AS nk,
				'sa_offline' AS syst,
				'src_pharm_offline_sales' AS tab,
				COALESCE(SPLIT_PART(INITCAP(src.empl_full_name), ' ', 1), 'n.a.') AS f_name,
				COALESCE(SPLIT_PART(INITCAP(src.empl_full_name), ' ', 2), 'n.a.') AS l_name,
				COALESCE(TO_DATE(src.empl_date_of_birth, 'MM/DD/YYYY'), '1900-01-01'::DATE) AS empl_bd,
				COALESCE(SUBSTRING(LTRIM(REGEXP_REPLACE(src.empl_phone, '\D', '', 'g'), '0|1'), 1, 10), 'n.a.') AS emp_phone,
				CASE WHEN LOWER(src.empl_gender) IN ('male', 'female')
					THEN
					LOWER(TRIM(src.empl_gender, ' '))
					ELSE 'n.a.' END AS gend,
				COALESCE(LOWER(src.empl_email), 'n.a.') AS email,
				COALESCE(LOWER(src.empl_role), 'n.a.') AS emp_role
			FROM src
					) upd
					
		ON ce.employee_src_id = upd.nk
		AND ce.source_table = upd.tab
		AND ce.source_system = upd.syst

		WHEN MATCHED AND (
		ce.empl_phone_num != upd.emp_phone OR
		ce.empl_email != upd.email OR
		ce.empl_position != upd.emp_role
		)
		THEN UPDATE SET
			empl_phone_num = upd.emp_phone,
			empl_email = upd.email,
			empl_position = upd.emp_role,
			ta_update_dt = CURRENT_DATE

		WHEN NOT MATCHED
			THEN INSERT VALUES (
				NEXTVAL('bl_3nf.bl_3nf_seq_employee_id'),
				upd.nk, upd.syst, upd.tab, upd.f_name, upd.l_name,
				upd.empl_bd, upd.emp_phone, upd.gend, upd.email,
				upd.emp_role, CURRENT_DATE, CURRENT_DATE
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_employees') f;

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


--DELETE FROM bl_3nf.ce_employees WHERE employee_id != -1;
--CALL bl_cl.prc_load_ce_employees();
--SELECT * FROM bl_3nf.ce_employees ORDER BY employee_id;
--SELECT * FROM bl_cl.logs;

COMMIT;
