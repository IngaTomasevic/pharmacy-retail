-- _____________________________________________ CE_SUPPLIERS LOAD PROCEDURE ____________________________________
/* CE_SUPPLIERS are implemented as SCD1 */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_suppliers()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_3nf';
	prcd VARCHAR(50) := 'prc_load_ce_suppliers';
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_suppliers') f;

	BEGIN
		WITH src AS (
		SELECT
			COALESCE(supplier_id, supplier) AS src_suppl_id,
			supplier AS src_suppl_name,
			supplier_phone AS src_phone,
			supplier_email AS src_email,
			'src_pharm_offline_sales' AS tab,
			'sa_offline' AS syst
		FROM sa_offline.src_pharm_offline_sales
		WHERE COALESCE(supplier_id, supplier, supplier_phone, supplier_email) IS NOT NULL
		GROUP BY supplier_id, supplier, supplier_phone, supplier_email
		UNION ALL
		SELECT
			COALESCE(supplier_id, supplier) AS src_suppl_id,
			supplier AS src_suppl_name,
			supplier_phone AS src_phone,
			supplier_email AS src_email,
			'src_pharm_online_sales' AS tab,
			'sa_online' AS syst
		FROM sa_online.src_pharm_online_sales
		WHERE COALESCE(supplier_id, supplier, supplier_phone, supplier_phone, supplier_email) IS NOT NULL
		GROUP BY supplier_id, supplier, supplier_phone, supplier_email
		)
		
		MERGE INTO bl_3nf.ce_suppliers ce
		USING (
			SELECT nk, syst, tab, suppl_name, MAX(phone) AS phone, MAX(email) AS email
			FROM (
				SELECT
					COALESCE(mp.supplier_id::VARCHAR, src.src_suppl_id, src.src_phone, src.src_email) AS nk,
					CASE WHEN mp.supplier_id IS NOT NULL THEN 'bl_cl' ELSE src.syst END AS syst,
					CASE WHEN mp.supplier_id IS NOT NULL THEN 'map_suppliers' ELSE src.tab END AS tab,
					COALESCE(mp.supplier_name, src.src_suppl_name, 'n.a.') AS suppl_name,
					COALESCE(REGEXP_REPLACE(src.src_phone, '\D', '', 'g'), '') AS phone,
					COALESCE(src.src_email, '') AS email
				FROM src
				LEFT OUTER JOIN bl_cl.map_suppliers mp
				ON mp.supplier_src_id = src.src_suppl_id
				AND mp.source_table = src.tab
				AND mp.source_system = src.syst
				)
				GROUP BY nk, syst, tab, suppl_name
				ORDER BY nk::INT -- just for beauty, should be skipped in huge real project
			) upd
			
		ON ce.supplier_src_id = upd.nk
		AND ce.source_system = upd.syst
		AND ce.source_table = upd.tab

		WHEN MATCHED AND (
		ce.supplier_name != upd.suppl_name OR
		ce.supplier_phone_num != upd.phone OR
		ce.supplier_email != upd.email
		)	THEN UPDATE SET
			supplier_name = upd.suppl_name,
			supplier_phone_num = upd.phone,
			supplier_email = upd.email,
			ta_update_dt = CURRENT_DATE

		WHEN NOT MATCHED
			THEN INSERT VALUES(
			NEXTVAL('bl_3nf.bl_3nf_seq_supplier_id'),
			upd.nk, upd.syst, upd.tab, upd.suppl_name, upd.phone, upd.email, CURRENT_DATE, CURRENT_DATE
			);

	-- exception during load that should be logged (if any)
	EXCEPTION WHEN OTHERS THEN
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
	END ;

	time_end := CLOCK_TIMESTAMP();
	time_run := EXTRACT (EPOCH FROM (time_end - time_start));

	SELECT f.count_ins, f.count_upd - (f.count_ins - rows_ins_before)
	INTO rows_ins_after, rows_upd_after
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_suppliers') f;

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


--DELETE FROM bl_3nf.ce_suppliers where supplier_id != -1;
--ALTER SEQUENCE bl_3nf.bl_3nf_seq_supplier_id RESTART WITH 1
--SELECT * FROM bl_3nf.ce_suppliers ORDER BY supplier_id;
--CALL bl_cl.prc_load_ce_suppliers();
--SELECT * FROM bl_cl.logs;

COMMIT; 
