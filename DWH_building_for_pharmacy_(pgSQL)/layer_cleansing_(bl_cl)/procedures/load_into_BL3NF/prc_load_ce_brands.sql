-- _____________________________________________ CE_BRANDS LOAD PROCEDURE ____________________________________
/* CE_BRANDS are implemented as SCD1 */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_brands()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_3nf';
	prcd VARCHAR(50) := 'prc_load_ce_brands';
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_brands') f;

	BEGIN
		WITH src AS (
		SELECT
			COALESCE(brand_id, brand_name) AS src_brand_id,
			brand_name AS src_brand_name,
			'src_pharm_offline_sales' AS tab,
			'sa_offline' AS syst
		FROM sa_offline.src_pharm_offline_sales
		WHERE COALESCE(brand_id, brand_name) IS NOT NULL
		GROUP BY brand_id, brand_name
		UNION ALL
		SELECT
			COALESCE(brand_id, brand) AS src_brand_id,
			brand AS src_brand_name,
			'src_pharm_online_sales' AS tab,
			'sa_online' AS syst
		FROM sa_online.src_pharm_online_sales
		WHERE COALESCE(brand_id, brand) IS NOT NULL
		GROUP BY brand_id, brand
		)
		MERGE INTO bl_3nf.ce_brands ce
		USING (
				SELECT *
				FROM (
				SELECT
					COALESCE(mp.brand_id::VARCHAR, src.src_brand_id) AS nk,
					CASE WHEN mp.brand_id IS NOT NULL THEN 'bl_cl' ELSE src.syst END AS syst,
					CASE WHEN mp.brand_id IS NOT NULL THEN 'map_brands' ELSE src.tab END AS tab,
					COALESCE(mp.brand_name, src.src_brand_name, 'n.a.') AS brand
				FROM src
				LEFT OUTER JOIN bl_cl.map_brands mp
				ON mp.brand_src_id = src.src_brand_id
				AND mp.source_table = src.tab
				AND mp.source_system = src.syst
						)
				GROUP BY nk, syst, tab, brand
				ORDER BY brand -- is not necessary and extra, but just for beauty in project
			) upd
			
		ON ce.brand_src_id = upd.nk
		AND ce.source_system = upd.syst
		AND ce.source_table = upd.tab

		WHEN MATCHED AND ce.brand_name != upd.brand
			THEN UPDATE SET 
			brand_name = upd.brand, 
			ta_update_dt = CURRENT_DATE

		WHEN NOT MATCHED
			THEN INSERT VALUES (
			NEXTVAL('bl_3nf.bl_3nf_seq_brand_id'),
			upd.nk, 
			upd.syst, 
			upd.tab, 
			upd.brand, 
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

	SELECT f.count_ins, f.count_upd - (f.count_ins - rows_ins_before)
	INTO rows_ins_after, rows_upd_after
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_brands') f;

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


--DELETE FROM bl_3nf.ce_brands WHERE brand_id != -1;
--SELECT * FROM bl_3nf.ce_brands ORDER BY brand_id;
--CALL bl_cl.prc_load_ce_brands();
--SELECT * FROM bl_cl.logs;

COMMIT; 
