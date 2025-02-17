-- _____________________________________________ CE_PROD_CATEGORIES LOAD PROCEDURE ____________________________________
/* CE_PROD_CATEGORIES are implemented as SCD1 (owerwrite) */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_prod_categories()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_3nf';
	prcd VARCHAR(50) := 'prc_load_ce_prod_categories';
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_prod_categories') f;

	BEGIN
		WITH src AS (
		SELECT
			class_id AS src_cat_id,
			class_name AS src_cat_name,
			class_descr AS src_descr,
			'src_pharm_offline_sales' AS tab,
			'sa_offline' AS syst
		FROM sa_offline.src_pharm_offline_sales
		WHERE COALESCE(class_id, class_name, class_descr) IS NOT NULL
		GROUP BY class_id, class_name, class_descr
		UNION ALL
		SELECT
			cat_id AS src_cat_id,
			category AS src_cat_name,
			'' AS src_descr,
			'src_pharm_online_sales' AS tab,
			'sa_online' AS syst
		FROM sa_online.src_pharm_online_sales
		WHERE COALESCE(cat_id, category) IS NOT NULL
		GROUP BY cat_id, category
		)
		MERGE INTO bl_3nf.ce_prod_categories ce
		USING (
				SELECT
					nk, src, tab, cat_name,
					CASE WHEN STRING_AGG(descr, '') = '' THEN 'n.a.' ELSE STRING_AGG(descr, '') END AS descr
				FROM (
				SELECT
					COALESCE(mp.prod_category_id::VARCHAR, src.src_cat_id, src.src_cat_name, src.src_descr) AS nk,
					CASE WHEN mp.prod_category_id IS NOT NULL THEN 'bl_cl' ELSE src.syst END AS src,
					CASE WHEN mp.prod_category_id IS NOT NULL THEN 'map_prod_categories' ELSE src.tab END AS tab,
					COALESCE(mp.prod_category_name::VARCHAR, src.src_cat_name, 'n.a.') AS cat_name,
					COALESCE(src.src_descr, 'n.a.') AS descr
				FROM src
				LEFT OUTER JOIN bl_cl.map_prod_categories mp
				ON mp.prod_category_src_id = src.src_cat_id
				AND mp.source_table = src.tab
				AND mp.source_system = src.syst
				)
				GROUP BY nk, src, tab, cat_name
				ORDER BY cat_name -- just for beauty, for huge real project of cource is extra action
			) upd
			
		ON ce.prod_category_src_id = upd.nk
		AND ce.source_system = upd.src
		AND ce.source_table = upd.tab

		WHEN MATCHED AND ce.prod_category_name != upd.cat_name
			THEN UPDATE SET 
			prod_category_name = upd.cat_name, 
			ta_update_dt = CURRENT_DATE

		WHEN NOT MATCHED
			THEN INSERT VALUES(
			NEXTVAL('bl_3nf.bl_3nf_seq_prod_category_id'),
			upd.nk, upd.src, upd.tab, upd.cat_name, upd.descr, CURRENT_DATE, CURRENT_DATE
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_prod_categories') f;

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


--DELETE FROM bl_3nf.ce_prod_categories WHERE prod_category_id != -1;
--ALTER SEQUENCE bl_3nf.bl_3nf_seq_prod_category_id RESTART WITH 1
--SELECT * FROM bl_3nf.ce_prod_categories;
--CALL bl_cl.prc_load_ce_prod_categories();
--SELECT * FROM bl_cl.logs;

COMMIT;
