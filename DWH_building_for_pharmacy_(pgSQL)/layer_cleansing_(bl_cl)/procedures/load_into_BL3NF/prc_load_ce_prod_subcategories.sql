-- _____________________________________________ CE_PROD_SUBCATEGORIES LOAD PROCEDURE ____________________________________
/* CE_PROD_SUBCATEGORIES are implemented as SCD1 (owerwrite) */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_prod_subcategories()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_3nf';
	prcd VARCHAR(50) := 'prc_load_ce_prod_subcategories';
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_prod_subcategories') f;

	BEGIN
		WITH src AS (
		SELECT
			subclass AS src_subcat_name,
			class_id AS src_cat_id,
			subclass_descr AS src_descr,
			'src_pharm_offline_sales' AS tab,
			'sa_offline' AS syst
		FROM sa_offline.src_pharm_offline_sales
		WHERE subclass IS NOT NULL
		GROUP BY subclass, class_id, subclass_descr
		UNION ALL
		SELECT
			subcategory AS src_subcat_name,
			cat_id AS src_cat_id,
			'' AS src_descr,
			'src_pharm_online_sales' AS tab,
			'sa_online' AS syst
		FROM sa_online.src_pharm_online_sales
		WHERE subcategory IS NOT NULL
		GROUP BY subcategory, cat_id
		)
		MERGE INTO bl_3nf.ce_prod_subcategories ce
		USING (
				SELECT
					nk, src, tab, subcat_name,
					CASE WHEN STRING_AGG(descr, '') = '' THEN 'n.a.' ELSE STRING_AGG(descr, '') END AS descr,
					cat_id
				FROM (
				SELECT
					/* Source ID becomes first attribute that is not NULL:
					 * subcat ID from map table, or from src */
					COALESCE(mp.subcategory_id::VARCHAR, src.src_subcat_name) AS nk,
					CASE WHEN mp.subcategory_id IS NOT NULL THEN 'bl_cl' ELSE src.syst END AS src,
					CASE WHEN mp.subcategory_id IS NOT NULL THEN 'map_prod_subcategories' ELSE src.tab END AS tab,
					COALESCE(mp.subcategory_name, src.src_subcat_name, 'n.a.') AS subcat_name,
					COALESCE(src.src_descr, 'n.a.') AS descr,
					COALESCE(ce_cats.prod_category_id, -1) AS cat_id
				FROM src
				-- subcateories are mapped by categories they belong to!
				LEFT OUTER JOIN bl_cl.map_prod_subcategories mp
				ON mp.subcategory_src_id = src.src_subcat_name
				AND mp.category_src_id = src.src_cat_id
				AND mp.source_table = src.tab
				AND mp.source_system = src.syst
				
				LEFT OUTER JOIN bl_cl.map_prod_categories mp2
				ON mp2.prod_category_src_id = src.src_cat_id
				AND mp2.source_table = src.tab
				AND mp2.source_system = src.syst

				-- extract refference to the parent category
				LEFT OUTER JOIN bl_3nf.ce_prod_categories ce_cats
				ON ce_cats.prod_category_src_id = COALESCE(mp2.prod_category_id::VARCHAR, src.src_cat_id)
				AND ce_cats.source_table = CASE
					WHEN mp2.prod_category_id IS NOT NULL THEN 'map_prod_categories'
					ELSE src.syst END
				AND ce_cats.source_system = CASE
					WHEN mp2.prod_category_id IS NOT NULL THEN 'bl_cl'
					ELSE src.syst END
				)
				GROUP BY nk, src, tab, subcat_name, cat_id
				ORDER BY subcat_name, cat_id::INT -- just for beauty, should be avoided in huge real project
			) upd
			
		ON ce.prod_subcategory_src_id = upd.nk
		AND ce.source_system = upd.src
		AND ce.source_table = upd.tab

		WHEN MATCHED AND ce.prod_subcategory_name != upd.subcat_name
			THEN UPDATE SET 
			prod_subcategory_name = upd.subcat_name, 
			ta_update_dt = CURRENT_DATE

		WHEN NOT MATCHED
			THEN INSERT VALUES (
			NEXTVAL('bl_3nf.bl_3nf_seq_prod_subcategory_id'),
			upd.nk, upd.src, upd.tab, upd.subcat_name, upd.descr, upd.cat_id, CURRENT_DATE, CURRENT_DATE
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_prod_subcategories') f;

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


--DELETE FROM bl_3nf.ce_prod_subcategories WHERE prod_subcategory_id != -1 ;
--ALTER SEQUENCE bl_3nf.bl_3nf_seq_prod_subcategory_id RESTART WITH 1
--SELECT * FROM bl_3nf.ce_prod_subcategories order by prod_subcategory_id;
--CALL bl_cl.prc_load_ce_prod_subcategories();
--SELECT * FROM bl_cl.logs;

COMMIT; 
