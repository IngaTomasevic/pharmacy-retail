-- _____________________________________________ CE_PROMO_CATEGORIES LOAD PROCEDURE ____________________________________
/* CE_PROMO_CATEGORIES are implemented as SCD1 */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_promo_categories()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_3nf';
	prcd VARCHAR(50) := 'prc_load_ce_promo_categories';
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_promo_categories') f;

	BEGIN
	WITH src AS (
			SELECT
				COALESCE(promotion_subcategory_id, promotion_subcategory) AS src_id,
				promotion_subcategory AS src_name,
				'src_pharm_offline_sales' AS tab,
				'sa_offline' AS syst
			FROM sa_offline.src_pharm_offline_sales
			WHERE COALESCE(promotion_subcategory_id, promotion_subcategory) IS NOT NULL
			GROUP BY promotion_subcategory_id, promotion_subcategory
			UNION ALL
			SELECT
				COALESCE(promo_type_id, promo_type) AS src_id,
				promo_type AS src_name,
				'src_pharm_online_sales' AS tab,
				'sa_online' AS syst
			FROM sa_online.src_pharm_online_sales
			WHERE COALESCE(promo_type_id, promo_type) IS NOT NULL
			GROUP BY promo_type_id, promo_type
			)
			
	MERGE INTO bl_3nf.ce_promo_categories ce
	USING (
		SELECT *
		FROM (
		SELECT
			COALESCE(mp.promo_category_id::VARCHAR, src.src_id) AS nk,
			CASE WHEN mp.promo_category_id IS NOT NULL THEN 'bl_cl' ELSE src.syst END AS syst,
			CASE WHEN mp.promo_category_id IS NOT NULL THEN 'map_promo_categories' ELSE src.tab END AS tab,
			COALESCE(mp.promo_category_name, src.src_name, 'n.a.') AS cat_name
		FROM src
		LEFT OUTER JOIN bl_cl.map_promo_categories mp
		ON mp.promo_category_src_id = src.src_id
		AND mp.source_table = src.tab
		AND mp.source_system = src.syst
				)
		GROUP BY nk, syst, tab, cat_name
		ORDER BY nk::INT
	) upd
	
	ON ce.promo_category_src_id = upd.nk
	AND ce.source_system = upd.syst
	AND ce.source_table = upd.tab

	WHEN MATCHED AND ce.promo_category_name != upd.cat_name
		THEN UPDATE SET 
		promo_category_name = upd.cat_name, 
		ta_update_dt = CURRENT_DATE

	WHEN NOT MATCHED
		THEN INSERT VALUES (
		NEXTVAL('bl_3nf.bl_3nf_seq_promo_category_id'),
		upd.nk, upd.syst, upd.tab, upd.cat_name, CURRENT_DATE, CURRENT_DATE
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_promo_categories') f;


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


--DELETE FROM bl_3nf.ce_promo_categories WHERE promo_category_id != -1;
--ALTER SEQUENCE bl_3nf.bl_3nf_seq_promo_category_id RESTART WITH 1
--SELECT * FROM bl_3nf.ce_promo_categories ORDER BY promo_category_name;
--CALL bl_cl.prc_load_ce_promo_categories();
--SELECT * FROM bl_cl.logs;

COMMIT;
