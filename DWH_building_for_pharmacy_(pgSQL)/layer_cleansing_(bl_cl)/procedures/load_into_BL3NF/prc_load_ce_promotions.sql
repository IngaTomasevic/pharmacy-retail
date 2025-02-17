-- _____________________________________________ CE_PROMOTIONS LOAD PROCEDURE ____________________________________
/* CE_PROMOTIONS are implemented as SCD1 */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_promotions()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_3nf';
	prcd VARCHAR(50) := 'prc_load_ce_promotions';
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_promotions') f;

	BEGIN
		WITH src AS (
		SELECT
			COALESCE(promotion_id, promotion) AS src_id,
			promotion AS src_name,
			discount,
			promotion_subcategory_id AS src_cat_id,
			promotion_channel_id AS src_cnl_id,
			'src_pharm_offline_sales' AS tab,
			'sa_offline' AS syst
		FROM sa_offline.src_pharm_offline_sales
		WHERE COALESCE(promotion_id, promotion) IS NOT NULL
		GROUP BY promotion_id, promotion, discount, promotion_subcategory_id, promotion_channel_id
		UNION ALL
		SELECT
			COALESCE(promo_id, promo) AS src_id,
			promo AS src_name,
			promo_discount AS discount,
			promo_type_id AS src_cat_id,
			promo_distr_id AS src_cnl_id,
			'src_pharm_online_sales' AS tab,
			'sa_online' AS syst
		FROM sa_online.src_pharm_online_sales
		WHERE COALESCE(promo_id, promo) IS NOT NULL
		GROUP BY promo_id, promo, promo_discount, promo_type_id, promo_distr_id
		)
		
		MERGE INTO bl_3nf.ce_promotions ce
		USING(
			SELECT *
			FROM (
			SELECT
				COALESCE(mp.promotion_id::VARCHAR, src.src_id) AS nk,
				CASE WHEN mp.promotion_id IS NOT NULL THEN 'bl_cl' ELSE src.syst END AS syst,
				CASE WHEN mp.promotion_id IS NOT NULL THEN 'map_promotions' ELSE src.tab END AS tab,
				TRIM(COALESCE(mp.promotion_name, src.src_name, 'n.a.'), '"“”"') AS promo_name,
				COALESCE(src.discount::INT, -1) AS disc,
				COALESCE(pc.promo_category_id, -1) AS promo_cat,
				COALESCE(pcn.promo_channel_id, -1) AS promo_cnl
			FROM src
			LEFT OUTER JOIN bl_cl.map_promotions mp
			ON mp.promotion_src_id = src.src_id
			AND mp.source_table = src.tab
			AND mp.source_system = src.syst

			LEFT OUTER JOIN bl_cl.map_promo_categories mp2
			ON mp2.promo_category_src_id = src.src_cat_id
			AND mp2.source_table = src.tab
			AND mp2.source_system = src.syst

			LEFT OUTER JOIN bl_3nf.ce_promo_categories pc
			ON pc.promo_category_src_id = COALESCE(mp2.promo_category_id::VARCHAR, src.src_cat_id)
			AND pc.source_table = CASE
				WHEN mp2.promo_category_id IS NOT NULL THEN 'map_promo_categories'
				WHEN src.src_cat_id IS NOT NULL THEN src.tab END
			AND pc.source_system = CASE
				WHEN mp2.promo_category_id IS NOT NULL THEN 'bl_cl'
				WHEN src.src_cat_id IS NOT NULL THEN src.syst END

			LEFT OUTER JOIN bl_cl.map_promo_chanels mp3
			ON mp3.promo_channel_src_id = src.src_cnl_id
			AND mp3.source_table = src.tab
			AND mp3.source_system = src.syst

			LEFT OUTER JOIN bl_3nf.ce_promo_channels pcn
			ON pcn.promo_channel_src_id = COALESCE(mp3.promo_channel_id::VARCHAR, src.src_cnl_id)
			AND pcn.source_table = CASE
				WHEN mp3.promo_channel_id IS NOT NULL THEN 'map_promo_chanels'
				WHEN src.src_cnl_id IS NOT NULL THEN src.tab END
			AND pcn.source_system = CASE
				WHEN mp3.promo_channel_id IS NOT NULL THEN 'bl_cl'
				WHEN src.src_cnl_id IS NOT NULL THEN src.syst END
					)
			GROUP BY nk, syst, tab, promo_name, disc, promo_cat, promo_cnl
			ORDER BY nk::INT
		) upd

		ON ce.promo_src_id = upd.nk
		AND ce.source_system = upd.syst
		AND ce.source_table = upd.tab

		WHEN MATCHED AND (
		ce.promo_name != upd.promo_name OR
		ce.promo_discount != upd.disc
		)	THEN UPDATE SET
			promo_name = upd.promo_name,
			promo_discount = upd.disc,
			ta_update_dt = CURRENT_DATE

		WHEN NOT MATCHED
			THEN INSERT VALUES (
			NEXTVAL('bl_3nf.bl_3nf_seq_promo_id'),
			upd.nk, upd.syst, upd.tab, upd.promo_name, upd.disc,
			upd.promo_cat, upd.promo_cnl, CURRENT_DATE, CURRENT_DATE
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_promotions') f;

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


--DELETE FROM bl_3nf.ce_promotions WHERE promo_id != -1;
--ALTER SEQUENCE bl_3nf.bl_3nf_seq_promo_id RESTART WITH 1
--SELECT * FROM bl_3nf.ce_promotions ORDER BY 5;
--CALL bl_cl.prc_load_ce_promotions();
--SELECT * FROM bl_cl.logs;

COMMIT; 
