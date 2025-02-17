-- _____________________________________________ DIM_PROMOTIONS LOAD PROCEDURE ____________________________________
/* DIM_PROMOTIONS are inplemented as SCD1 (overwrite) */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_promotions()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_dm';
	prcd VARCHAR(50) := 'prc_load_dim_promotions';
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_dm.dim_promotions') f;

	BEGIN
		MERGE INTO bl_dm.dim_promotions dim
		USING (
		SELECT
			ce_promo.promo_id								AS promo_id,
			COALESCE(ce_promo.promo_name, 'n.a.')			AS promo_name,
			COALESCE(ce_promo.promo_discount, 0)			AS promo_discount,
			COALESCE(ce_cat.promo_category_id, -1)			AS promo_category_id,
			COALESCE(ce_cat.promo_category_name, 'n.a.')	AS promo_category_name,
			COALESCE(ce_cnl.promo_channel_id, -1)			AS promo_channel_id,
			COALESCE(ce_cnl.promo_channel_name, 'n.a.')		AS promo_channel_name
		FROM bl_3nf.ce_promotions ce_promo

		LEFT OUTER JOIN bl_3nf.ce_promo_categories ce_cat
		ON ce_promo.promo_category_id = ce_cat.promo_category_id

		LEFT OUTER JOIN bl_3nf.ce_promo_channels ce_cnl
		ON ce_promo.promo_channel_id = ce_cnl.promo_channel_id

		WHERE promo_id != -1
			) ce

		ON dim.promo_src_id = ce.promo_id::VARCHAR
		AND dim.source_table = 'ce_promotions'
		AND dim.source_system = 'bl_3nf'

		WHEN MATCHED AND (
		dim.promo_name != ce.promo_name OR
		dim.promo_discount != ce.promo_discount
		)
		THEN UPDATE SET
			promo_name = ce.promo_name,
			promo_discount = ce.promo_discount,
			ta_update_dt = CURRENT_DATE

		WHEN NOT MATCHED
			THEN INSERT VALUES (
				NEXTVAL('bl_dm.bl_dm_seq_promo_surr_id'),
				ce.promo_id,
				'bl_3nf',
				'ce_promotions',
				ce.promo_name,
				ce.promo_discount,
				ce.promo_category_id,
				ce.promo_category_name,
				ce.promo_channel_id,
				ce.promo_channel_name,
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_dm.dim_promotions') f;

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


--DELETE FROM bl_dm.dim_promotions WHERE promo_surr_id != -1;
--CALL bl_cl.prc_load_dim_promotions();
--SELECT * FROM bl_dm.dim_promotions ORDER BY promo_surr_id;
--SELECT * FROM bl_cl.logs;

COMMIT; 
