-- _____________________________________________ CE_PRODUCTS_SCD LOAD PROCEDURE ____________________________________
/* CE_PRODUCTS_SCD are implemented as SCD2 */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_products_scd()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_3nf';
	prcd VARCHAR(50) := 'prc_load_ce_products_scd';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	time_run NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN
	time_start := CLOCK_TIMESTAMP();

	SELECT COUNT(*)
	INTO rows_ins_before
	FROM bl_3nf.ce_products_scd;

	/* when updating, 'is_active' is changing, we can count by this column,
	 * because we do not store separate update_dt in SCD2 */
	SELECT COUNT(*)
	INTO rows_upd_before
	FROM bl_3nf.ce_products_scd
	WHERE is_active = 'N';

	BEGIN
		/* First CTE contains combined products attributes from 2 sources */
		WITH src AS (
		SELECT
			COALESCE(prod_id, prod_name)		AS prod_id,
			prod_name							AS prod_name,
			subclass							AS subcat_id,
			class_id							AS cat_id,
			brand_id							AS brand_id,
			MIN("day"::DATE)					AS sales_dt, -- start_dt is considered first occurance of product is sales transactions
			'src_pharm_offline_sales'			AS tab,
			'sa_offline'						AS syst
		FROM sa_offline.src_pharm_offline_sales
		WHERE COALESCE(prod_id, prod_name) IS NOT NULL
		GROUP BY prod_id, prod_name, subclass, class_id, brand_id
		UNION ALL
		SELECT
			COALESCE(medicine_id, medicine),
			medicine,
			subcategory,
			cat_id,
			brand_id,
			MIN("date"::DATE), -- start_dt is considered first occurance of product is sales transactions
			'src_pharm_online_sales'b,
			'sa_online'
		FROM sa_online.src_pharm_online_sales
		WHERE COALESCE(medicine_id, medicine) IS NOT NULL
		GROUP BY medicine_id, medicine, subcategory, cat_id, brand_id
		),
		
		/* Second CTE contains ready to load data (conformed, transformed through map tables),
		 * handled NULLs and filtered: only modified versions of old products that should be added,
		 * or completely new products. */
		new_prod AS (
				SELECT
					-- Select min(sales_dt) again accross 2 sources for each product version.
					nk, src, tab, p_name, form, umm, um, upp, subcat_id, brand_id, MIN(sales_dt) AS start_dt
				FROM (
					SELECT
						COALESCE(mp.product_id::VARCHAR, src.prod_id, src.prod_name)				AS nk,
						CASE WHEN mp.product_id IS NOT NULL THEN 'bl_cl' ELSE src.syst END			AS src,
						CASE WHEN mp.product_id IS NOT NULL THEN 'map_products' ELSE src.tab END	AS tab,
						COALESCE(mp.product_name, src.prod_name, 'n.a')								AS p_name,
						COALESCE(mp.product_form, 'n.a.')											AS form,
						COALESCE(mp.unit_mass_measurement, 'n.a.')									AS umm,
						COALESCE(mp.unit_mass, 0)													AS um,
						COALESCE(mp.units_per_package, 1)											AS upp,
						COALESCE(ce_subcats.prod_subcategory_id, -1)								AS subcat_id,
						COALESCE(br.brand_id, -1)													AS brand_id,
						src.sales_dt																AS sales_dt
					FROM src
						LEFT OUTER JOIN bl_cl.map_products mp
						ON mp.product_src_id = src.prod_id
						AND mp.source_table = src.tab
						AND mp.source_system = src.syst
						/* Start_dt in map table is the same as min(sales_dt) in source. And is needed
						 * to join appropriate product version. */
						AND mp.start_dt = src.sales_dt

						LEFT OUTER JOIN bl_cl.map_prod_subcategories mp2
						ON UPPER(mp2.subcategory_src_id) = UPPER(src.subcat_id)
						AND mp2.category_src_id = src.cat_id
						AND mp2.source_table = src.tab
						AND mp2.source_system = src.syst

						LEFT OUTER JOIN bl_3nf.ce_prod_subcategories ce_subcats
						ON ce_subcats.prod_subcategory_src_id = COALESCE(mp2.subcategory_id::VARCHAR, src.subcat_id)
						AND ce_subcats.source_table =
							CASE WHEN mp2.subcategory_id IS NOT NULL THEN 'map_prod_subcategories' ELSE src.tab END
						AND ce_subcats.source_system =
							CASE WHEN mp2.subcategory_id IS NOT NULL THEN 'bl_cl' ELSE src.tab END

						LEFT OUTER JOIN bl_cl.map_brands mp3
						ON mp3.brand_src_id = src.brand_id
						AND mp3.source_table = src.tab
						AND mp3.source_system = src.syst

						LEFT OUTER JOIN bl_3nf.ce_brands br
						ON br.brand_src_id = COALESCE(mp3.brand_id::VARCHAR, src.brand_id)
						AND br.source_table =
							CASE WHEN mp3.brand_id IS NOT NULL THEN 'map_brands' ELSE src.tab END
						AND br.source_system =
							CASE WHEN mp3.brand_id IS NOT NULL THEN 'bl_cl' ELSE src.tab END
						)
					WHERE NOT EXISTS (
							/* Avoid duplicated insert: filter versions of product that are already
							 * stored. Filter by source triplet and additionally
							 * by product name (we extract other attributes from it). */
							SELECT 1
							FROM bl_3nf.ce_products_scd ce_prods
							WHERE ce_prods.product_src_id = nk
							AND ce_prods.source_table = tab
							AND ce_prods.source_system = src
							AND ce_prods.product_name = p_name
						)
					GROUP BY nk, src, tab, p_name, form, umm, um, upp, subcat_id, brand_id
			),
			
		/* Third CTE contains combined data from the previous CTE and versions of old products
		 * that are already in the DWH. This is needed because their end_dt, is_active have to be updated.
		 * And we don't want to do it row by row (the source can contain a lot of version of the same product).
		 * And already stored surr_id of old versions should be used for new versions. */
		dif_prod AS (
				SELECT NULL prod_id, np.*, NULL end_dt, NULL is_active, CURRENT_DATE ins_dt
				FROM new_prod np
				UNION ALL
				SELECT *
				FROM bl_3nf.ce_products_scd p
				WHERE EXISTS (
							SELECT 1 FROM new_prod np
							WHERE p.product_src_id = np.nk
							AND p.source_table = np.tab
							AND p.source_system = np.src
								)),
								
		/* Fourth CTE created for getting surr_ids. For already stored products - we use old surr_id from the previous CTE dif_prod.
		 * For the new arrived products - we create the new one. Since source can contain several versions
		 * of same product, and we should use the same ID for them, and sequence each time gets the new value,
		 * we group our data for getting nextval only once and will use it further for merge */
		get_prod_id AS (
				SELECT
					COALESCE(
							MAX(prod_id) FILTER (WHERE prod_id IS NOT NULL),
							NEXTVAL('bl_3nf.bl_3nf_seq_product_id'))		AS surr_id,
					nk														AS src_id
				FROM dif_prod
				GROUP BY nk
				ORDER BY nk::INT
						)
		MERGE INTO bl_3nf.ce_products_scd ce_prod 
		USING (
				SELECT
					gpi.surr_id,
					dif.nk,
					dif.src,
					dif.tab,
					dif.p_name,
					dif.form,
					dif.umm,
					dif.um,
					dif.upp,
					dif.subcat_id,
					dif.brand_id,
					dif.start_dt,
					/* by LEAD function get correct end_dt using appropriate partitioned windows. 
					 * Start dates will be used for ordering, (LEAD - 1 day interval) will be end_dt.  
					 * If the version has no leading versions, the end will be '9999-12-31'. */
					COALESCE((LEAD(start_dt) OVER w - INTERVAL '1 day')::DATE, '9999-12-31'::DATE) AS end_dt,
					CASE WHEN
						COALESCE(LEAD(start_dt) OVER w, '9999-12-31'::DATE) = '9999-12-31'::DATE
						THEN 'y' ELSE 'n' END AS is_active,
					dif.ins_dt AS ins_dt -- for old versions insert_dt doesn't change

				FROM dif_prod dif
					INNER JOIN get_prod_id gpi ON dif.nk = gpi.src_id
				WINDOW
					w AS (PARTITION BY dif.nk ORDER BY dif.start_dt)
				) AS upd

		/* Since we already separately got old surr IDs and assigned new ones, MERGE will be performed by it. */
		ON ce_prod.product_id = upd.surr_id
		AND ce_prod.start_dt = upd.start_dt

		-- this matches old version, that should be updated
		WHEN MATCHED THEN
			UPDATE SET 
			end_dt = upd.end_dt, 
			is_active = upd.is_active

		-- this matches new versions of old products, and completely new products, that should be inserted
		WHEN NOT MATCHED THEN
			INSERT VALUES (
				upd.surr_id, upd.nk, upd.src, upd.tab, upd.p_name, upd.form, upd.umm, upd.um, upd.upp,
				upd.subcat_id, upd.brand_id, upd.start_dt, upd.end_dt, upd.is_active, upd.ins_dt
				);

		-- exception during load that should be logged (if any)
		EXCEPTION WHEN OTHERS THEN
			er_flag := 'Y';
			er_code := SQLSTATE::VARCHAR(15);
			er_msg := SQLERRM::VARCHAR(300);
	END ;

	time_end := CLOCK_TIMESTAMP();
	time_run := EXTRACT (EPOCH FROM (time_end - time_start));

	SELECT COUNT(*)
	INTO rows_ins_after
	FROM bl_3nf.ce_products_scd;

	SELECT COUNT(*)
	INTO rows_upd_after
	FROM bl_3nf.ce_products_scd
	WHERE is_active = 'N';

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


--DELETE FROM bl_3nf.ce_products_scd WHERE product_id != -1;
--ALTER SEQUENCE bl_3nf.bl_3nf_seq_product_id RESTART WITH 1
--SELECT * FROM bl_3nf.ce_products_scd order by 1
--SELECT count(*) FROM bl_3nf.ce_products_scd
--CALL bl_cl.prc_load_ce_products_scd();
--SELECT * FROM bl_cl.logs ;

--CALL bl_cl.prc_load_ce_products_scd();

--SELECT * 
--FROM bl_3nf.ce_products_scd
--WHERE UPPER(product_name) LIKE 'MORPHINE%' or UPPER(product_name) LIKE 'CISAPRIDE%' ORDER BY 2;


COMMIT; 
