-- _____________________________________________ DIM_PRODUCTS_SCD LOAD PROCEDURE ____________________________________
/* DIM_PRODUCTS_SCD are inplemented as SCD2 (add new row) */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_products_scd()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_dm';
	prcd VARCHAR(50) := 'prc_load_dim_products_scd';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	run_time_sec NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN
	time_start := CLOCK_TIMESTAMP();

	SELECT COUNT(*)
	INTO rows_ins_before
	FROM bl_dm.dim_products_scd;

	/* when updating, 'is_active' is changing, we can count by this column,
	 * because we do not store separate update_dt in SCD2 */
	SELECT COUNT(*)
	INTO rows_upd_before
	FROM bl_dm.dim_products_scd
	WHERE is_active = 'N';

	BEGIN
		MERGE INTO bl_dm.dim_products_scd dim
		USING (
		SELECT
			ce_prod.product_id									AS product_id,
			COALESCE(ce_prod.product_name, 'n.a.')				AS product_name,
			COALESCE(ce_prod.product_form, 'n.a.')				AS product_form,
			COALESCE(ce_prod.unit_mass_measurement, 'n.a.')		AS unit_mass_measurement,
			COALESCE(ce_prod.unit_mass, -1)						AS unit_mass,
			COALESCE(ce_prod.units_per_package, -1)				AS units_per_package,
			COALESCE(ce_subcat.prod_subcategory_id, -1)			AS prod_subcategory_id,
			COALESCE(ce_subcat.prod_subcategory_name, 'n.a.')	AS prod_subcategory_name,
			COALESCE(ce_subcat.prod_subcategory_descr, 'n.a.')	AS prod_subcategory_descr,
			COALESCE(ce_cat.prod_category_id, -1)				AS prod_category_id,
			COALESCE(ce_cat.prod_category_name, 'n.a.')			AS prod_category_name,
			COALESCE(ce_cat.prod_category_descr, 'n.a.')		AS prod_category_descr,
			COALESCE(ce_br.brand_id, -1)						AS brand_id,
			COALESCE(ce_br.brand_name, 'n.a.')					AS brand_name,
			COALESCE(ce_prod.start_dt, '1900-01-01'::DATE)		AS start_dt,
			COALESCE(ce_prod.end_dt, '9999-12-31'::DATE)		AS end_dt,
			COALESCE(ce_prod.is_active, 'Y')					AS is_active
		FROM bl_3nf.ce_products_scd ce_prod

		LEFT OUTER JOIN bl_3nf.ce_prod_subcategories ce_subcat
		ON ce_prod.prod_subcategory_id = ce_subcat.prod_subcategory_id

		LEFT OUTER JOIN bl_3nf.ce_prod_categories ce_cat
		ON ce_subcat.prod_category_id = ce_cat.prod_category_id

		LEFT OUTER JOIN bl_3nf.ce_brands ce_br
		ON ce_prod.brand_id = ce_br.brand_id

		WHERE product_id != -1
			) ce

		ON dim.product_src_id = ce.product_id::VARCHAR
		AND dim.start_dt = ce.start_dt
		AND dim.source_table = 'ce_products_scd'
		AND dim.source_system = 'bl_3nf'

		/* End_dt can be updated even after the version of product is already
		 * not active (when some version arrive late between 2 closed version
		 * and changes their end dates. That is why we select 2 attributes in
		 * the OR condition of the statement below */
		WHEN MATCHED AND (
		dim.is_active != ce.is_active OR
		dim.end_dt != ce.end_dt
		)
		THEN UPDATE SET
			is_active = ce.is_active,
			end_dt = ce.end_dt

		WHEN NOT MATCHED
			THEN INSERT VALUES (
				NEXTVAL('bl_dm.bl_dm_seq_product_surr_id'),
				ce.product_id,
				'bl_3nf',
				'ce_products_scd',
				ce.product_name,
				ce.product_form,
				ce.unit_mass_measurement,
				ce.unit_mass,
				ce.units_per_package,
				ce.prod_subcategory_id,
				ce.prod_subcategory_name,
				ce.prod_subcategory_descr,
				ce.prod_category_id,
				ce.prod_category_name,
				ce.prod_category_descr,
				ce.brand_id,
				ce.brand_name,
				ce.start_dt,
				ce.end_dt,
				ce.is_active,
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

	SELECT COUNT(*)
	INTO rows_ins_after
	FROM bl_dm.dim_products_scd;

	SELECT COUNT(*)
	INTO rows_upd_after
	FROM bl_dm.dim_products_scd
	WHERE is_active = 'N';

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

--DELETE FROM bl_dm.dim_products_scd WHERE product_surr_id != -1;
--CALL bl_cl.prc_load_dim_products_scd();
--SELECT * FROM bl_dm.dim_products_scd;
--SELECT * FROM bl_cl.logs;

COMMIT; 
