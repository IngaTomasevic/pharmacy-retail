--________________________ MAPPING: PRODUCTS _______________________________________
/* Map product by subcategories and categories they belong to, because same categories may have
 * same subcategories names, and there are several product names that belong to different subcategories. */

/* Procedure adopted for changing according to SCD2 */


--TRUNCATE bl_cl.map_products;

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_map_products()
LANGUAGE plpgsql 
AS $$
DECLARE 
	max_id INT; 
	current_id INT;
	rows_ins_before INT;
	rows_ins_after INT;
	schema_n VARCHAR(10) := 'bl_cl';
	prcd VARCHAR(50) := 'prc_load_map_products';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	run_time_sec NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
	rec record;

	/* Cursor is used because all versions of products should be mapped with same id. 
	 * Taken into account that at map stage there are different sources, we can't use window function to 
	 * get same id (different names of products versions, different NKs in different sources -> no attribute for windowing). 
	 * It was decided go row by row and get already existed ID, or asign next. 
	 
	 * Comparison each next row will be performed:
	 * - either by source triplet (if NK from the same source already exists)
	 * - either by combination (product name + subcategory + category), they are same in both sources.
	 * Becuase there is the possibility, that the product already is present in table, but it came from 
	 * foreign source. We have to have the ability to recognize, that such product is the same we are comparing.
	 * Or other version of product (with other name) already exists, and it's id should be taken.
	
	 * Cursor works well with ordered set, it gives us possibility to have control over how it should
	 * go row-by row. Because simple INSERT inserts random records numbers. */
	bound_cur CURSOR FOR 
	WITH src AS (
		SELECT 
			prod_id AS prod_id, 
			prod_name AS prod,
			subclass AS subcat,
			class_id AS cat_id,
			class_name AS cat,
			/* Min date (as start_dt) will be used for join with sources when loading into 3nf. 
			 * All start_dt will be tied with the first occurance of product in sales transactions table. 
			 * And since we should store all versions starting from map stage, there should be some attribute
			 * to identify the period. For map table start_dt is enough, no need end_dt or is_active. */
			MIN("day"::DATE) AS sales_dt, 
			'src_pharm_offline_sales' AS tab, 
			'sa_offline' AS syst
		FROM sa_offline.src_pharm_offline_sales ofl
		WHERE NOT EXISTS (
				-- filter new products or changed (add names to filtering, all other attributes are derived from name)
				SELECT 1
				FROM bl_cl.map_products mp
				WHERE mp.product_src_id = ofl.prod_id AND 
				UPPER(mp.product_name) = UPPER(prod_name) AND 
				mp.source_table = 'src_pharm_offline_sales' AND 
				mp.source_system = 'sa_offline'
						)
		AND prod_id IS NOT NULL
		GROUP BY prod_id, prod_name, subclass, class_id, class_name
		UNION ALL
		SELECT 
			medicine_id,
			medicine,
			subcategory,
			cat_id,
			category,
			MIN("date"::DATE) AS sales_dt,
			'src_pharm_online_sales' AS tab, 
			'sa_online' AS syst
		FROM sa_online.src_pharm_online_sales onl
		WHERE NOT EXISTS (
				-- filter new products or changed (add names to filtering, all other attributes are derived from name)
				SELECT 1
				FROM bl_cl.map_products mp
				WHERE mp.product_src_id = onl.medicine_id AND 
				UPPER(mp.product_name) = UPPER(medicine) AND
				mp.source_table = 'src_pharm_online_sales' AND 
				mp.source_system = 'sa_online'
				)
		AND medicine_id IS NOT NULL
		GROUP BY medicine_id, medicine, subcategory, cat_id, category
		ORDER BY prod -- ordering is crucial here
			)
		SELECT 
			UPPER(src.prod)								AS product_name,
			src.prod									AS product_src_name, 
			src.prod_id									AS product_src_id,
			src.subcat									AS product_src_subcategory,
			src.cat										AS product_src_category,
			(REGEXP_MATCHES(UPPER(src.prod), 
				'SOLUTION|TABLET|CAPSULE|INJECTION|CREAM|SUSPENSION|INHALER|NASAL SPRAY|OINTMENT|GEL'))[1]	AS product_form,
			(REGEXP_MATCHES(UPPER(src.prod), 'MG|ML|G'))[1]													AS unit_mass_measurement,
			(REGEXP_MATCHES(UPPER(src.prod), '\d+'))[1]::NUMERIC(7, 2)										AS unit_mass,
			(REGEXP_MATCH(RTRIM(UPPER(src.prod), ' PCS| G'), '(\d+)$'))[1]::INT								AS units_per_package,
			src.tab																							AS source_table,
			src.syst																						AS source_system,
			src.sales_dt																					AS start_dt
		FROM src ;
	
BEGIN 
	time_start := CLOCK_TIMESTAMP();
	
	SELECT COALESCE(MAX(product_id), 0) 
	INTO max_id
	FROM bl_cl.map_products;

	SELECT COUNT(*)
	INTO rows_ins_before
	FROM bl_cl.map_products;

	BEGIN
	FOR rec IN bound_cur
	LOOP 
		/* Row by row:
		 * if there is already id for such product - get it,
		 * if no - assing new (max_id from products + 1).
		 * Compare either by source triplet (if same source),
		 * either by names of products, subcategories and categories. 
		 * Ordering in cursor ensure that all products with same name are inserted (all sources).
		 * And it gives as the possibility to extract the same id for same product version. */
		SELECT DISTINCT mp.product_id
		INTO current_id
		FROM bl_cl.map_products mp
		WHERE 
		mp.product_src_id = rec.product_src_id AND 
		rec.source_table = mp.source_table AND 
		rec.source_system = mp.source_system
		OR 
		UPPER(rec.product_name) = UPPER(mp.product_name) AND 
		-- subcategories are in plural/singular, should be uniformed when comparing
		INITCAP(REGEXP_REPLACE(TRIM(rec.product_src_subcategory), '(-class|class|s)$', '', 'g')) = INITCAP(REGEXP_REPLACE(TRIM(mp.product_src_subcategory), '(-class|class|s)$', '', 'g')) AND
		UPPER(rec.product_src_category) = UPPER(mp.product_src_category);
	
		IF current_id IS NULL 
			THEN max_id := max_id + 1;
		END IF;

		INSERT INTO bl_cl.map_products
		VALUES (
		COALESCE(current_id, max_id), 
		rec.product_name, 
		rec.product_src_name, 
		rec.product_src_id, 
		rec.product_src_subcategory, 
		rec.product_src_category, 
		rec.product_form, 
		rec.unit_mass_measurement, 
		rec.unit_mass, 
		rec.units_per_package, 
		rec.source_table, 
		rec.source_system,
		rec.start_dt
		); 
	END LOOP;
	

	-- correct manually measures for single unit product
	UPDATE bl_cl.map_products
	SET units_per_package = 1
	WHERE units_per_package IS NULL
	OR product_form IN ('CREAM', 'SOLUTION', 'INHALER', 'NASAL SPRAY', 'SUSPENSION');
	
	-- handle single NULLs that occured when extracting unit_mass_measurements from names
	UPDATE bl_cl.map_products
	SET unit_mass_measurement = 'n.a', unit_mass= -1
	WHERE unit_mass_measurement IS NULL;
	
	-- exception during load that should be logged (if any)
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE '%: %', SQLSTATE, SQLERRM;
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
	END;
	
	SELECT COUNT(*)
	INTO rows_ins_after
	FROM bl_cl.map_products;

	time_end := CLOCK_TIMESTAMP();
	run_time_sec := EXTRACT (EPOCH FROM (time_end - time_start));

	CALL bl_cl.prc_load_logs(
			prcd,
			schema_n,
			time_start,
			time_end,
			run_time_sec,
			rows_ins_after - rows_ins_before,
			0,
			er_flag,
			er_code,
			er_msg
			);
		
	-- other exceptions that can occur during last 4 actions
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE '%: %', SQLSTATE, SQLERRM;
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
	COMMIT;
END; $$;


--CALL bl_cl.prc_load_map_products();
--SELECT * FROM bl_cl.logs where procedure_name = 'prc_load_map_products' order by start_time desc;
--SELECT * FROM bl_cl.map_products;

--SELECT * 
--FROM bl_cl.map_products 
--WHERE UPPER(product_name) LIKE 'MORPHINE%' or UPPER(product_name) LIKE 'CISAPRIDE%'
--ORDER BY 1, 2

COMMIT;
