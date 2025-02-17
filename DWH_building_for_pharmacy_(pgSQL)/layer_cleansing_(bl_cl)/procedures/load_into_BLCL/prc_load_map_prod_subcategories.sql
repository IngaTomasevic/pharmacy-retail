--________________________ MAPPING: PROD SUBCATEGORIES ______________________________
/* Several categories can have subcategories with same names.
 * Thus, must be mapped together (by same logic as cities and states) */ 

/* Procedure adopted for changing according to SCD1 (changes in name) */


--TRUNCATE bl_cl.map_prod_subcategories;

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_map_prod_subcategories()
LANGUAGE plpgsql 
AS $$
DECLARE 
	max_id INT;
	rows_before INT;
	rows_after INT;
	schema_n VARCHAR(10) := 'bl_cl';
	prcd VARCHAR(50) := 'prc_load_map_prod_subcategories';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	run_time_sec NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN 
	time_start := CLOCK_TIMESTAMP();
	
	SELECT COALESCE(MAX(subcategory_id), 0) 
	INTO max_id
	FROM bl_cl.map_prod_subcategories;
	
	SELECT COUNT(*)
	INTO rows_before
	FROM bl_cl.map_prod_subcategories;

	BEGIN
	WITH src AS (
			SELECT 
				subcategory AS subcat, 
				category AS cat,
				cat_id  AS cat_id, 
				'src_pharm_online_sales' AS tab, 
				'sa_online' AS syst
			FROM sa_online.src_pharm_online_sales onl
			WHERE subcategory IS NOT NULL
			GROUP BY subcategory, category, cat_id
			UNION ALL
			SELECT 
				subclass AS subcat,
				class_name AS cat, 
				class_id AS cat_id, 
				'src_pharm_offline_sales' AS tab, 
				'sa_offline' AS syst
			FROM sa_offline.src_pharm_offline_sales ofl
			WHERE subclass IS NOT NULL
			GROUP BY subclass, class_name, class_id
		)
	
	MERGE INTO bl_cl.map_prod_subcategories mp
	USING(
		SELECT
			-- assign unique key for each subcategory - category combination
			-- perform transformation (conform when plural to singular) and some cleaning
			DENSE_RANK () OVER (ORDER BY 
				INITCAP(REGEXP_REPLACE(TRIM(src.subcat), '(-class|class|s)$', '', 'g')), 
				COALESCE(mp.prod_category_id::VARCHAR, src.cat_id)
																) + max_id AS subcategory_id,
			INITCAP(REGEXP_REPLACE(TRIM(src.subcat), '(-class|class|s)$', '', 'g')) AS subcategory_name,  
			src.subcat AS subcategory_src_name,
			src.subcat AS subcategory_src_id,
			src.cat_id AS category_src_id,
			src.tab, 
			src.syst
		FROM src
	
		LEFT OUTER JOIN bl_cl.map_prod_categories mp 
		ON UPPER(mp.prod_category_src_name) = UPPER(src.cat)
		AND mp.source_table = src.tab
		AND mp.source_system = src.syst
		) upd

	ON mp.subcategory_src_id = upd.subcategory_src_id
	AND mp.category_src_id = upd.category_src_id
	AND mp.source_table = upd.tab
	AND mp.source_system = upd.syst
	
	WHEN MATCHED AND mp.subcategory_name != upd.subcategory_name
		THEN UPDATE SET subcategory_name = upd.subcategory_name
	
	WHEN NOT MATCHED 
		THEN INSERT VALUES(
		upd.subcategory_id, 
		upd.subcategory_name, 
		upd.subcategory_src_name, 
		upd.subcategory_src_id,  
		upd.category_src_id, 
		upd.tab,
		upd.syst
		);
	
	-- exception during load that should be logged (if any)
	EXCEPTION WHEN OTHERS THEN
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
	RAISE NOTICE '%', SQLERRM;
	END ;

	SELECT COUNT(*)
	INTO rows_after
	FROM bl_cl.map_prod_subcategories;

	time_end := CLOCK_TIMESTAMP();
	run_time_sec := EXTRACT (EPOCH FROM (time_end - time_start));

	CALL bl_cl.prc_load_logs(
			prcd,
			schema_n,
			time_start,
			time_end,
			run_time_sec,
			(rows_after - rows_before),
			0,
			er_flag,
			er_code,
			er_msg
			);
		
	-- other exceptions that can occur during last 4 actions
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Error during %: %', prcd, SQLERRM;
	COMMIT; 
END; $$;
	
--CALL bl_cl.prc_load_map_prod_subcategories();
--SELECT * FROM bl_cl.map_prod_subcategories order by 1;
--SELECT * FROM bl_cl.logs;

COMMIT;
