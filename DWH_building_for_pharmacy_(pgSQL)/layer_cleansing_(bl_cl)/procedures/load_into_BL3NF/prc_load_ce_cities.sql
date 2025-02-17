-- _____________________________________________ CE_CITIES LOAD PROCEDURE ____________________________________
/* Cities are static implemented as SCD1 */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_cities()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_3nf';
	prcd VARCHAR(50) := 'prc_load_ce_cities';
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_cities') f;

	BEGIN

	WITH src AS (
			SELECT
				city AS src_city_name,
				state AS src_state_name,
				'src_pharm_offline_sales' AS tab,
				'sa_offline' AS syst
			FROM sa_offline.src_pharm_offline_sales
			WHERE city IS NOT NULL
			GROUP BY city, state
			UNION ALL
			SELECT
				cust_city AS src_city_name,
				cust_state AS src_state_name,
				'src_pharm_online_sales' AS tab,
				'sa_online' AS syst
			FROM sa_online.src_pharm_online_sales
			WHERE cust_city IS NOT NULL
			GROUP BY cust_city, cust_state
			)
	MERGE INTO bl_3nf.ce_cities ce
	USING (
		SELECT *
		-- subquery for final ordering, filtering and grouping
		FROM (
			SELECT
				/* Source ID becomes first attribute that is not NULL:
				 * subcat ID from map table, or from src */
				COALESCE(mp.city_id::VARCHAR, src.src_city_name) AS nk,
				CASE WHEN mp.city_id IS NOT NULL THEN 'bl_cl' ELSE src.syst END AS source_system,
				CASE WHEN mp.city_id IS NOT NULL THEN 'map_cities_by_states' ELSE src.tab END AS source_table,
				COALESCE(mp.city_name, src.src_city_name, 'n.a.') AS city_name,
				COALESCE(st.state_id, -1) AS state_id
			FROM src
		
			LEFT OUTER JOIN bl_cl.map_cities_by_states mp
			ON mp.city_src_id = src.src_city_name
			AND mp.state_src_id = src.src_state_name
			AND mp.source_table = src.tab
			AND mp.source_system = src.syst
		
			LEFT OUTER JOIN bl_3nf.ce_states st
			ON st.state_src_id = COALESCE(mp.state_id::VARCHAR, src.src_state_name)
			AND st.source_table = CASE
				WHEN mp.state_id IS NOT NULL THEN 'map_states'
				ELSE src.tab END
			AND st.source_system = CASE
				WHEN mp.state_id IS NOT NULL THEN 'bl_cl'
				ELSE src.syst END
				)
		-- use grouping to avoid duplicates (grouping faster than distinct)
		GROUP BY nk, source_system, source_table, city_name, state_id
		ORDER BY city_name
	) upd
	
	ON ce.city_src_id = upd.nk
	AND ce.source_system = upd.source_system
	AND ce.source_table = upd.source_table

	WHEN MATCHED AND ce.city_name != upd.city_name
		THEN UPDATE SET 
		city_name = upd.city_name, 
		ta_update_dt = CURRENT_DATE
		
	WHEN NOT MATCHED 
		THEN INSERT VALUES(
		NEXTVAL('bl_3nf.bl_3nf_seq_city_id'),
		upd.nk,
		upd.source_system, 
		upd.source_table, 
		upd.city_name, 
		upd.state_id, 
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

	/* Since new inserted rows have the same update_dt as current_date,
	 * to extract count of only updated rows we must substract difference
	 * of inserted rows (before-after) from count of updated rows after. */
	SELECT f.count_ins, f.count_upd - (f.count_ins - rows_ins_before)
	INTO rows_ins_after, rows_upd_after
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_cities') f;

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
		RAISE NOTICE '%: %', SQLSTATE, SQLERRM;
	COMMIT;
END; $$;


--DELETE FROM bl_3nf.ce_cities WHERE city_id != -1;
--ALTER SEQUENCE bl_3nf.bl_3nf_seq_city_id RESTART WITH 1
--SELECT * FROM bl_3nf.ce_cities ORDER BY city_id;
--CALL bl_cl.prc_load_ce_cities();
--SELECT * FROM bl_cl.logs;

COMMIT;
