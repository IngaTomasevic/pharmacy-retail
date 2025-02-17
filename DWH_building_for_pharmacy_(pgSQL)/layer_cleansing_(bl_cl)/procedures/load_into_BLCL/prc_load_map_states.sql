--________________________ MAPPING: STATES _______________________________________
/* Bring states to appropriate format and map them. After analyzing sources it was
 * clarified, that in sources they are named in shortened form like 'il', 'fl', 'ca', or 'n'y',
 * or in full form. Although only 5 states are present in sources for now, take
 * into account all existed USA states and add them into map table when they occur in the
 * business. */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_map_states()
LANGUAGE plpgsql
AS $$
DECLARE
	max_id INT;
	rows_before INT;
	rows_after INT;
	schema_n VARCHAR(10) := 'bl_cl';
	prcd VARCHAR(50) := 'prc_load_map_states';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	run_time_sec NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN
	time_start := CLOCK_TIMESTAMP();

	SELECT COUNT(*)
	INTO rows_before
	FROM  bl_cl.map_states;

	-- needed for assigning next IDs for mapping
	SELECT COALESCE(MAX(state_id), 0)
	INTO max_id
	FROM bl_cl.map_states;

	BEGIN
	WITH src AS (
		SELECT
			cust_state AS state,
			'src_pharm_online_sales' AS tab,
			'sa_online' AS syst
		FROM sa_online.src_pharm_online_sales onl
		WHERE cust_state IS NOT NULL
		GROUP BY cust_state
		UNION ALL
		SELECT
			state,
			'src_pharm_offline_sales',
			'sa_offline'
		FROM sa_offline.src_pharm_offline_sales ofl
		WHERE state IS NOT NULL
		GROUP BY state
			),

	states AS (
		SELECT
			ROW_NUMBER() OVER (ORDER BY state) + max_id AS state_id,
			state
		FROM (
		SELECT REGEXP_SPLIT_TO_TABLE(
		'Alabama,Alaska,Arizona,Arkansas,California,Colorado,Connecticut,Delaware,Florida,Georgia,Hawaii,Idaho,Illinois,Indiana,Iowa,
Kansas,Kentucky,Louisiana,Maine,Maryland,Massachusetts,Michigan,Minnesota,Mississippi,Missouri,Montana,Nebraska,Nevada,New Hampshire,New Jersey
New Mexico,New York,North Carolina,North Dakota,Ohio,Oklahoma,Oregon,Pennsylvania,Rhode Island,South Carolina,South Dakota,Tennessee,Texas,
Utah,Vermont,Virginia,Washington,West Virginia,Wisconsin,Wyoming,Carolina', ',')
		AS state
		))
	MERGE INTO bl_cl.map_states mp
	USING (
		SELECT
			states.state_id AS state_id,
			states.state AS state_name,
			src.state AS state_src_name,
			src.state AS state_src_id,
			src.tab AS source_table,
			src.syst AS source_system
		FROM
		/* Use INNER JOIN to map only somehow defined states (written more or less in correct short form).
		 * If something will be written completely not recognizable,
		 * it will be default -1 state when loading into bl_3nf. */
		INNER JOIN states
		ON (LOWER(src.state) = 'ca' AND LOWER(states.state) = 'california')
		OR (LOWER(src.state) = 'fl' AND LOWER(states.state) = 'florida')
		OR (LOWER(src.state) = 'il' AND LOWER(states.state) = 'illinois')
		OR (LOWER(states.state) = 'new york' AND LOWER(src.state) = 'n.y')
		OR LOWER(src.state) = LOWER(states.state)
		) upd
	ON upd.state_src_id = mp.state_src_id
	AND upd.source_table = mp.source_table
	AND upd.source_system = mp.source_system

	WHEN MATCHED
		THEN DO NOTHING

	WHEN NOT MATCHED
		THEN INSERT
		VALUES (upd.state_id, upd.state_name, upd.state_src_name,
				upd.state_src_id, upd.source_table, upd.source_system);

	SELECT COUNT(*)
	INTO rows_after
	FROM  bl_cl.map_states;

	-- exception during load that should be logged (if any)
	EXCEPTION WHEN OTHERS THEN
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
	END;

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

--CALL bl_cl.prc_load_map_states();
--SELECT * FROM bl_cl.map_states order by 1;
--SELECT * FROM bl_cl.logs;

COMMIT;
