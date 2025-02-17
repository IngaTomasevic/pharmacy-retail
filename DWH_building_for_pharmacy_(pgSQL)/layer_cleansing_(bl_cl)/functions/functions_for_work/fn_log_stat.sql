--____________________________________________________ LOG_STAT_FUNCTION_____________________________________
/* Returns the logging data of all procedures last run time. When each procedure was run last time. 
 * Useful when testing, need only filtering by the exact procedure name, or list of procedures, or schema name */

--DROP FUNCTION IF EXISTS bl_cl.fn_log_stat;

CREATE OR REPLACE FUNCTION bl_cl.fn_log_stat()
RETURNS SETOF bl_cl.logs
LANGUAGE plpgsql
AS $$
BEGIN
	RETURN query
	SELECT logs.*
	FROM bl_cl.logs logs
	INNER JOIN   (
					SELECT procedure_name, MAX(start_time) AS last_start
					FROM bl_cl.logs
					GROUP BY procedure_name
					) 
	lst ON lst.procedure_name = logs.procedure_name AND lst.last_start = logs.start_time;

	EXCEPTION WHEN OTHERS
		THEN RAISE NOTICE '%: %', SQLSTATE, SQLERRM;
	RETURN;
END; $$;


--SELECT * FROM bl_cl.fn_log_stat();

COMMIT;
