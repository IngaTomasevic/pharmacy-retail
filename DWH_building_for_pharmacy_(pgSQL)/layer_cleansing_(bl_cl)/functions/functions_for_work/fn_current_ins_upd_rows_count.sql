-- _____________________________________________ INSERT/UPDATE ROWS COUNT FUNCTION ____________________________________
/* Functions that will be used in procedures for getting information for logging:
 * counting inserted and updated rows. Because majority of loadings are performed with MERGE, 
 * and there is no possibility to use RETURNING with MERGE, it will be counted
 * at the beginning of loading procedure and at the end. 
 * 
 * Since inserted rows get the same update date as inserted date (CURRENT DATE), 
 * at the end of procedure the real rows_updated will be counted as difference
 * (rows updated_after - (rows inserted_before - rows inserted_after))
 * to exclude those rows that were inserted and receive the update_dt = CURRENT_DATE. */

CREATE OR REPLACE FUNCTION bl_cl.fn_current_ins_upd_rows_count (
	IN tabname TEXT, OUT count_ins INT, OUT count_upd INT
	)
LANGUAGE plpgsql
AS $$
BEGIN
	EXECUTE
	'SELECT COUNT(*)
	FROM '||tabname
	INTO count_ins;

	EXECUTE
	'SELECT COUNT(*)
	FROM '||tabname||'
	WHERE ta_update_dt = CURRENT_DATE'
	INTO count_upd;
END; $$;

COMMIT;
