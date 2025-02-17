--________________________ PROFILING: DATA TYPES MISMATCH _______________________________________
/* Function counts data type mismatches in the columns of the given table */

CREATE OR REPLACE FUNCTION bl_cl.fn_screen_data_types(
			table_n TEXT, column_n TEXT, data_type TEXT
			)
RETURNS VOID 
LANGUAGE plpgsql 
AS $$
DECLARE
	rows_count INT := 0;
BEGIN 
	EXECUTE 
	'SELECT count('||column_n||'::'||data_type||') 
	FROM '||table_n 
	INTO rows_count; 
	RAISE NOTICE '% appropriate rows in %.%', rows_count, table_n, column_n;
	
	EXCEPTION WHEN data_exception THEN 
		RAISE NOTICE 'Data mismatches in %.%', table_n, column_n;
END; $$;

COMMIT;