--________________________ FUNCTIONS FOR DATA PROFILING _______________________________________
/* function that counts NULLs IN ALL COLUMNS of the given table */
CREATE OR REPLACE FUNCTION bl_cl.fn_screen_nulls(table_n TEXT, schema_n TEXT)
RETURNS VOID 
LANGUAGE plpgsql 
AS $$
DECLARE
	col TEXT;
	nulls_count INT := 0;
BEGIN 
	FOR col IN 
		SELECT column_name AS col
		FROM information_schema.COLUMNS
		WHERE table_name = table_n
	LOOP 
		EXECUTE 
		'SELECT count(*) 
		FROM (
			SELECT '||col||'
			FROM '||schema_n||'.'||table_n||'
			WHERE '||col||' IS NULL
			)' INTO nulls_count; 
		IF nulls_count > 0 THEN 
			RAISE NOTICE '% nulls in % (%)', nulls_count, col, table_n;
		END IF;	
	END LOOP;
END; 
$$;

COMMIT; 