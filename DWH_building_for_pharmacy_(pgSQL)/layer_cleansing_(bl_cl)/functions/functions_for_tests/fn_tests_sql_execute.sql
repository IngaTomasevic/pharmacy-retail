/* Function that returns result from execution all tests queries from bl_cl.test_sql table */

--DROP FUNCTION IF EXISTS bl_cl.fn_tests_sql_execute;
CREATE OR REPLACE FUNCTION bl_cl.fn_tests_sql_execute(OUT the_test_name VARCHAR(50), OUT the_test_result INT)
RETURNS SETOF record 
LANGUAGE plpgsql
AS $$
DECLARE 
	test_row record;
	test_res INT;
BEGIN 
	FOR test_row IN (
					SELECT test_name, test_sql
					FROM bl_cl.tests_sql
					) 
	LOOP 
		EXECUTE test_row.test_sql INTO test_res;
		the_test_name := test_row.test_name;
		the_test_result := test_res;
		RETURN NEXT;
	END LOOP;
END; $$;
--SELECT * FROM bl_cl.fn_tests_sql_execute();