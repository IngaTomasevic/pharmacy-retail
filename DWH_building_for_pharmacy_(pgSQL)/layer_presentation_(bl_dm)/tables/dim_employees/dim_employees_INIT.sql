DO $$
BEGIN
	INSERT INTO bl_dm.dim_employees
	SELECT 
		-1, 
		'n.a.', 
		'manual', 
		'manual', 
		'n.a.', 
		'n.a.', 
		'1900-01-01'::DATE,
		'n.a.', 
		'n.a.', 
		'n.a.', 
		'n.a.', 
		CURRENT_DATE, 
		CURRENT_DATE;
	EXCEPTION WHEN unique_violation THEN
		RAISE NOTICE 'Skipping: %', SQLERRM;
END; $$;
--SELECT * FROM bl_dm.dim_employees;
		
COMMIT; 
