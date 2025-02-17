DO $$
BEGIN
	INSERT INTO bl_3nf.ce_stores
	SELECT 
		-1, 
		'n.a.', 
		'manual', 
		'manual', 
		'n.a.', 
		-1, 
		'n.a.', 
		'n.a.', 
		'n.a.', 
		'1900-01-01'::DATE, 
		-1, 
		CURRENT_DATE, 
		CURRENT_DATE;
	EXCEPTION WHEN unique_violation THEN
		RAISE NOTICE 'Skipping: %', SQLERRM;
END; $$;
--SELECT * FROM bl_3nf.ce_stores;

COMMIT;
