DO $$
BEGIN
	INSERT INTO bl_3nf.ce_states
	SELECT 
		-1, 
		'n.a.', 
		'manual', 
		'manual', 
		'n.a.', 
		CURRENT_DATE, 
		CURRENT_DATE;
	EXCEPTION WHEN unique_violation THEN
		RAISE NOTICE 'Skipping: %', SQLERRM;
END; $$;
--SELECT * FROM bl_3nf.ce_states;

COMMIT;
