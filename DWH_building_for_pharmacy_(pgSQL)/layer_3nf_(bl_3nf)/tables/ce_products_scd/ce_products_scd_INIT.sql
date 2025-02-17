DO $$
BEGIN
	INSERT INTO bl_3nf.ce_products_scd
	SELECT 
		-1, 
		'n.a.', 
		'manual', 
		'manual', 
		'n.a.', 
		'n.a.', 
		'n.a.', 
		-1, 
		-1, 
		-1, 
		-1, 
		'1900-01-01'::DATE, 
		'9999-12-31'::DATE, 
		'Y', 
		CURRENT_DATE;
	EXCEPTION WHEN unique_violation THEN
		RAISE NOTICE 'Skipping: %', SQLERRM;
END; $$;
--SELECT * FROM bl_3nf.ce_products_scd;

COMMIT;
