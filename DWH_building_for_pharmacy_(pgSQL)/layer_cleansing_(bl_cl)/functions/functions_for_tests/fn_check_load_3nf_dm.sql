--________________________________________________ FUNCTIONS: CHECK load from 3NF to DM _____________________________________
/* Functions that check the integrity of data between 3NF layer and DM. Allow detect entities (by their keys)
 * which are present on only one layer. Check, how proper and fully data has been loaded from 3NF to DM */

--DROP TYPE IF EXISTS bl_cl.match_3nf_dm;

DO $$
BEGIN
	CREATE TYPE bl_cl.match_3nf_dm AS (
		key_name_3nf VARCHAR,
		key_val_3nf VARCHAR,
		key_name_dm VARCHAR,
		key_val_dm VARCHAR
		);

	EXCEPTION WHEN duplicate_object
		THEN RAISE NOTICE 'Skipping: %', SQLERRM;
END; $$;


/* Function for single ce-dim (core entity - dimension) check
 * LOGIC: perform FULL OUTER JOIN ce and dim by id(ce) and src_id(dim),
 * filter default entities (-1), identify not corrrelated entities by NULLs */


--DROP FUNCTION IF EXISTS bl_cl.fn_check_load_3nf_dm;
CREATE OR REPLACE FUNCTION bl_cl.fn_check_load_3nf_dm(ce VARCHAR, ce_id VARCHAR, dim VARCHAR, dim_src_id VARCHAR)
RETURNS SETOF bl_cl.match_3nf_dm -- returns type created above
LANGUAGE plpgsql
AS $$
BEGIN
	RETURN query
	EXECUTE '
	SELECT
		$1::VARCHAR									AS key_name_3nf,
		ce.'||quote_ident(ce_id)||'::VARCHAR		AS key_val_3nf,
		$2::VARCHAR									AS key_name_dm,
		dim.'||quote_ident(dim_src_id)||'::VARCHAR	AS key_val_dm

	FROM ' || ce ||' ce
	FULL OUTER JOIN ' || dim ||' dim
	ON ce.'||quote_ident(ce_id)||'::VARCHAR = dim.'||quote_ident(dim_src_id)||'::VARCHAR AND
		dim.source_system = ''bl_3nf''

	WHERE(
		ce.'||quote_ident(ce_id)||'::VARCHAR IS NULL or dim.'||quote_ident(dim_src_id)||' IS NULL) AND
		ce.'||quote_ident(ce_id)||'::VARCHAR != ''-1'''
	USING ce_id, dim_src_id;

	IF NOT FOUND
		THEN RAISE NOTICE 'MATCH: % AND %', ce, dim;
	ELSE
		RAISE NOTICE 'DISMATCH: % AND %', ce, dim;
	END IF;

	EXCEPTION WHEN OTHERS
		THEN RAISE NOTICE '%: %', SQLSTATE, SQLERRM;
END; $$;

--SELECT * FROM bl_cl.fn_check_load_3nf_dm('bl_3nf.ce_customers', 'customer_id', 'bl_dm.dim_customers', 'customer_src_id');

COMMIT; 
