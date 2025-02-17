-- ________________________________________ DIM_TIME_DAY _______________________________________________
-- DEFAULT row
DO $$
BEGIN	
	INSERT INTO bl_dm.dim_time_day
	VALUES (
			19000101,
			'1900-01-01'::DATE,
			'n.a',
			-1, 
			-1, 
			-1,
			-1,
			-1, 
			'1900-01-01'::DATE,
			-1, 
			'n.a',
			-1, 
			'1900-01-01'::DATE,
			'n.a',
			-1, 
			'1900-01-01'::DATE,
			'n.a',
			-1,
			-1,
			'1900-01-01'::DATE,
			CURRENT_DATE
			);
		
	EXCEPTION WHEN unique_violation THEN 
		RAISE NOTICE 'Skipping: %', SQLERRM USING ERRCODE = SQLSTATE;
END;
$$;
--SELECT * FROM bl_dm.dim_time_day;





-- all other dates using function created to generate dates data accroding to the specified range
DO $$
BEGIN 
	INSERT INTO bl_dm.dim_time_day
	SELECT * FROM bl_cl.fn_make_dates('2022-01-01'::DATE, '2030-12-31'::DATE);

	EXCEPTION WHEN unique_violation THEN 
		RAISE NOTICE 'Skipping: %', SQLERRM USING ERRCODE = SQLSTATE;
END; $$;
--SELECT * FROM bl_dm.dim_time_day;

COMMIT;
