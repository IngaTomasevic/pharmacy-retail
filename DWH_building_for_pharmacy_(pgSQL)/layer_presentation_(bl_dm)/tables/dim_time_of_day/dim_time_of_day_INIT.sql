-- ________________________________________ DIM_TIME_OF_DAY _______________________________________________
/* Do not insert DEFAULT record with -1 id for non-specified time, 
 * because if time isn't determind, the time 00:00:00 should be used as default*/

	
--TRUNCATE  bl_dm.dim_time_of_day;

DO $$
BEGIN 
	INSERT INTO bl_dm.dim_time_of_day
	SELECT 
		NEXTVAL('bl_dm.bl_dm_seq_time_of_day_surr_id'),
		sec,
		EXTRACT (HOUR FROM sec),
		TO_CHAR (sec, 'HH12 AM'), 
		EXTRACT (MINUTE FROM sec),
		EXTRACT (SECOND FROM sec),
		CURRENT_DATE 
	-- subquery for generating seconds (possible only from timestamp) -> 
	-- than convert it to TIME TYPE
	FROM (
		SELECT dttm::TIME AS sec
		FROM GENERATE_SERIES (
				'1900-01-01 00:00:00'::TIMESTAMP, 
				'1900-01-01 23:59:59'::TIMESTAMP, 
				INTERVAL '1 second'
							) AS dttm
		);
	EXCEPTION WHEN unique_violation THEN 
	RAISE NOTICE 'Skipping: %', SQLERRM USING ERRCODE = SQLSTATE;
END; $$;
--SELECT * FROM bl_dm.dim_time_of_day;


	
COMMIT;