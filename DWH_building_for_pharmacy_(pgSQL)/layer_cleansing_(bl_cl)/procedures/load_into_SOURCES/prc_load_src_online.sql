--______________________________ PROCEDURE: LOAD FROM CSV into SRC TABLE _____________________________________
/* Suits both full and incremental loading. Incremental loading is maintained by FILTERING using mta table.
 * Inserts data from csv files into source online table ('src_...') */

--DROP PROCEDURE IF EXISTS bl_cl.prc_load_src_online;

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_src_online()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_onl INT := 0;
	last_dt_onl TIMESTAMP;
	prcd VARCHAR(50) := 'prc_load_src_online';
	schema_name VARCHAR(10) := 'sa_online';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	run_time_sec NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN
	BEGIN 
	time_start = CLOCK_TIMESTAMP();
	END; 

	/* Perform filtering by the last_event_dt in the mta tables, that store
	 * last load date of each source, last loaded sales timestamp and number of loaded rows */
	BEGIN
	WITH ins_onl AS (
			INSERT INTO sa_online.src_pharm_online_sales
			SELECT 
				receipt_number,
				"date",
				"time",
				cust_id,
				cust_full_name,
				cust_phone,
				cust_email,
				cust_gender,
				cust_birthdate,
				user_registration,
				cust_address_id,
				cust_city,
				cust_state,
				cust_postal_code,
				cust_street_num,
				cust_street_name,
				cust_build_num,
				promo_distr_id,
				promo_distr,
				promo_type_id,
				promo_type,
				promo_id,
				promo,
				promo_discount,
				medicine_id,
				medicine,
				cat_id,
				category,
				subcategory,
				brand_id,
				brand,
				supplier_id,
				supplier,
				supplier_phone,
				supplier_email,
				"cost",
				price,
				quantity,
				sales_amount, 
				-- additional created technical column ta_date_time(TIMESTAMP) for indexing, that increase prformance
				"date"::DATE + COALESCE(REPLACE("time", 'time', '00:00:00'), '00:00:00')::TIME
			FROM sa_online.ext_pharm_online_sales
			WHERE ("date"|| ' ' ||
					COALESCE(REPLACE("time", 'time', '00:00:00'), '00:00:00'))::TIMESTAMP > 
											(
											-- filter by last event_dt from MTA table
											SELECT COALESCE(MAX(last_event_dt), '1900-01-01'::DATE)
											FROM bl_cl.mta_last_load_sa_online
											)
			RETURNING ta_date_time
		)
		SELECT MAX(ta_date_time), COUNT(*)
		INTO last_dt_onl, rows_ins_onl
		FROM ins_onl;

	IF last_dt_onl IS NOT NULL THEN
		INSERT INTO bl_cl.mta_last_load_sa_online(last_event_dt, rows_number)
		VALUES (last_dt_onl, rows_ins_onl);
	END IF;

	-- exception that should be logged
	EXCEPTION WHEN OTHERS THEN
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
		RAISE NOTICE '%: %', SQLSTATE, SQLERRM;
	END; 
	
	time_end := CLOCK_TIMESTAMP();
	run_time_sec := EXTRACT (EPOCH FROM (time_end - time_start));

	CALL bl_cl.prc_load_logs(
			prcd,
			schema_name,
			time_start,
			time_end,
			run_time_sec,
			rows_ins_onl,
			0, -- no update actions in this procedure
			er_flag,
			er_code,
			er_msg
			);
		
	-- exception that can occur during last 3 actions
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE '%: %', SQLSTATE, SQLERRM;
	COMMIT;
END; $$;


--CALL bl_cl.prc_load_src_online();
--SELECT count(*) FROM sa_online.src_pharm_online_sales;
--SELECT * FROM bl_cl.logs order by start_time desc;
--SELECT * FROM bl_cl.mta_last_load_sa_online;

COMMIT; 
