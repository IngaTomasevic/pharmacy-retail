--______________________________ PROCEDURE: LOAD FROM CSV into SRC TABLE _____________________________________
/* Suits both full and incremental loading. Incremental loading is maintained by FILTERING using mta table.
 * Inserts data from csv files into offline source tables ('src_...') */

--DROP PROCEDURE IF EXISTS bl_cl.prc_load_src_offline;

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_src_offline()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_ofl INT := 0;
	last_dt_ofl TIMESTAMP;
	prcd VARCHAR(50) := 'prc_load_src_offline';
	schema_name VARCHAR(10) := 'sa_offline';
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
	WITH ins_ofl AS (
			INSERT INTO sa_offline.src_pharm_offline_sales
			-- use excplicit select columns, because columns order can change
			SELECT 
				invoice,
				"day",
				"time",
				empl_id,
				empl_full_name,
				empl_date_of_birth,
				empl_phone,
				empl_gender,
				empl_email,
				"role",
				pharmacy_id,
				pharmacy,
				address_id,
				city,
				state,
				postal_code,
				street_num,
				street,
				build_num,
				pharmacy_phone,
				pharmacy_email,
				registration_date,
				floor_space,
				payment_type,
				promotion_channel_id,
				promotion_channel,
				promotion_subcategory_id,
				promotion_subcategory,
				promotion_id,
				promotion,
				discount,
				prod_id,
				prod_name,
				prod_descr,
				class_id,
				class_name,
				class_descr,
				subclass,
				subclass_descr,
				brand_id,
				brand_name,
				supplier_id,
				supplier,
				supplier_phone,
				supplier_email,
				unit_cost,
				unit_price,
				quantity,
				final_sales_amount,
				-- additional created technical column ta_date_time(TIMESTAMP) for indexing, that increase prformance
				TO_DATE("day", 'YYYY-MM-DD') + COALESCE(REPLACE("time", 'time', '00:00:00'), '00:00:00')::TIME
			FROM sa_offline.ext_pharm_offline_sales
			WHERE ("day" || ' ' || COALESCE(REPLACE("time", 'time', '00:00:00'), '00:00:00'))::TIMESTAMP > (
														-- filter by the last loaded event_dt stored in mta table
														SELECT COALESCE(max(last_event_dt), '1900-01-01'::DATE)
														FROM bl_cl.mta_last_load_sa_offline
														)
				RETURNING ta_date_time
		)
		SELECT MAX(ta_date_time), COUNT(*)
		INTO last_dt_ofl, rows_ins_ofl
		FROM ins_ofl;

	-- if some data loaded, write data into mta table
	IF last_dt_ofl IS NOT NULL THEN
		INSERT INTO bl_cl.mta_last_load_sa_offline(last_event_dt, rows_number)
		VALUES (last_dt_ofl, rows_ins_ofl);
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
			rows_ins_ofl,
			0, -- no update actions in this procedure
			er_flag,
			er_code, 
			er_msg
			);
		
	-- exception that can occur during last 4 actions
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE '%: %', SQLSTATE, SQLERRM;
	COMMIT;
END; $$;


--CALL bl_cl.prc_load_src_offline();
--SELECT count(*) FROM sa_offline.src_pharm_offline_sales;
--SELECT * FROM bl_cl.logs order by start_time desc;
--SELECT * FROM bl_cl.mta_last_load_sa_offline;

COMMIT; 
