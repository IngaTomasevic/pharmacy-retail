-- _____________________________________________ CE_STORES LOAD PROCEDURE ____________________________________
/* CE_STORES is implemented as SCD1. Stores are present in one (offline) source channel */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_stores()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_3nf';
	prcd VARCHAR(50) := 'prc_load_ce_stores';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	time_run NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN
	time_start := CLOCK_TIMESTAMP();

	SELECT f.count_ins, f.count_upd
	INTO rows_ins_before, rows_upd_before
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_stores') f;

	BEGIN
		MERGE INTO bl_3nf.ce_stores ce
		USING (
			SELECT
				COALESCE(src.pharmacy_id, src.pharmacy, src.pharmacy_phone, src.pharmacy_email) AS nk,
				'sa_offline' AS syst,
				'src_pharm_offline_sales' AS tab,
				COALESCE(INITCAP(src.pharmacy), 'n.a.') AS store,
				COALESCE(adr.address_id, -1) AS adr_id,
				COALESCE(src.build_num, 'n.a.') AS build,
				COALESCE(SUBSTRING(REGEXP_REPLACE(pharmacy_phone, '\D', '', 'g'), 1, 10), 'n.a.') AS phone,
				COALESCE(src.pharmacy_email, 'n.a.') AS email,
				COALESCE(TO_DATE(src.registration_date, 'YYYY-MM-DD'), '1900-01-01'::DATE) AS open_dt,
				COALESCE(src.floor_space::NUMERIC(8,2), 0) AS fl_space -- 0 in case of NULL to avoid errors in grouped calculations
			FROM (
				SELECT
					pharmacy_id,
					pharmacy,
					address_id,
					build_num,
					pharmacy_phone,
					pharmacy_email,
					registration_date,
					floor_space
				FROM sa_offline.src_pharm_offline_sales
				WHERE COALESCE(pharmacy_id, pharmacy, pharmacy_phone, pharmacy_email) IS NOT NULL
				GROUP BY
					pharmacy_id,
					pharmacy,
					address_id,
					build_num,
					pharmacy_phone,
					pharmacy_email,
					registration_date,
					floor_space
					) src
					
			LEFT OUTER JOIN bl_cl.map_addresses mp
			ON mp.address_src_id = src.address_id
			AND mp.source_table = 'src_pharm_offline_sales'
			AND mp.source_system = 'sa_offline'

			LEFT OUTER JOIN bl_3nf.ce_addresses adr
			ON adr.address_src_id = COALESCE(mp.address_id::VARCHAR, src.address_id) AND
			adr.source_table = CASE
				WHEN mp.address_id IS NOT NULL THEN 'map_addresses'
				WHEN src.address_id IS NOT NULL THEN 'src_pharm_offline_sales' END AND
			adr.source_system = CASE
				WHEN mp.address_id IS NOT NULL THEN 'bl_cl'
				WHEN src.address_id IS NOT NULL THEN 'sa_offline' END
			) upd

		ON ce.store_src_id = upd.nk
		AND ce.source_system = upd.syst
		AND ce.source_table = upd.tab

		WHEN MATCHED AND (
		ce.store_name != upd.store OR
		ce.store_address_id != upd.adr_id OR
		ce.store_build_num != upd.build OR
		ce.store_phone_num != upd.phone OR
		ce.store_email != upd.email OR
		ce.floor_space != upd.fl_space
		)	THEN UPDATE SET
			store_name = upd.store,
			store_address_id = upd.adr_id,
			store_build_num = upd.build,
			store_phone_num = upd.phone,
			store_email = upd.email,
			floor_space = upd.fl_space,
			ta_update_dt = CURRENT_DATE

		WHEN NOT MATCHED
			THEN INSERT VALUES (
			NEXTVAL('bl_3nf.bl_3nf_seq_store_id'),
			upd.nk, upd.syst, upd.tab, upd.store, upd.adr_id, upd.build, upd.phone,
			upd.email, upd.open_dt, upd.fl_space, CURRENT_DATE, CURRENT_DATE
			);
		
		-- exception during load that should be logged (if any)
		EXCEPTION WHEN OTHERS THEN
			er_flag := 'Y';
			er_code := SQLSTATE::VARCHAR(15);
			er_msg := SQLERRM::VARCHAR(300);
	END ;

	time_end := CLOCK_TIMESTAMP();
	time_run := EXTRACT (EPOCH FROM (time_end - time_start));

	SELECT f.count_ins, f.count_upd - (f.count_ins - rows_ins_before)
	INTO rows_ins_after, rows_upd_after
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_stores') f;


	CALL bl_cl.prc_load_logs(
			prcd,
			schema_n,
			time_start,
			time_end,
			time_run,
			(rows_ins_after - rows_ins_before),
			(rows_upd_after - rows_upd_before),
			er_flag,
			er_code,
			er_msg
			);
		
	
	-- exception that can occur during last 4 actions
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Error in %, %: %', prcd, SQLSTATE, SQLERRM;
	COMMIT;
END; $$;


--DELETE FROM bl_3nf.ce_stores WHERE STORE_ID != -1;
--ALTER SEQUENCE bl_3nf.bl_3nf_seq_store_id RESTART WITH 1
--SELECT * FROM bl_3nf.ce_stores ORDER BY store_id;
--CALL bl_cl.prc_load_ce_stores();
--SELECT * FROM bl_cl.logs;

COMMIT; 
