-- _____________________________________________ CE_CUSTOMERS LOAD PROCEDURE ____________________________________
/* CE_CUSTOMERS are implemented as SCD1 (owerwrite). Customers are present only in online channel source. */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_customers()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_3nf';
	prcd VARCHAR(50) := 'prc_load_ce_customers';
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
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_customers') f;

	BEGIN
		WITH src AS (
		SELECT
			cust_id,
			cust_full_name,
			cust_phone,
			cust_email,
			cust_gender,
			cust_birthdate,
			user_registration,
			cust_address_id,
			cust_city,
			cust_postal_code
		FROM sa_online.src_pharm_online_sales
		/* Filter entities for which at least one attribute is present for unique
		 * identifying, otherwise it will be default entity when loading into fact */
		WHERE COALESCE(cust_id, cust_email) IS NOT NULL 
		GROUP BY
			cust_id,
			cust_full_name,
			cust_phone,
			cust_email,
			cust_gender,
			cust_birthdate,
			user_registration,
			cust_address_id,
			cust_city,
			cust_postal_code
		ORDER BY cust_id::int
		)
		MERGE INTO bl_3nf.ce_customers ce
		USING (
					SELECT
						-- if customer isn't present alresdy, nk become his email, because it's unique
						COALESCE(src.cust_id, src.cust_email) AS nk,
						'sa_online' AS src,
						'src_pharm_online_sales' AS tab,
						COALESCE(SPLIT_PART(INITCAP(src.cust_full_name), ' ', 1), 'n.a.') AS f_name,
						COALESCE(SPLIT_PART(INITCAP(src.cust_full_name), ' ', 2), 'n.a.') AS l_name,
						COALESCE(SUBSTRING(REGEXP_REPLACE(regexp_replace(src.cust_phone, '\+1\-|001\-', '', 1, 0, 'i'), '\D', '', 'g'), 1, 10), 'n.a.') AS phone,
						COALESCE(LOWER(src.cust_email), 'n.a.') AS email,
						COALESCE(LOWER(src.cust_gender), 'n.a.') AS gender,
						COALESCE(TO_DATE(src.cust_birthdate, 'YYYY-MM-DD'), '1900-01-01'::DATE) AS bd,
						COALESCE(TO_DATE(src.user_registration, 'YYYY-MM-DD'), '1900-01-01'::DATE) AS reg_dt,
						COALESCE(adr.address_id, -1) AS adr_id
					FROM src
					LEFT OUTER JOIN bl_cl.map_addresses mp
					ON mp.address_src_id = src.cust_address_id AND
					mp.source_table = 'src_pharm_online_sales' AND 
					mp.source_system = 'sa_online'

					LEFT OUTER JOIN bl_3nf.ce_addresses adr
					ON adr.address_src_id = COALESCE(mp.address_id::VARCHAR, src.cust_address_id) AND
					adr.source_table = CASE
						WHEN mp.address_id IS NOT NULL THEN 'map_addresses'
						ELSE 'src_pharm_online_sales' END AND
					adr.source_system = CASE
						WHEN mp.address_id IS NOT NULL THEN 'bl_cl'
						ELSE 'sa_online' END
				) upd
				
		ON ce.customer_src_id = upd.nk
		AND ce.source_system = upd.src
		AND ce.source_table = upd.tab

		WHEN MATCHED AND (
		-- specify only attributes that should and can be changed (e.g. birthday, email -> not)
		-- because new email registered -> means new customer
		ce.customer_first_name != upd.f_name OR
		ce.customer_last_name != upd.l_name OR
		ce.customer_phone_num != upd.phone OR
		ce.customer_address_id != upd.adr_id
		)
			THEN UPDATE SET
			customer_first_name = upd.f_name,
			customer_last_name = upd.l_name,
			customer_phone_num = upd.phone,
			customer_address_id = upd.adr_id,
			ta_update_dt = CURRENT_DATE

		WHEN NOT MATCHED
			THEN INSERT VALUES(
			NEXTVAL('bl_3nf.bl_3nf_seq_customer_id'), upd.nk, upd.src, upd.tab, upd.f_name, upd.l_name,
			upd.phone, upd.email, upd.gender, upd.bd, upd.reg_dt, upd.adr_id, CURRENT_DATE, CURRENT_DATE
			);

	-- exception during load that should be logged (if any)
	EXCEPTION WHEN OTHERS THEN
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
	END ;

	time_end := CLOCK_TIMESTAMP();
	time_run := EXTRACT (EPOCH FROM (time_end - time_start));

	/* Since new inserted rows have the same update_dt as current_date,
	 * to extract count of only updated rows, we must substract difference
	 * of inserted rows (before-after) from all updated rows after. */
	SELECT f.count_ins, f.count_upd - (f.count_ins - rows_ins_before)
	INTO rows_ins_after, rows_upd_after
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_customers') f;

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


--DELETE FROM bl_3nf.ce_customers WHERE customer_id != -1;
--ALTER SEQUENCE bl_3nf.bl_3nf_seq_customer_id RESTART WITH 1
--SELECT COUNT(*) FROM bl_3nf.ce_customers ORDER BY customer_id;
--CALL bl_cl.prc_load_ce_customers();
--SELECT * FROM bl_cl.logs;

COMMIT;
