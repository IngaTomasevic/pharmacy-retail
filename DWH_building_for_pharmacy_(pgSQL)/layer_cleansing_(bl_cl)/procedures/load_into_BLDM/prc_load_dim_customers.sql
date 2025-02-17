-- _____________________________________________ DIM_CUSTOMERS LOAD PROCEDURE ____________________________________
/* DIM_CUSTOMERS are inplemented as SCD1 (owerwrite) */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_dim_customers()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_dm';
	prcd VARCHAR(50) := 'prc_load_dim_customers';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	run_time_sec NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN
	time_start := CLOCK_TIMESTAMP();

	SELECT f.count_ins, f.count_upd
	INTO rows_ins_before, rows_upd_before
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_dm.dim_customers') f;

	BEGIN
		MERGE INTO bl_dm.dim_customers dim
		USING (
		SELECT
			ce_cust.customer_id										AS customer_id,
			COALESCE(ce_cust.customer_first_name, 'n.a.')			AS customer_first_name,
			COALESCE(ce_cust.customer_last_name, 'n.a.')			AS customer_last_name,
			COALESCE(ce_cust.customer_phone_num, 'n.a.')			AS customer_phone_num,
			COALESCE(ce_cust.customer_email, 'n.a.')				AS customer_email,
			COALESCE(ce_cust.customer_gender, 'n.a.')				AS customer_gender,
			COALESCE(ce_cust.customer_birth_dt, '1900-01-01'::DATE) AS customer_birth_dt,
			COALESCE(ce_cust.account_reg_dt, '1900-01-01'::DATE)	AS account_reg_dt,
			COALESCE(ce_adr.address_id, -1)							AS address_id,
			COALESCE(ce_adr.address_descr, 'n.a.')					AS address_descr,
			COALESCE(ce_adr.zip_code, 'n.a.')						AS zip_code,
			COALESCE(ce_c.city_id, -1)								AS city_id,
			COALESCE(ce_c.city_name, 'n.a.')						AS city_name,
			COALESCE(ce_s.state_id, -1)								AS state_id,
			COALESCE(ce_s.state_name, 'n.a.')						AS state_name
		FROM bl_3nf.ce_customers ce_cust

		LEFT OUTER JOIN bl_3nf.ce_addresses ce_adr
		ON ce_cust.customer_address_id = ce_adr.address_id

		LEFT OUTER JOIN bl_3nf.ce_cities ce_c
		ON ce_adr.city_id = ce_c.city_id

		LEFT OUTER JOIN bl_3nf.ce_states ce_s
		ON ce_c.state_id = ce_s.state_id

		WHERE customer_id != -1
			) ce

		ON dim.customer_src_id = ce.customer_id::VARCHAR
		AND dim.source_table = 'ce_customers'
		AND dim.source_system = 'bl_3nf'

		-- not all attributes have to be updated by SCD1, select only required
		WHEN MATCHED AND (
		dim.customer_first_name != ce.customer_first_name OR
		dim.customer_last_name != ce.customer_last_name OR
		dim.customer_phone_num != ce.customer_phone_num OR
		dim.customer_address_id != ce.address_id
		)	THEN UPDATE SET
				customer_first_name = ce.customer_first_name,
				customer_last_name = ce.customer_last_name,
				customer_phone_num = ce.customer_phone_num,
				customer_address_id = ce.address_id,
				ta_update_dt = CURRENT_DATE

		WHEN NOT MATCHED
			THEN INSERT VALUES (
				NEXTVAL('bl_dm.bl_dm_seq_customer_surr_id'),
				ce.customer_id,
				'bl_3nf',
				'ce_customers',
				ce.customer_first_name,
				ce.customer_last_name,
				ce.customer_phone_num,
				ce.customer_email,
				ce.customer_gender,
				ce.customer_birth_dt,
				ce.account_reg_dt,
				ce.address_id,
				ce.address_descr,
				ce.zip_code,
				ce.city_id,
				ce.city_name,
				ce.state_id,
				ce.state_name,
				CURRENT_DATE,
				CURRENT_DATE
				);
			
		-- exception during load that should be logged (if any)
		EXCEPTION WHEN OTHERS THEN
			er_flag := 'Y';
			er_code := SQLSTATE::VARCHAR(15);
			er_msg := SQLERRM::VARCHAR(300);
	END;

	time_end := CLOCK_TIMESTAMP();
	run_time_sec := EXTRACT (EPOCH FROM (time_end - time_start));

	/* Since new inserted rows have the same update_dt as current_date,
	 * to extract count of only updated rows we must substract difference
	 * of inserted rows (before-after) from count of updated rows after. */
	SELECT f.count_ins, f.count_upd - (f.count_ins - rows_ins_before)
	INTO rows_ins_after, rows_upd_after
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_dm.dim_customers') f;

	CALL bl_cl.prc_load_logs(
			prcd,
			schema_n,
			time_start,
			time_end,
			run_time_sec,
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


--DELETE FROM bl_dm.dim_customers WHERE customer_surr_id != -1;
--CALL bl_cl.prc_load_dim_customers();
--SELECT * FROM bl_dm.dim_customers ORDER BY customer_surr_id;
--SELECT * FROM bl_cl.logs;

COMMIT;
