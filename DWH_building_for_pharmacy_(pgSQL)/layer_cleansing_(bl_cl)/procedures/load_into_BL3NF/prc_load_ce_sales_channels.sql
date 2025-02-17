-- _____________________________________________ CE_SALES_CHANNELS LOAD PROCEDURE ____________________________________
/* CE_SALES_CHANNELS are implemented as SCD0 (static values, no changes), because
 * these values do not come from sources, but are created manually to identify
 * sources itself when loading into fact tables. */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_sales_channels()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins INT;
	schema_n VARCHAR(10) := 'bl_3nf';
	prcd VARCHAR(50) := 'prc_load_ce_sales_channels';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	time_run NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN
	time_start := CLOCK_TIMESTAMP();

	BEGIN
	WITH ins AS(
		INSERT INTO bl_3nf.ce_sales_channels(
			sales_channel_id,
			sales_channel_src_id,
			source_system,
			source_table,
			sales_channel_name,
			ta_insert_dt,
			ta_update_dt
		)
		SELECT
			NEXTVAL('bl_3nf.bl_3nf_seq_sales_channel_id'),
			'n.a.',
			'manual',
			'manual',
			'offline',
			CURRENT_DATE,
			CURRENT_DATE
		WHERE 'offline' NOT IN (
							SELECT LOWER(sales_channel_name)
							FROM bl_3nf.ce_sales_channels
							)
		UNION ALL
			SELECT
			NEXTVAL('bl_3nf.bl_3nf_seq_sales_channel_id'),
			'n.a.',
			'manual',
			'manual',
			'online',
			CURRENT_DATE,
			CURRENT_DATE
		WHERE 'online' NOT IN (
							SELECT LOWER(sales_channel_name)
							FROM bl_3nf.ce_sales_channels
							)
		RETURNING sales_channel_id
	)
	SELECT COUNT(*)
	INTO rows_ins
	FROM ins;

	-- exception during load that should be logged (if any)
	EXCEPTION WHEN OTHERS THEN
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
	END ;

	time_end := CLOCK_TIMESTAMP();
	time_run := EXTRACT (EPOCH FROM (time_end - time_start));

	CALL bl_cl.prc_load_logs(
		prcd,
		schema_n,
		time_start,
		time_end,
		time_run,
		rows_ins,
		0,
		er_flag,
		er_code,
		er_msg
		);
	
	-- exception that can occur during last 3 actions
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Error in %, %: %', prcd, SQLSTATE, SQLERRM;
	COMMIT;
END; $$;


--DELETE FROM bl_3nf.ce_sales_channels WHERE sales_channel_id != -1;
--ALTER SEQUENCE bl_3nf.bl_3nf_seq_sales_channel_id RESTART WITH 1
--SELECT * FROM bl_3nf.ce_sales_channels;
--CALL bl_cl.prc_load_ce_sales_channels();
--SELECT * FROM bl_cl.logs;

COMMIT;
