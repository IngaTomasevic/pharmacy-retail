--________________________________________ FACT TABLE LOADING BL_DM __________________________________
/* THE ROLLING WINDOW is put as a parameter into procedure or DEFAULT 2 month interval is used.  */


CREATE OR REPLACE PROCEDURE bl_cl.prc_load_fct_sales_dd(rolling_period INTERVAL DEFAULT '2 month'::INTERVAL)
LANGUAGE plpgsql
AS $f$
DECLARE
	/* Variables for logging and dynamic execution */
	rows_before INT;
	rows_after INT;
	last_load DATE;
	part_num VARCHAR(7);
	part_start DATE;
	part_end DATE;
	last_sales_date DATE;
	schema_n VARCHAR := 'bl_dm';
	prcd VARCHAR(50) := 'prc_load_fct_sales_dd';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	time_run NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
	rec record;
BEGIN
	time_start := CLOCK_TIMESTAMP();

	SELECT COUNT(*)
	INTO rows_before
	FROM bl_dm.fct_sales_dd;

	SELECT COALESCE(MAX(last_sales_dt) , '2022-01-01'::DATE)::DATE
	INTO last_load
	FROM bl_cl.mta_last_load_fct_sales_dd;

	/* OPTIMIZATIONS: 
	 * Avoid join order comparisons, the best join order is analyzed before and specified manualy. 
	 * Fact table can be set UNLOGGED during loading, but it didn't show any performance difference and was decided 
	 * not to inlcude in this project, becuse moreover it requires owning privileges for admin to ALTER table. 
	 * Maybe for huge project this UNLOGGEd option can bring more benefit */

	SET join_collapse_limit TO 1;
--	ALTER TABLE bl_dm.fct_sales_dd SET UNLOGGED;


	/* The begin-end subblocks are created each time, when exception should be handled separately 
	 * for the part of code and than go further to next statements. */

	/* Loop through all month in the period from (last_load - rolling_period) to the current_date,
	 * manipulate partitions, insert data accordingly */
	FOR rec IN
	SELECT
		EXTRACT(YEAR FROM x)									AS y,
		EXTRACT(MONTH  FROM x)									AS m,
		(DATE_TRUNC('month', x))::DATE							AS cur_month,
		(DATE_TRUNC('month', (x +  INTERVAL '1 month')))::DATE	AS next_month
	-- define all month in the range from where rolling period starts to the end of current month
	FROM GENERATE_SERIES((last_load - rolling_period), (CURRENT_DATE + INTERVAL '1 month'), INTERVAL '1 month') AS m(x)

	LOOP
		BEGIN
		part_num :=  rec.y||'_'||LPAD(rec.m::VARCHAR, 2, '0'); -- specify the suffix of the partition name ('2022_02')
		part_start := rec.cur_month; -- specify the start of the partition 
		part_end := rec.next_month; -- specify the start of the NEXT partition (or the end of the current one)

		EXECUTE
		'CREATE TABLE IF NOT EXISTS bl_dm.part_fct_sales_'||part_num||'
		(LIKE bl_dm.fct_sales_dd INCLUDING DEFAULTS INCLUDING CONSTRAINTS)';

		/* Range values are not dates, but meaningful integers, that is PK of dim_time_day. 
		 * For instance '20220101'. Theses integers are ranged well, and are softer than dates. 
		 * NO need to create dates for partitions ranging . */
		EXECUTE
		'ALTER TABLE IF EXISTS bl_dm.part_fct_sales_'||part_num||'
		ADD CONSTRAINT "chk_sales_partition_range_'||part_num||'" CHECK
		(time_day_id BETWEEN '|| REPLACE(rec.cur_month::VARCHAR, '-', '') ||' AND ' || REPLACE(rec.next_month::VARCHAR, '-', '') ||')';

		EXCEPTION WHEN duplicate_object
			THEN RAISE NOTICE '%', SQLERRM;
		END;

	
		BEGIN
		EXECUTE
		'ALTER TABLE bl_dm.fct_sales_dd DETACH PARTITION bl_dm.part_fct_sales_'||part_num;

		EXCEPTION WHEN OTHERS THEN
			RAISE NOTICE '%', SQLERRM;
		END;

		/* bl_3nf contains only part of facts, other are calculated here, using simple mathematical operators to extract for instance
		 * regular_unit_price, profit, discount amount and others.. When fact is non-identified (NULL) - leave as it is. except 
		 * quantity (set 1 if NULL). Aggregate calculations works well with NULLs and put 0 instead of NULL is a mistake, 
		 * because 0 has different meaning (100% discount, 0 dollars). Non-identified is just NULL. When there is a possibility to 
		 * encaunter 0 division error, specify explicitly NULL for such CASE. */

		BEGIN
		EXECUTE FORMAT (
		$$ WITH nf_3 AS (
				SELECT
					COALESCE(dim_date.time_day_id, -1)																	AS time_day_id,
					COALESCE(dim_time.time_of_day_surr_id)																AS time_of_day_surr_id,
					COALESCE(dim_prod.product_surr_id, -1)																AS product_surr_id,
					COALESCE(dim_supl.supplier_surr_id, -1)																AS supplier_surr_id,
					COALESCE(dim_emp.employee_surr_id, -1)																AS employee_surr_id,
					COALESCE(dim_cust.customer_surr_id, -1)																AS customer_surr_id,
					COALESCE(dim_stores.store_surr_id, -1)																AS store_surr_id,
					COALESCE(dim_pm.payment_method_surr_id, -1)															AS payment_method_surr_id,
					COALESCE(dim_promo.promo_surr_id, -1)																AS promo_surr_id,
					COALESCE(dim_sch.sales_channel_surr_id, -1)															AS sales_channel_surr_id,

					
					
					sls.unit_cost_dollar_amount																					AS fct_unit_cost_dollar_amount,
					CASE WHEN dim_promo.promo_discount = 100 THEN NULL
					ELSE (sls.sales_dollar_amount / sls.sales_quantity) / (1 - COALESCE(dim_promo.promo_discount, 0) / 100) END AS fct_regular_unit_dollar_price,

					sls.sales_dollar_amount / sls.sales_quantity																AS fct_discount_unit_dollar_price,
					COALESCE(sls.sales_quantity, 1)																				AS fct_sales_quantity,
					sls.unit_cost_dollar_amount * sls.sales_quantity															AS fct_extended_cost_dollar_amount,

					CASE WHEN dim_promo.promo_discount = 100 THEN NULL
					ELSE sls.sales_dollar_amount / (1 - dim_promo.promo_discount / 100) - sls.sales_dollar_amount END			AS fct_extended_discount_dollar_amount,

					sls.sales_dollar_amount																						AS fct_extended_sales_dollar_amount,
					sls.sales_dollar_amount - (sls.unit_cost_dollar_amount * sls.sales_quantity)								AS fct_profit_dollar_amount


				FROM (
					SELECT *
					FROM bl_3nf.ce_sales
					WHERE event_dt >= $1 AND event_dt < $2
					) sls
				
				LEFT OUTER JOIN bl_dm.dim_time_of_day dim_time
				ON COALESCE(sls.sales_time, '00:00:00'::TIME) = dim_time.time_of_day

				LEFT OUTER JOIN bl_dm.dim_time_day dim_date
				ON REPLACE(sls.event_dt::VARCHAR, '-', '')::INT = dim_date.time_day_id

				LEFT OUTER JOIN bl_dm.dim_products_scd dim_prod
				ON sls.product_id::VARCHAR = dim_prod.product_src_id AND
				dim_prod.source_system = 'bl_3nf' AND
				dim_prod.source_table = 'ce_products_scd' AND
				sls.event_dt BETWEEN dim_prod.start_dt AND dim_prod.end_dt

				LEFT OUTER JOIN bl_dm.dim_customers dim_cust
				ON sls.customer_id::VARCHAR = dim_cust.customer_src_id AND
				dim_cust.source_system = 'bl_3nf' AND
				dim_cust.source_table = 'ce_customers'

				LEFT OUTER JOIN bl_dm.dim_employees dim_emp
				ON sls.employee_id::VARCHAR = dim_emp.employee_src_id AND
				dim_emp.source_system = 'bl_3nf' AND
				dim_emp.source_table = 'ce_employees'

				LEFT OUTER JOIN bl_dm.dim_promotions dim_promo
				ON sls.promo_id::VARCHAR = dim_promo.promo_src_id AND
				dim_promo.source_system = 'bl_3nf' AND
				dim_promo.source_table = 'ce_promotions'

				LEFT OUTER JOIN bl_dm.dim_stores
				ON sls.store_id::VARCHAR = dim_stores.store_src_id AND
				dim_stores.source_system = 'bl_3nf' AND
				dim_stores.source_table = 'ce_stores'

				LEFT OUTER JOIN bl_dm.dim_suppliers dim_supl
				ON sls.supplier_id::VARCHAR = dim_supl.supplier_src_id AND
				dim_supl.source_system = 'bl_3nf' AND
				dim_supl.source_table = 'ce_suppliers'

				LEFT OUTER JOIN bl_dm.dim_sales_channels dim_sch
				ON sls.sales_channel_id::VARCHAR = dim_sch.sales_channel_src_id AND
				dim_sch.source_system = 'bl_3nf' AND
				dim_sch.source_table = 'ce_sales_channels'

				LEFT OUTER JOIN bl_dm.dim_payment_methods dim_pm
				ON sls.payment_method_id::VARCHAR = dim_pm.payment_method_src_id AND
				dim_pm.source_system = 'bl_3nf' AND
				dim_pm.source_table = 'ce_payment_methods'
				)
		/* Because we take rolling period, we should avoid duplicates in already previously loaded partitions.
		 * Except showed the best performance (MERGE and EXCEPT were compared). 
		 * Since insert_dt are diferent, take CURRENT_DATE for comparison */
		INSERT INTO bl_dm.part_fct_sales_$$||part_num||$$
		SELECT *, CURRENT_DATE FROM nf_3
		EXCEPT 
		SELECT 
			time_day_id,
			time_of_day_surr_id,
			product_surr_id,
			supplier_surr_id,
			employee_surr_id,
			customer_surr_id,
			store_surr_id,
			payment_method_surr_id,
			promo_surr_id,
			sales_channel_surr_id,
			fct_unit_cost_dollar_amount,
			fct_regular_unit_dollar_price,
			fct_discount_unit_dollar_price,
			fct_sales_quantity,
			fct_extended_cost_dollar_amount,
			fct_extended_discount_dollar_amount,
			fct_extended_sales_dollar_amount,
			fct_profit_dollar_amount,				
			CURRENT_DATE 
		FROM bl_dm.part_fct_sales_$$||part_num
		) USING part_start, part_end; -- for each partition filter appropriate dates USING INT identifier(PK ID), faster and softer, uses INDEX SCAN
		
		-- exception during load that should be logged (if any)
		EXCEPTION WHEN OTHERS THEN
			er_flag := 'Y';
			er_code := SQLSTATE::VARCHAR(15);
			er_msg := SQLERRM::VARCHAR(300);
		END;
	
	
		BEGIN 
		EXECUTE
		$$ALTER TABLE bl_dm.fct_sales_dd ATTACH PARTITION bl_dm.part_fct_sales_$$||part_num||$$
		-- range values are meaningful INT that is generated from dates digits
		FOR VALUES FROM ($$||REPLACE(part_start::VARCHAR, '-', '')::INT||$$) TO ($$||REPLACE(part_end::VARCHAR, '-', '')::INT||$$)$$ ;

		-- exception during load that should be logged (if any)
		EXCEPTION WHEN OTHERS THEN
			er_flag := 'Y';
			er_code := SQLSTATE::VARCHAR(15);
			er_msg := SQLERRM::VARCHAR(300);
		END;
	END LOOP;

--	ALTER TABLE bl_dm.fct_sales_dd SET LOGGED;

	SELECT COUNT(*)
	INTO rows_after
	FROM bl_dm.fct_sales_dd;

	SELECT MAX(TO_DATE(fct.time_day_id::VARCHAR, 'YYYYMMDD'))
	INTO last_sales_date
	FROM bl_dm.fct_sales_dd fct;


	-- insert data into mta table only if insert has happened (don't put empty runs)
	IF last_sales_date IS NOT NULL AND (rows_after - rows_before) != 0
		THEN INSERT INTO bl_cl.mta_last_load_fct_sales_dd(last_sales_dt, rows_number)
		VALUES (last_sales_date, rows_after - rows_before);
	END IF;

	time_end := CLOCK_TIMESTAMP();
	time_run := EXTRACT (EPOCH FROM (time_end - time_start));

	CALL bl_cl.prc_load_logs(
			prcd,
			schema_n,
			time_start,
			time_end,
			time_run,
			rows_after - rows_before,
			0, -- no update actions in this procedure
			er_flag,
			er_code,
			er_msg
			);
		
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Error in %, %: %', prcd, SQLSTATE, SQLERRM;
	COMMIT; 
END; $f$;


COMMIT;

--TRUNCATE bl_dm.fct_sales_dd;
--CALL bl_cl.prc_load_fct_sales_dd();
--SELECT * FROM bl_cl.logs;
--SELECT * FROM bl_cl.mta_last_load_fct_sales_dd