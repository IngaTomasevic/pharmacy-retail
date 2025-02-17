--________________________________________ FACT TABLE LOADING 3NF__________________________________
/* 1. Select all NKs and facts from both sources in the subquery (decrease columns number for joining).
 * 2. Perform filtering by the last loaded sales_dt stored in mta table, load only incremental dates, that are 'more than'
 * 3. Join src through map tables (if needed) with dimensions.
 * 4. SET CONSTRAINTS ALL DEFERRED -> to avoid calling triggers when loading (for increasing performance). Is more compact than deleting constraints.
 * 5. SET join_collapse limit to 1 -> to increase performance avoiding comparisons of JOIN order. It was analyzed before and the best order is already done.
 * 6. Identify the timestamp of last loaded transactions, count rows inserted and write this data into mta table (only if loading was performed, is not empty run). 
 * 7. If cost or amount is non-identified (NULL) -> leave as it is. Because 0 has different meaning (100 % discount), but non-identified should be NULL.
 */



CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_sales()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins INT;
	last_sales_timestamp TIMESTAMP;
	schema_n VARCHAR := 'bl_3nf';
	prcd VARCHAR(50) := 'prc_load_ce_sales';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	time_run NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if EXCEPTION
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN
	time_start = CLOCK_TIMESTAMP();

	/* Subblock for INSERT operation to automatically rollback in case of error, 
	 * get error data and go further to write apppripriate data into the log table */
	
	BEGIN
	/* OPTIMIZATIONS:
	 * Avoid join order comparisons, the best order is analyzed before and specified manualy.
	 * Avoid triggers calls during loading to increase the performance. */
		
	SET join_collapse_limit TO 1;
	SET CONSTRAINTS ALL DEFERRED;

	WITH src AS (
				SELECT
					COALESCE("day"::DATE, '1900-01-01'::DATE)									AS event_dt,
					COALESCE((REGEXP_MATCH("time", '\d{2}:\d{2}:\d{2}'))[1], '00:00:00')::TIME	AS sales_time,
					COALESCE(prod_id, prod_name)												AS prod_id,
					COALESCE(supplier_id, supplier)												AS supl_id,
					COALESCE(empl_id, empl_phone, empl_email)									AS empl_id,
					NULL																		AS cust_id,
					COALESCE(pharmacy_id, pharmacy, pharmacy_phone, pharmacy_email)				AS store_id,
					payment_type																AS payment_meth_id,
					COALESCE(promotion_id, promotion)											AS promo_id,
					'offline'																	AS sales_chnl,
					unit_cost::NUMERIC(8,2)														AS unit_cost,
					quantity::INT																AS quantity,
					final_sales_amount::NUMERIC(8,2)											AS amount,
					'src_pharm_offline_sales'													AS tab,
					'sa_offline'																AS syst
				FROM sa_offline.src_pharm_offline_sales
				WHERE 
				/* Filtering by last loaded sales_dt from MTA table. Because dates and times are stored as varchar
				* as all other attributes, we created technical column that is indexed and used for filtering below. */
				ta_date_time > (
								SELECT COALESCE(MAX(last_sales_dt), '1900-01-01'::DATE)
								FROM bl_cl.mta_last_load_ce_sales										
							)
				UNION ALL

				SELECT
					COALESCE("date"::DATE, '1900-01-01'::DATE)									AS event_dt,
					COALESCE((REGEXP_MATCH("time", '\d{2}:\d{2}:\d{2}'))[1], '00:00:00')::TIME	AS sales_time,
					COALESCE(medicine_id, medicine)												AS prod_id,
					COALESCE(supplier_id, supplier)												AS supl_id,
					NULL																		AS empl_id,
					COALESCE(cust_id, cust_email)												AS cust_id,
					NULL																		AS store_id,
					'card'																		AS payment_meth_id,
					COALESCE(promo_id, promo)													AS promo_id,
					'online'																	AS sales_chnl,
					"cost"::NUMERIC(8,2),
					quantity::INT ,
					sales_amount::NUMERIC(8,2),
					'src_pharm_online_sales'													AS tab,
					'sa_online'																	AS syst
				FROM sa_online.src_pharm_online_sales
				WHERE 
			/* Filtering by last loaded sales_dt from MTA table. Because dates and times are stored as varchar
			* as all other attributes, we created technical column that is indexed and used for filtering below. */
				ta_date_time > (
								SELECT COALESCE(MAX(last_sales_dt), '1900-01-01'::DATE)
								FROM bl_cl.mta_last_load_ce_sales
							)
					),
	ins AS (
	INSERT INTO bl_3nf.ce_sales(
			event_dt,
			sales_time,
			product_id,
			supplier_id,
			employee_id,
			customer_id,
			store_id,
			payment_method_id,
			promo_id,
			sales_channel_id,
			unit_cost_dollar_amount,
			sales_quantity,
			sales_dollar_amount,
			ta_insert_dt
			)
	SELECT *
	FROM (
	SELECT
		src.event_dt AS dt,
		src.sales_time AS tm,
		COALESCE(ce_mp.ce_product_id, -1) AS prod,
		COALESCE(ce_supl.supplier_id, -1) AS supl,
		COALESCE(ce_empl.employee_id, -1) AS empl,
		COALESCE(ce_cust.customer_id, -1) AS cust,
		COALESCE(ce_stores.store_id, -1) AS store,
		COALESCE(ce_pm.payment_method_id, -1) AS pm,
		COALESCE(ce_promo.promo_id, -1) AS promo,
		COALESCE(ce_sc.sales_channel_id, -1) sc,
		src.unit_cost, -- if cost is missed, leave as it is (NULL)
		COALESCE(src.quantity, 1) AS qnt, -- is quantity is undefined(NULL), put 1
		src.amount, -- if amount is missed, leave as it is (NULL)
		CURRENT_DATE AS ins_dt
	FROM src
	LEFT OUTER JOIN bl_3nf.ce_customers ce_cust
	ON ce_cust.customer_src_id = src.cust_id AND
	ce_cust.source_table = src.tab AND
	ce_cust.source_system = src.syst

	/* SRC will be joined with CE thorugh mp_src_id from MAP but taking into account
	 * start and end dates from CE. That is why MAP-CE should be treated as one joined table when joining then with SRC.
	 * We can't join sequentially SRC with MAP and then with CE (will be a lot of duplicates). SRC needs start and end dates 
	 * to select exact product version. */
	LEFT OUTER JOIN (
					SELECT
						ce_prod.product_id AS ce_product_id,
						ce_prod.product_src_id AS ce_src_id,
						ce_prod.product_name AS ce_name,
						ce_prod.start_dt AS ce_start,
						ce_prod.end_dt AS ce_end,
						ce_prod.source_table AS ce_src_tab,
						ce_prod.source_system AS ce_src_syst,
						mp_prod.product_id AS mp_product_id,
						mp_prod.product_src_id AS mp_src_id,
						mp_prod.start_dt AS mp_start,
						mp_prod.source_table AS mp_src_tab,
						mp_prod.source_system AS mp_src_syst
					FROM bl_3nf.ce_products_scd ce_prod
					LEFT OUTER JOIN bl_cl.map_products mp_prod
					ON ce_prod.product_src_id = mp_prod.product_id::VARCHAR
					AND ce_prod.source_table = 'map_products'
					AND ce_prod.source_system = 'bl_cl'
					) ce_mp
	ON src.prod_id = COALESCE(ce_mp.mp_src_id, ce_mp.ce_src_id::VARCHAR) AND
	src.tab = COALESCE(ce_mp.mp_src_tab, ce_mp.ce_src_tab) AND
	src.syst = COALESCE(ce_mp.mp_src_syst, ce_mp.ce_src_syst)
	AND src.event_dt BETWEEN COALESCE(ce_mp.mp_start, ce_mp.ce_start, '1900-01-01'::DATE) AND COALESCE(ce_mp.ce_end, '9999-12-31'::DATE)

	LEFT OUTER JOIN bl_3nf.ce_employees ce_empl
	ON ce_empl.employee_src_id = src.empl_id AND
	ce_empl.source_table = src.tab AND
	ce_empl.source_system = src.syst

	LEFT OUTER JOIN bl_cl.map_promotions mp_promo
	ON src.promo_id = mp_promo.promotion_src_id AND
	mp_promo.source_table = src.tab AND
	mp_promo.source_system = src.syst

	LEFT OUTER JOIN bl_3nf.ce_promotions ce_promo
	ON ce_promo.promo_src_id = COALESCE(mp_promo.promotion_id::VARCHAR, src.promo_id) AND
	ce_promo.source_table = CASE
				WHEN mp_promo.promotion_id IS NOT NULL THEN 'map_promotions'
				ELSE src.tab END AND
	ce_promo.source_system = CASE
				WHEN mp_promo.promotion_id IS NOT NULL THEN 'bl_cl'
				ELSE src.tab END

	LEFT OUTER JOIN bl_3nf.ce_stores
	ON ce_stores.store_src_id = src.store_id AND
	ce_stores.source_table = src.tab AND
	ce_stores.source_system = src.syst

	LEFT OUTER JOIN bl_cl.map_suppliers mp_supl
	ON src.supl_id = mp_supl.supplier_src_id AND
	mp_supl.source_table = src.tab AND
	mp_supl.source_system = src.syst

	LEFT OUTER JOIN bl_3nf.ce_suppliers ce_supl
	ON ce_supl.supplier_src_id = COALESCE(mp_supl.supplier_id::VARCHAR, src.supl_id) AND
	ce_supl.source_table = CASE
				WHEN mp_supl.supplier_id IS NOT NULL THEN 'map_suppliers'
				ELSE src.tab END AND
	ce_supl.source_system = CASE
				WHEN mp_supl.supplier_id IS NOT NULL THEN 'bl_cl'
				ELSE src.tab END

	/* Do not use source triplet for ce_sales_channels join
	 because we created channels manually (offline, online)
	 there are no such attributes in src tables */
	LEFT OUTER JOIN bl_3nf.ce_sales_channels ce_sc
	ON ce_sc.sales_channel_name = src.sales_chnl

	/* Paymnet methods were created manually, do not use source triplet when join, because
	only offline source contain names ('cash'/'card'), all online sales are made by 'card' */
	LEFT OUTER JOIN bl_3nf.ce_payment_methods ce_pm
	ON ce_pm.payment_method_src_id = src.payment_meth_id
	)
	GROUP BY dt, tm, prod, supl, empl, cust, store, pm, promo, sc, unit_cost, qnt, amount, amount, ins_dt
	RETURNING event_dt, sales_time
	)
	-- extract data for logging and mta table
	SELECT COUNT(*), MAX(event_dt + sales_time)
	INTO rows_ins, last_sales_timestamp
	FROM ins;

	-- exception during load that should be logged (if any)
	EXCEPTION WHEN OTHERS THEN
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
	END;


	/* Insert data into MTA only if it was inserted, do not put empty actions */
	IF last_sales_timestamp IS NOT NULL AND rows_ins != 0
		THEN INSERT INTO bl_cl.mta_last_load_ce_sales(last_sales_dt, rows_number)
		VALUES (last_sales_timestamp, rows_ins);
	END IF;

	time_end := CLOCK_TIMESTAMP();
	time_run := EXTRACT (EPOCH FROM (time_end - time_start));

	CALL bl_cl.prc_load_logs(
			prcd,
			schema_n,
			time_start,
			time_end,
			time_run,
			rows_ins,
			0, -- no update actions in this procedure
			er_flag,
			er_code, 
			er_msg
			);

	-- exception that can occur during last 4 operations
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Error in %, %: %', prcd, SQLSTATE, SQLERRM;
	COMMIT ;
END; $$;


COMMIT;

--CALL bl_cl.prc_load_ce_sales();
--SELECT * FROM bl_cl.logs;
--SELECT * FROM bl_cl.mta_last_load_ce_sales
