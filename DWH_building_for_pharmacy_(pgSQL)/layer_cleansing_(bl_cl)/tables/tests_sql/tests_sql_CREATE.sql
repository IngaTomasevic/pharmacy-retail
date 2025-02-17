/* Table that stores test slq queries______________________ */

--DROP TABLE IF EXISTS bl_cl.tests_sql;
CREATE TABLE IF NOT EXISTS bl_cl.tests_sql(
	test_name VARCHAR(50) NOT NULL UNIQUE,
	test_sql TEXT NOT NULL
	);




/* Insert all queries_________________________________*/

DO $$
BEGIN 
INSERT INTO bl_cl.tests_sql
VALUES (
-- count of all transactions in both sources
'ROWS COUNT: transactions SRC (combined)', 
'SELECT 
(SELECT COUNT(*) 
FROM sa_offline.src_pharm_offline_sales ) +
(SELECT COUNT(*) 
FROM sa_online.src_pharm_online_sales )'), 

-- count of rows in ce_sales (3nf)
('ROWS COUNT: transactions 3NF', 
'SELECT COUNT(*) FROM bl_3nf.ce_sales'),

-- count of rows in fct_sales_dd (DM)
('ROWS COUNT: transactions DM', 
'SELECT COUNT(*) FROM bl_dm.fct_sales_dd'),

-- duplicated rows count(if any) at 3nf
('DUPICATES COUNT: transactions 3NF', 
'SELECT COUNT(dupl_count)
FROM (
	SELECT COUNT(*) AS dupl_count
	FROM bl_3nf.ce_sales
	GROUP BY 
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
WHERE dupl_count > 1'),

-- duplicated rows count(if any) at DM
('DUPICATES COUNT: transactions DM', 
'SELECT COUNT(dupl_count)
FROM (
	SELECT COUNT(*) AS dupl_count
	FROM bl_dm.fct_sales_dd
	GROUP BY 
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
		ta_insert_dt
	)
WHERE dupl_count > 1'),

/* count of keys that are not related between SRC and 3nf, 
 * including all address hierarchy attributes */
('KEYS MISMATCH COUNT: address hierarchy SRC/3NF', 
'WITH src AS (
		SELECT
			address_id AS addr_id,
			street AS addr,
			city AS city,
			state AS state,
			''src_pharm_offline_sales'' AS tab
		FROM sa_offline.src_pharm_offline_sales
		GROUP BY address_id, street, city, state
		UNION ALL
		SELECT
			cust_address_id,
			cust_street_name,
			cust_city,
			cust_state,
			''src_pharm_online_sales'' AS tab
		FROM sa_online.src_pharm_online_sales
		GROUP BY cust_address_id, cust_street_name, cust_city, cust_state
		),
nf3 AS (
		SELECT mp.address_src_id, mp.source_table, ce_a.address_descr, ce_c.city_name, ce_s.state_name
		FROM bl_3nf.ce_addresses ce_a
		INNER JOIN bl_cl.map_addresses mp
		ON ce_a.address_src_id = mp.address_id::VARCHAR
		INNER JOIN bl_3nf.ce_cities ce_c
		ON ce_c.city_id = ce_a.city_id
		INNER JOIN bl_3nf.ce_states ce_s
		ON ce_s.state_id = ce_c.state_id
		), 
excpt AS (
	SELECT src.addr_id, UPPER(mp_a.address_descr), UPPER(lkp.city_name_lkp), UPPER(mp.state_name), src.tab
	FROM src
		INNER JOIN bl_cl.map_addresses mp_a
		ON mp_a.address_src_descr = src.addr
		AND mp_a.source_table = src.tab
		INNER JOIN bl_cl.lkp_cities lkp
		ON UPPER(lkp.city_name_src) = UPPER(src.city)
		INNER JOIN bl_cl.map_states mp
		ON mp.state_src_name = src.state
		AND mp.source_table = src.tab
	EXCEPT
	SELECT address_src_id, UPPER(address_descr), UPPER(city_name), UPPER(state_name), source_table
	FROM nf3
) 
SELECT COUNT(*) FROM excpt'),


/* count of keys that are not related between SRC and 3nf, 
 * including all product hierarchy attributes (prod, category, subcat) */
('KEYS MISMATCH COUNT: product hierarchy SRC/3NF',
'SELECT COUNT(*)
	FROM (
		SELECT
		UPPER(ofl.prod_name),
		UPPER(mp.subcategory_name),
		UPPER(ofl.class_name),
		UPPER(ofl.brand_name)
		FROM sa_offline.src_pharm_offline_sales ofl
		INNER JOIN bl_cl.map_prod_subcategories mp ON
		UPPER(mp.subcategory_src_name) = UPPER(ofl.subclass)
		GROUP BY ofl.prod_name, mp.subcategory_name, ofl.class_name, ofl.brand_name

		UNION

		SELECT
		UPPER(onl.medicine),
		UPPER(mp.subcategory_name),
		UPPER(onl.category),
		UPPER(onl.brand)
		FROM sa_online.src_pharm_online_sales onl
		INNER JOIN bl_cl.map_prod_subcategories mp ON
		UPPER(mp.subcategory_src_name) = UPPER(onl.subcategory)
		GROUP BY onl.medicine, mp.subcategory_name, onl.category, onl.brand

		EXCEPT

		SELECT
		UPPER(p.product_name),
		UPPER(ps.prod_subcategory_name),
		UPPER(pc.prod_category_name),
		UPPER(b.brand_name)
		FROM bl_3nf.ce_products_scd p
		INNER JOIN bl_3nf.ce_prod_subcategories ps
		ON p.prod_subcategory_id = ps.prod_subcategory_id
		INNER JOIN bl_3nf.ce_prod_categories pc
		ON pc.prod_category_id = ps.prod_category_id
		INNER JOIN bl_3nf.ce_brands b
		ON p.brand_id = b.brand_id
		)'),
		

/* Count of not related keys between 3nf and DM, using function */
('KEYS MISMATCH COUNT: all entities 3NF/DM',		
'SELECT COUNT(*) FROM bl_cl.fn_check_master_load_3nf_dm()');	

EXCEPTION WHEN unique_violation
	THEN RAISE NOTICE 'Skipping: %', SQLERRM;
END; $$;
--SELECT * FROM bl_cl.tests_sql;




