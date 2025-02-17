CREATE TABLE IF NOT EXISTS sa_online.src_pharm_online_sales(
		receipt_number VARCHAR(255),
		"date" VARCHAR(255),
		"time" VARCHAR(255),
		cust_id VARCHAR(255),
		cust_full_name VARCHAR(255),
		cust_phone VARCHAR(255),
		cust_email VARCHAR(255),
		cust_gender VARCHAR(255),
		cust_birthdate VARCHAR(255),
		user_registration VARCHAR(255),
		cust_address_id VARCHAR(255),
		cust_city VARCHAR(255),
		cust_state VARCHAR(255),
		cust_postal_code VARCHAR(255),
		cust_street_num VARCHAR(255),
		cust_street_name VARCHAR(255),
		cust_build_num VARCHAR(255),
		promo_distr_id VARCHAR(255),
		promo_distr VARCHAR(255),
		promo_type_id VARCHAR(255),
		promo_type VARCHAR(255),
		promo_id VARCHAR(255),
		promo VARCHAR(255),
		promo_discount VARCHAR(255),
		medicine_id VARCHAR(255),
		medicine VARCHAR(255),
		cat_id VARCHAR(255),
		category VARCHAR(255),
		subcategory VARCHAR(255),
		brand_id VARCHAR(255),
		brand VARCHAR(255),
		supplier_id VARCHAR(255),
		supplier VARCHAR(255),
		supplier_phone VARCHAR(255),
		supplier_email VARCHAR(255),
		"cost" VARCHAR(255),
		price VARCHAR(255),
		quantity VARCHAR(255),
		sales_amount VARCHAR(255)
	);
	
/* Create additional technical column with time_date for further indexing, 
 * that will be used when incremental loading. Because we will use the last loaded
 * sales transactions date from mta table. And Source tables contains varchar.
 * It is very slow to perform Seq Scan by dates ant times from source table 
 * each time when filtering transactions date. 
 * This column will be indexed and increase performance 60 times. */
DO $$
BEGIN 
	ALTER TABLE IF EXISTS sa_online.src_pharm_online_sales
	ADD COLUMN ta_date_time TIMESTAMP;
	
	EXCEPTION WHEN duplicate_column
		THEN RAISE NOTICE 'Skipping: %', SQLERRM;
END; $$;



CREATE INDEX IF NOT EXISTS idx_online_sales_ta_date_time
ON sa_online.src_pharm_online_sales USING brin(ta_date_time);

--SELECT pg_size_pretty(pg_relation_size('sa_online.idx_online_sales_ta_date_time'));

COMMIT;
