CREATE TABLE IF NOT EXISTS sa_offline.src_pharm_offline_sales(
		invoice VARCHAR(255),
		"day" VARCHAR(255),
		"time" VARCHAR(255),
		empl_id VARCHAR(255),
		empl_full_name VARCHAR(255),
		empl_date_of_birth VARCHAR(255),
		empl_phone VARCHAR(255),
		empl_gender VARCHAR(255),
		empl_email VARCHAR(255),
		"role" VARCHAR(255),
		pharmacy_id VARCHAR(255),
		pharmacy VARCHAR(255),
		address_id VARCHAR(255),
		city VARCHAR(255),
		state VARCHAR(255),
		postal_code VARCHAR(255),
		street_num VARCHAR(255),
		street VARCHAR(255),
		build_num VARCHAR(255),
		pharmacy_phone VARCHAR(255),
		pharmacy_email VARCHAR(255),
		registration_date VARCHAR(255),
		floor_space VARCHAR(255),
		payment_type VARCHAR(255),
		promotion_channel_id VARCHAR(255),
		promotion_channel VARCHAR(255),
		promotion_subcategory_id VARCHAR(255),
		promotion_subcategory VARCHAR(255),
		promotion_id VARCHAR(255),
		promotion VARCHAR(255),
		discount VARCHAR(255),
		prod_id VARCHAR(255),
		prod_name VARCHAR(255),
		prod_descr VARCHAR(255),
		class_id VARCHAR(255),
		class_name VARCHAR(255),
		class_descr VARCHAR(255),
		subclass VARCHAR(255),
		subclass_descr VARCHAR(255),
		brand_id VARCHAR(255),
		brand_name VARCHAR(255),
		supplier_id VARCHAR(255),
		supplier VARCHAR(255),
		supplier_phone VARCHAR(255),
		supplier_email VARCHAR(255),
		unit_cost VARCHAR(255),
		unit_price VARCHAR(255),
		quantity VARCHAR(255),
		final_sales_amount VARCHAR(255)
		);
	
/* Create additional technical column with time_date for further indexing, 
 * that will be used when incremental loading. Because we will use the last loaded
 * sales transactions date from mta table. And Source tables contains varchar.
 * It is very slow to perform Seq Scan by dates ant times from source table 
 * each time when filtering transactions date. 
 * This column will be indexed and increase performance 60 times. */
DO $$
BEGIN 
	ALTER TABLE IF EXISTS sa_offline.src_pharm_offline_sales
	ADD COLUMN ta_date_time TIMESTAMP;
	
	EXCEPTION WHEN duplicate_column
		THEN RAISE NOTICE 'Skipping: %', SQLERRM;
END; $$;


CREATE INDEX IF NOT EXISTS idx_offline_sales_ta_datetime
ON sa_offline.src_pharm_offline_sales USING brin(ta_date_time);

--SELECT pg_size_pretty(pg_relation_size('sa_offline.idx_offline_sales_ta_datetime'));

COMMIT;
