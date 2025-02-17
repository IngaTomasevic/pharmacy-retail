SET search_path TO sa_offline;

CREATE EXTENSION IF NOT EXISTS file_fdw;
CREATE SERVER IF NOT EXISTS fdw_files 
FOREIGN DATA WRAPPER file_fdw;

/* Specify VARCHAR for all columns to avoid Errors, because some cells can contain any symbols,
 * we don't know, what is the content. Some data may be modified during transportation,
 * Appropriate data types will be established in the 3NF layer */

CREATE FOREIGN TABLE IF NOT EXISTS sa_offline.ext_pharm_offline_sales(
		invoice VARCHAR(255),
		"day" VARCHAR(255), -- specify column-names as it is in source
		"time" VARCHAR(255) ,
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
		)
SERVER fdw_files
OPTIONS (
		filename 'D:\CEEAU23_Inga_tomasevic\DWH_Part1\Topic_01\pharm_offline_sales.csv',
		HEADER 'true',
		FORMAT 'csv',
		DELIMITER ','
		);
--SELECT * FROM sa_offline.ext_pharm_offline_sales;
	
	
COMMIT;
