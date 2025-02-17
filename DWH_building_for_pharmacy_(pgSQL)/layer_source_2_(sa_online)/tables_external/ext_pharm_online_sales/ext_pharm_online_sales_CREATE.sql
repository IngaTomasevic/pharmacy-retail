SET search_path TO sa_online;

CREATE EXTENSION IF NOT EXISTS file_fdw;
CREATE SERVER IF NOT EXISTS fdw_files
FOREIGN DATA WRAPPER file_fdw;

/* Specify VARCHAR for all columns to avoid Errors, because some cells can contain any symbols,
 * we don't know, what is the content. Moreover, the data may be modified during transportation,
 * Appropriate data types will be established in the 3NF layer */

CREATE FOREIGN TABLE IF NOT EXISTS sa_online.ext_pharm_online_sales(
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
		)
SERVER fdw_files
OPTIONS (
		filename 'D:\CEEAU23_Inga_tomasevic\DWH_Part1\Topic_01\pharm_online_sales.csv',
		HEADER 'true',
		FORMAT 'csv',
		DELIMITER ','
				);
			
COMMIT;