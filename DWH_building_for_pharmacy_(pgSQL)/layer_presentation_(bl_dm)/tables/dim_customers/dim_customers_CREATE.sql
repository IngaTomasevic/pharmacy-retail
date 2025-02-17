CREATE SEQUENCE IF NOT EXISTS bl_dm.bl_dm_seq_customer_surr_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;


--DROP TABLE IF EXISTS bl_dm.dim_customers;
CREATE TABLE IF NOT EXISTS bl_dm.dim_customers( 
		customer_surr_id INT NOT NULL,
		customer_src_id VARCHAR(20) NOT NULL, 
		source_system VARCHAR(6) NOT NULL, 
		source_table VARCHAR(12) NOT NULL, 
		customer_first_name VARCHAR(50) NOT NULL, 
		customer_last_name VARCHAR(60) NOT NULL, 
		customer_phone_num VARCHAR(20) NOT NULL,
		customer_email VARCHAR(255) NOT NULL,
		customer_gender VARCHAR(6) NOT NULL, 
		customer_birth_dt DATE NOT NULL,
		account_reg_dt DATE NOT NULL,
		customer_address_id INT NOT NULL,
		customer_address_descr VARCHAR(50) NOT NULL, 
		customer_zip_code VARCHAR(10) NOT NULL,
		customer_city_id INT NOT NULL,
		customer_city_name VARCHAR(40) NOT NULL, 
		customer_state_id INT NOT NULL,
		customer_state_name VARCHAR(15) NOT NULL,		
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_dim_customers_customer_surr_id 
		PRIMARY KEY (customer_surr_id)
		);
		
	
ALTER SEQUENCE IF EXISTS bl_dm.bl_dm_seq_customer_surr_id
OWNED BY bl_dm.dim_customers.customer_surr_id; 

COMMIT; 
