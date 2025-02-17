CREATE SEQUENCE IF NOT EXISTS bl_3nf.bl_3nf_seq_customer_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;
	
--DROP TABLE IF EXISTS bl_3nf.ce_customers;
CREATE TABLE IF NOT EXISTS bl_3nf.ce_customers( 
		customer_id INT NOT NULL,
		customer_src_id VARCHAR(50) NOT NULL, 
		source_system VARCHAR(10) NOT NULL, 
		source_table VARCHAR(23) NOT NULL, 
		customer_first_name VARCHAR(50) NOT NULL, 
		customer_last_name VARCHAR(60) NOT NULL, 
		customer_phone_num VARCHAR(20) NOT NULL,
		customer_email VARCHAR(255) NOT NULL,
		customer_gender VARCHAR(6) NOT NULL, 
		customer_birth_dt DATE NOT NULL,
		account_reg_dt DATE NOT NULL,
		customer_address_id INT NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_ce_customers_customer_id 
		PRIMARY KEY (customer_id),
		
		CONSTRAINT fk_ce_customers_customer_address_id 
		FOREIGN KEY (customer_address_id)
		REFERENCES bl_3nf.ce_addresses (address_id)	
		);
	
ALTER SEQUENCE IF EXISTS bl_3nf.bl_3nf_seq_customer_id
OWNED BY bl_3nf.ce_customers.customer_id; 

COMMIT;
