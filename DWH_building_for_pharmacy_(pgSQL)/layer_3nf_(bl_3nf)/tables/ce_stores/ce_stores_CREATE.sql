CREATE SEQUENCE IF NOT EXISTS bl_3nf.bl_3nf_seq_store_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;
	
--DROP TABLE IF EXISTS bl_3nf.ce_stores;
CREATE TABLE IF NOT EXISTS bl_3nf.ce_stores( 
		store_id INT NOT NULL,
		store_src_id VARCHAR(50) NOT NULL, 
		source_system VARCHAR(10) NOT NULL, 
		source_table VARCHAR(23) NOT NULL, 
		store_name VARCHAR(60) NOT NULL, 
		store_address_id INT NOT NULL,
		store_build_num VARCHAR(12) NOT NULL,
		store_phone_num VARCHAR(20) NOT NULL,
		store_email VARCHAR(255) NOT NULL,
		opening_dt DATE NOT NULL,
		floor_space NUMERIC(8, 2) NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_ce_stores_store_id 
		PRIMARY KEY (store_id),
		
		CONSTRAINT fk_ce_stores_store_address_id 
		FOREIGN KEY (store_address_id)
		REFERENCES bl_3nf.ce_addresses (address_id)
		);

ALTER SEQUENCE IF EXISTS bl_3nf.bl_3nf_seq_store_id
OWNED BY bl_3nf.ce_stores.store_id; 

COMMIT;
