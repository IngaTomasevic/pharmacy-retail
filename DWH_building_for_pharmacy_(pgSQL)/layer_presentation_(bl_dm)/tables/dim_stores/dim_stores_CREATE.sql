CREATE SEQUENCE IF NOT EXISTS bl_dm.bl_dm_seq_store_surr_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;


--DROP TABLE IF EXISTS bl_dm.dim_stores;
CREATE TABLE IF NOT EXISTS bl_dm.dim_stores( 
		store_surr_id INT NOT NULL,
		store_src_id VARCHAR(20) NOT NULL, 
		source_system VARCHAR(6) NOT NULL, 
		source_table VARCHAR(9) NOT NULL, 
		store_name VARCHAR(60) NOT NULL, 
		store_address_id INT NOT NULL,
		store_address_descr VARCHAR(50) NOT NULL, 
		store_zip_code VARCHAR(10) NOT NULL,
		store_city_id INT NOT NULL,
		store_city_name VARCHAR(40) NOT NULL, 
		storer_state_id INT NOT NULL,
		store_state_name VARCHAR(15) NOT NULL,
		store_build_num VARCHAR(10) NOT NULL,
		store_phone_num VARCHAR(20) NOT NULL,
		store_email VARCHAR(255) NOT NULL,
		opening_dt DATE NOT NULL,
		floor_space NUMERIC(8, 2) NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_dim_stores_store_surr_id
		PRIMARY KEY (store_surr_id)
		);
	
		
	
ALTER SEQUENCE IF EXISTS bl_dm.bl_dm_seq_store_surr_id
OWNED BY bl_dm.dim_stores.store_surr_id; 

COMMIT; 
