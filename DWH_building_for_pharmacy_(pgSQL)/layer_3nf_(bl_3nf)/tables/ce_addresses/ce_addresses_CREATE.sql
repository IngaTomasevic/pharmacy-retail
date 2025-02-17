CREATE SEQUENCE IF NOT EXISTS bl_3nf.bl_3nf_seq_address_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;
	
--DROP TABLE IF EXISTS bl_3nf.ce_addresses;
CREATE TABLE IF NOT EXISTS bl_3nf.ce_addresses( 
		address_id INT NOT NULL,
		address_src_id VARCHAR(50) NOT NULL, 
		source_system VARCHAR(10) NOT NULL, 
		source_table VARCHAR(23) NOT NULL, 
		address_descr VARCHAR(150) NOT NULL, 
		city_id INT NOT NULL,
		zip_code VARCHAR(10) NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_ce_addresses_address_id 
		PRIMARY KEY (address_id),
		
		CONSTRAINT fk_ce_addresses_city_id 
		FOREIGN KEY (city_id)
		REFERENCES bl_3nf.ce_cities (city_id)
		);

ALTER SEQUENCE IF EXISTS bl_3nf.bl_3nf_seq_address_id
OWNED BY bl_3nf.ce_addresses.address_id; 

COMMIT;