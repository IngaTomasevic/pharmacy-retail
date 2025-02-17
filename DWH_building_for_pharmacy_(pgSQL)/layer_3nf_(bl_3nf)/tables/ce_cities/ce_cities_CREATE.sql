CREATE SEQUENCE IF NOT EXISTS bl_3nf.bl_3nf_seq_city_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;


--DROP TABLE IF EXISTS bl_3nf.ce_cities;
CREATE TABLE IF NOT EXISTS bl_3nf.ce_cities( 
		city_id INT NOT NULL,
		city_src_id VARCHAR(50) NOT NULL, 
		source_system VARCHAR(10) NOT NULL, 
		source_table VARCHAR(23) NOT NULL, 
		city_name VARCHAR(100) NOT NULL, 
		state_id INT NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_ce_cities_city_id 
		PRIMARY KEY (city_id),
		
		CONSTRAINT fk_ce_cities_state_id 
		FOREIGN KEY (state_id)
		REFERENCES bl_3nf.ce_states (state_id)
		);
	
ALTER SEQUENCE IF EXISTS bl_3nf.bl_3nf_seq_city_id
OWNED BY bl_3nf.ce_cities.city_id; 

COMMIT;
