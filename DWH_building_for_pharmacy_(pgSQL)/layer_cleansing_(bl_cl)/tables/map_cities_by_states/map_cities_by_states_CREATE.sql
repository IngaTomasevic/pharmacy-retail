-- Several states can have cities with same names. Thus, cities must be mapped by states including

--DROP TABLE IF EXISTS bl_cl.map_cities_by_states;
CREATE UNLOGGED TABLE IF NOT EXISTS bl_cl.map_cities_by_states(
		city_id SMALLINT NOT NULL, 
		city_name VARCHAR(30) NOT NULL,
		city_src_name VARCHAR(30),
		city_src_id VARCHAR(30) NOT NULL, 
		state_id INT NOT NULL,
		state_name VARCHAR(15) NOT NULL,
		state_src_name VARCHAR(10) NOT NULL,
		state_src_id VARCHAR(10) NOT NULL,
		source_table VARCHAR(23) NOT NULL, 
		source_system VARCHAR(10) NOT NULL
		);	
		
COMMIT;
