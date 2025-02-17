-- Deduplication states, bringin to the one spelling

--DROP TABLE IF EXISTS bl_cl.map_states;
CREATE UNLOGGED TABLE IF NOT EXISTS bl_cl.map_states(
		state_id SMALLINT NOT NULL, 
		state_name VARCHAR(15) NOT NULL,
		state_src_name VARCHAR(15), 
		state_src_id VARCHAR(15) NOT NULL, 
		source_table VARCHAR(23) NOT NULL, 
		source_system VARCHAR(10) NOT NULL
		);
			
COMMIT;

	