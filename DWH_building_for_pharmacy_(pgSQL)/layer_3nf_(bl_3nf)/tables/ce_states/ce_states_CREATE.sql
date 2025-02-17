CREATE SEQUENCE IF NOT EXISTS bl_3nf.bl_3nf_seq_state_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;


--DROP TABLE IF EXISTS bl_3nf.ce_states;
CREATE TABLE IF NOT EXISTS bl_3nf.ce_states( 
		state_id INT NOT NULL,
		state_src_id VARCHAR(50) NOT NULL, 
		source_system VARCHAR(10) NOT NULL, 
		source_table VARCHAR(23) NOT NULL, 
		state_name VARCHAR(30) NOT NULL UNIQUE, 
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL,
		CONSTRAINT pk_ce_states_state_id
		PRIMARY KEY (state_id)
		);
		
ALTER SEQUENCE IF EXISTS bl_3nf.bl_3nf_seq_state_id
OWNED BY bl_3nf.ce_states.state_id; 	

COMMIT;
