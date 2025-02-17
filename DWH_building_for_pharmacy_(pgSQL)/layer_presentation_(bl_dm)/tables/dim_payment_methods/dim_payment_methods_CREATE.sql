CREATE SEQUENCE IF NOT EXISTS bl_dm.bl_dm_seq_payment_method_surr_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;


--DROP TABLE IF EXISTS bl_dm.dim_payment_methods;
CREATE TABLE IF NOT EXISTS bl_dm.dim_payment_methods( 
		payment_method_surr_id INT NOT NULL,
		payment_method_src_id VARCHAR(20) NOT NULL, 
		source_system VARCHAR(6) NOT NULL, 
		source_table VARCHAR(18) NOT NULL, 
		payment_method_name VARCHAR(4) NOT NULL, 
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_dim_payment_methods_payment_method_surr_id
		PRIMARY KEY (payment_method_surr_id)
		);
	
		
	
ALTER SEQUENCE IF EXISTS bl_dm.bl_dm_seq_payment_method_surr_id
OWNED BY bl_dm.dim_payment_methods.payment_method_surr_id; 

COMMIT; 
