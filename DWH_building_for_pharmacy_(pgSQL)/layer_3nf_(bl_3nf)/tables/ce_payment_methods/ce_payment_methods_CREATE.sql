CREATE SEQUENCE IF NOT EXISTS bl_3nf.bl_3nf_seq_payment_method_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;
	
--DROP TABLE IF EXISTS bl_3nf.ce_payment_methods;
CREATE TABLE IF NOT EXISTS bl_3nf.ce_payment_methods( 
		payment_method_id INT NOT NULL,
		payment_method_src_id VARCHAR(4) NOT NULL, 
		source_system VARCHAR(10) NOT NULL, 
		source_table VARCHAR(23) NOT NULL, 
		payment_method_name VARCHAR(4) NOT NULL, 
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_ce_payment_methods_payment_method_id 
		PRIMARY KEY (payment_method_id)
		);
	
ALTER SEQUENCE IF EXISTS bl_3nf.bl_3nf_seq_payment_method_id
OWNED BY bl_3nf.ce_payment_methods.payment_method_id; 

COMMIT;
