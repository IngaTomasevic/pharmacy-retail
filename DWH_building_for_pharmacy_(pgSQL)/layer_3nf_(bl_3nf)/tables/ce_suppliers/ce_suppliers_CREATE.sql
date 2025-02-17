CREATE SEQUENCE IF NOT EXISTS bl_3nf.bl_3nf_seq_supplier_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;
	
--DROP TABLE IF EXISTS bl_3nf.ce_suppliers;
CREATE TABLE IF NOT EXISTS bl_3nf.ce_suppliers( 
		supplier_id INT NOT NULL,
		supplier_src_id VARCHAR(50) NOT NULL, 
		source_system VARCHAR(10) NOT NULL, 
		source_table VARCHAR(23) NOT NULL, 
		supplier_name VARCHAR(70) NOT NULL, 
		supplier_phone_num VARCHAR(20) NOT NULL, 
		supplier_email VARCHAR(255) NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_ce_suppliers_supplier_id 
		PRIMARY KEY (supplier_id)
		);

ALTER SEQUENCE IF EXISTS bl_3nf.bl_3nf_seq_supplier_id
OWNED BY bl_3nf.ce_suppliers.supplier_id; 

COMMIT;
