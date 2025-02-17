CREATE SEQUENCE IF NOT EXISTS bl_dm.bl_dm_seq_supplier_surr_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;


--DROP TABLE IF EXISTS bl_dm.dim_suppliers;
CREATE TABLE IF NOT EXISTS bl_dm.dim_suppliers( 
		supplier_surr_id INT NOT NULL,
		supplier_src_id VARCHAR(20) NOT NULL, 
		source_system VARCHAR(6) NOT NULL, 
		source_table VARCHAR(13) NOT NULL, 
		supplier_name VARCHAR(70) NOT NULL, 
		supplier_phone_num VARCHAR(20) NOT NULL, 
		supplier_email VARCHAR(255) NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_dim_suppliers_supplier_surr_id
		PRIMARY KEY (supplier_surr_id)
		);
	
		
	
ALTER SEQUENCE IF EXISTS bl_dm.bl_dm_seq_supplier_surr_id
OWNED BY bl_dm.dim_suppliers.supplier_surr_id; 

COMMIT; 
