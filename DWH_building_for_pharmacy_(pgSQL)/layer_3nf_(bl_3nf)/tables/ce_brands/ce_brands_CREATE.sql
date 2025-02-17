CREATE SEQUENCE IF NOT EXISTS bl_3nf.bl_3nf_seq_brand_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;
	
--DROP TABLE IF EXISTS bl_3nf.ce_brands;
CREATE TABLE IF NOT EXISTS bl_3nf.ce_brands( 
		brand_id INT NOT NULL,
		brand_src_id VARCHAR(50) NOT NULL, 
		source_system VARCHAR(10) NOT NULL, 
		source_table VARCHAR(23) NOT NULL, 
		brand_name VARCHAR(70) NOT NULL, 
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_ce_brands_brand_id 
		PRIMARY KEY (brand_id)
		);

ALTER SEQUENCE IF EXISTS bl_3nf.bl_3nf_seq_brand_id
OWNED BY bl_3nf.ce_brands.brand_id; 

COMMIT;
