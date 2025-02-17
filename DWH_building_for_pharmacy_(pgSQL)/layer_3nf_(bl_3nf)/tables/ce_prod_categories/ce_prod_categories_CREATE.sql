CREATE SEQUENCE IF NOT EXISTS bl_3nf.bl_3nf_seq_prod_category_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;
	
--DROP TABLE IF EXISTS bl_3nf.ce_prod_categories;
CREATE TABLE IF NOT EXISTS bl_3nf.ce_prod_categories( 
		prod_category_id INT NOT NULL,
		prod_category_src_id VARCHAR(50) NOT NULL, 
		source_system VARCHAR(10) NOT NULL, 
		source_table VARCHAR(23) NOT NULL, 
		prod_category_name VARCHAR(70) NOT NULL, 
		prod_category_descr VARCHAR(300) NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		
		CONSTRAINT pk_ce_prod_categories_prod_category_id 
		PRIMARY KEY (prod_category_id)
		);

ALTER SEQUENCE IF EXISTS bl_3nf.bl_3nf_seq_prod_category_id
OWNED BY bl_3nf.ce_prod_categories.prod_category_id;

COMMIT;
