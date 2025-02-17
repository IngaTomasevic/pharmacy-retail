CREATE SEQUENCE IF NOT EXISTS bl_3nf.bl_3nf_seq_prod_subcategory_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;
	
--DROP TABLE IF EXISTS bl_3nf.ce_prod_subcategories;
CREATE TABLE IF NOT EXISTS bl_3nf.ce_prod_subcategories( 
		prod_subcategory_id INT NOT NULL,
		prod_subcategory_src_id VARCHAR(100) NOT NULL, 
		source_system VARCHAR(10) NOT NULL, 
		source_table VARCHAR(23) NOT NULL, 
		prod_subcategory_name VARCHAR(100) NOT NULL, 
		prod_subcategory_descr VARCHAR(300) NOT NULL,
		prod_category_id INT NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_ce_prod_subcategories_prod_subcategory_id 
		PRIMARY KEY (prod_subcategory_id),
		
		CONSTRAINT fk_ce_prod_subcategories_prod_category_id 
		FOREIGN KEY (prod_category_id)
		REFERENCES bl_3nf.ce_prod_categories (prod_category_id)
		);

ALTER SEQUENCE IF EXISTS bl_3nf.bl_3nf_seq_prod_subcategory_id
OWNED BY bl_3nf.ce_prod_subcategories.prod_subcategory_id;

COMMIT;
