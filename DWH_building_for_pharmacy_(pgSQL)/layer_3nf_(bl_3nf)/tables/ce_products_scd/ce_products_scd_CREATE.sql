CREATE SEQUENCE IF NOT EXISTS bl_3nf.bl_3nf_seq_product_id
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;
	
--DROP TABLE IF EXISTS bl_3nf.ce_products_scd;
CREATE TABLE IF NOT EXISTS bl_3nf.ce_products_scd( 
		product_id INT NOT NULL,
		product_src_id VARCHAR(50) NOT NULL, 
		source_system VARCHAR(10) NOT NULL, 
		source_table VARCHAR(23) NOT NULL, 
		product_name VARCHAR(70) NOT NULL, 
		product_form VARCHAR(15) NOT NULL,
		unit_mass_measurement VARCHAR(15) NOT NULL,
		unit_mass NUMERIC(7,2) NOT NULL,
		units_per_package INT NOT NULL,
		prod_subcategory_id INT NOT NULL,
		brand_id INT NOT NULL,
		start_dt DATE NOT NULL,
		end_dt DATE NOT NULL,
		is_active VARCHAR(1),
		ta_insert_dt DATE NOT NULL, 
		CONSTRAINT pk_ce_products_scd_product_id_start_dt 
		PRIMARY KEY (product_id, start_dt),
		
		CONSTRAINT fk_ce_products_scd_prod_subcategory_id 
		FOREIGN KEY (prod_subcategory_id)
		REFERENCES bl_3nf.ce_prod_subcategories (prod_subcategory_id),
		
		CONSTRAINT fk_ce_products_scd_brand_id 
		FOREIGN KEY (brand_id)
		REFERENCES bl_3nf.ce_brands (brand_id)
		);
	
ALTER SEQUENCE IF EXISTS bl_3nf.bl_3nf_seq_product_id
OWNED BY bl_3nf.ce_products_scd.product_id;

COMMIT;
