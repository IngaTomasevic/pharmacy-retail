CREATE SEQUENCE IF NOT EXISTS bl_dm.bl_dm_seq_product_surr_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;


--DROP TABLE IF EXISTS bl_dm.dim_products_scd;
CREATE TABLE IF NOT EXISTS bl_dm.dim_products_scd( 
		product_surr_id INT NOT NULL,
		product_src_id VARCHAR(20) NOT NULL, 
		source_system VARCHAR(6) NOT NULL, 
		source_table VARCHAR(15) NOT NULL, 
		product_name VARCHAR(70) NOT NULL, 
		product_form VARCHAR(15) NOT NULL,
		unit_mass_measurement VARCHAR(15) NOT NULL,
		unit_mass NUMERIC(7,2) NOT NULL,
		units_per_package INT NOT NULL,
		prod_subcategory_id INT NOT NULL,
		prod_subcategory_name VARCHAR(50) NOT NULL, 
		prod_subcategory_descr VARCHAR(300) NOT NULL,
		prod_category_id INT NOT NULL,
		prod_category_name VARCHAR(50) NOT NULL, 
		prod_category_descr VARCHAR(300) NOT NULL,
		brand_id INT NOT NULL,
		brand_name VARCHAR(70) NOT NULL, 
		start_dt DATE NOT NULL,
		end_dt DATE NOT NULL,
		is_active VARCHAR(1),
		ta_insert_dt DATE NOT NULL, 
		CONSTRAINT pk_dim_products_scd_product_surr_id
		PRIMARY KEY (product_surr_id)
		);
	
		
	
ALTER SEQUENCE IF EXISTS bl_dm.bl_dm_seq_product_surr_id
OWNED BY bl_dm.dim_products_scd.product_surr_id;

COMMIT; 
