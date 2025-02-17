-- Create map table for categories (are present in both sources) 

--DROP TABLE IF EXISTS bl_cl.map_prod_categories;
CREATE UNLOGGED TABLE IF NOT EXISTS bl_cl.map_prod_categories(
		prod_category_id INTEGER NOT NULL,
		prod_category_name VARCHAR(50) NOT NULL,
		prod_category_src_name VARCHAR(50) NOT NULL,
		prod_category_src_id VARCHAR(50) NOT NULL,
		source_table VARCHAR(23) NOT NULL,
		source_system VARCHAR(10) NOT NULL
		);
			
COMMIT;
