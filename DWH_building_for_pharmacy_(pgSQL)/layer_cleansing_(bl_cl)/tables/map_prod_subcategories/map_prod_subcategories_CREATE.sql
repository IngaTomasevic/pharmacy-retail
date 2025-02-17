/* Several categories can have subcategories with same names.
 * Thus, must be mapped together */ 

--DROP TABLE IF EXISTS bl_cl.map_prod_subcategories;
CREATE UNLOGGED TABLE IF NOT EXISTS bl_cl.map_prod_subcategories(
		subcategory_id INT NOT NULL, 
		subcategory_name VARCHAR(100) NOT NULL,
		subcategory_src_name VARCHAR(100),
		subcategory_src_id VARCHAR(100), 
		category_src_id VARCHAR(50) NOT NULL,
		source_table VARCHAR(23) NOT NULL, 
		source_system VARCHAR(10) NOT NULL
		);	
		
			
COMMIT;
