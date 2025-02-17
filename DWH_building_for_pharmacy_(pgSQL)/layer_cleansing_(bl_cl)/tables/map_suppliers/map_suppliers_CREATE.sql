--DROP TABLE IF EXISTS bl_cl.map_suppliers;
CREATE UNLOGGED TABLE IF NOT EXISTS bl_cl.map_suppliers(
			supplier_id INT NOT NULL,
			supplier_name VARCHAR(70) NOT NULL, 
			supplier_src_name VARCHAR(70), 
			supplier_src_id VARCHAR(50),
			source_table VARCHAR(23),
			source_system VARCHAR(10)
			); 
			
COMMIT;
