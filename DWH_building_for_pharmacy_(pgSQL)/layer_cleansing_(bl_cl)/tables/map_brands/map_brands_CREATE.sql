-- DROP TABLE IF EXISTS bl_cl.map_brands;
CREATE UNLOGGED TABLE IF NOT EXISTS bl_cl.map_brands (
		brand_id INT4 NOT NULL,
		brand_name VARCHAR(70) NOT NULL,
		brand_src_name VARCHAR(70) NOT NULL,
		brand_src_id VARCHAR(50) NOT NULL,
		source_table VARCHAR(23) NOT NULL,
		source_system VARCHAR(10) NOT NULL
);
			
COMMIT;
