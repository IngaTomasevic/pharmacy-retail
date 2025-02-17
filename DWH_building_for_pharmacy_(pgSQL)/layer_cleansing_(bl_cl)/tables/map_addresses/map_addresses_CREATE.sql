-- Addresses should be mapped also (are present in both sources)

--DROP TABLE IF EXISTS bl_cl.map_addresses;
CREATE UNLOGGED TABLE IF NOT EXISTS bl_cl.map_addresses(
		address_id SMALLINT NOT NULL, 
		address_descr VARCHAR(50) NOT NULL,
		address_src_descr VARCHAR(50),
		address_src_id VARCHAR(30) NOT NULL, 
		city VARCHAR(30) NOT NULL, 
		zip VARCHAR(30), 
		source_table VARCHAR(23) NOT NULL, 
		source_system VARCHAR(10) NOT NULL
		);	
			
COMMIT;
