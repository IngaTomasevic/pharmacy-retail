-- Promotions should be deduplicated and cleaned
CREATE UNLOGGED TABLE IF NOT EXISTS bl_cl.map_promotions(
			promotion_id INT NOT NULL,
			promotion_name VARCHAR(70) NOT NULL, 
			promotion_src_name VARCHAR(70), 
			promotion_src_id VARCHAR(50), 
			source_table VARCHAR(23),
			source_system VARCHAR(10)
			); 
			
COMMIT;
