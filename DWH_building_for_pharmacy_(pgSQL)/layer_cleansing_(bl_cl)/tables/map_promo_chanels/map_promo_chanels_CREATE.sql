--DROP TABLE bl_cl.map_promo_chanels;	
CREATE UNLOGGED TABLE IF NOT EXISTS bl_cl.map_promo_chanels(
		promo_channel_id INT NOT NULL,
		promo_channel_name VARCHAR(255) NOT NULL,
		promo_channel_src_name VARCHAR(255) NOT NULL,
		promo_channel_src_id VARCHAR(255) NOT NULL,
		source_table varchar(23) NOT NULL,
		source_system VARCHAR(10) NOT NULL
	);
			
COMMIT;
