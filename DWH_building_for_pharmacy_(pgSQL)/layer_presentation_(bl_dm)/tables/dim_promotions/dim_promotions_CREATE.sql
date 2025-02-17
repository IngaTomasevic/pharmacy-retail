CREATE SEQUENCE IF NOT EXISTS bl_dm.bl_dm_seq_promo_surr_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;


--DROP TABLE IF EXISTS bl_dm.dim_promotions;
CREATE TABLE IF NOT EXISTS bl_dm.dim_promotions( 
		promo_surr_id INT NOT NULL,
		promo_src_id VARCHAR(20) NOT NULL, 
		source_system VARCHAR(6) NOT NULL, 
		source_table VARCHAR(14) NOT NULL, 
		promo_name VARCHAR(100) NOT NULL,
		promo_discount INT NOT NULL,
		promo_category_id INT NOT NULL,
		promo_category_name VARCHAR(50) NOT NULL,
		promo_channel_id INT NOT NULL,
		promo_channel_name VARCHAR(50) NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_dim_promotions_promo_surr_id
		PRIMARY KEY (promo_surr_id)
		);
		
	
ALTER SEQUENCE IF EXISTS bl_dm.bl_dm_seq_promo_surr_id
OWNED BY bl_dm.dim_promotions.promo_surr_id; 	

COMMIT; 
