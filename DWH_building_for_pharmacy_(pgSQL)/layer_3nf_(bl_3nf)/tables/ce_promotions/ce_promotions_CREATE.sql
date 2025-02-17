CREATE SEQUENCE IF NOT EXISTS bl_3nf.bl_3nf_seq_promo_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;
	
--DROP TABLE IF EXISTS bl_3nf.ce_promotions;
CREATE TABLE IF NOT EXISTS bl_3nf.ce_promotions( 
		promo_id INT NOT NULL,
		promo_src_id VARCHAR(50) NOT NULL, 
		source_system VARCHAR(10) NOT NULL, 
		source_table VARCHAR(23) NOT NULL, 
		promo_name VARCHAR(100) NOT NULL,
		promo_discount INT NOT NULL,
		promo_category_id INT NOT NULL,
		promo_channel_id INT NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_ce_promotions_promo_id 
		PRIMARY KEY (promo_id),
		
		CONSTRAINT fk_ce_promotions_promo_category_id 
		FOREIGN KEY (promo_category_id)
		REFERENCES bl_3nf.ce_promo_categories (promo_category_id),
		
		CONSTRAINT fk_ce_promotions_promo_channel_id 
		FOREIGN KEY (promo_channel_id)
		REFERENCES bl_3nf.ce_promo_channels (promo_channel_id)	
		);

ALTER SEQUENCE IF EXISTS bl_3nf.bl_3nf_seq_promo_id
OWNED BY bl_3nf.ce_promotions.promo_id; 

COMMIT;
