CREATE SEQUENCE IF NOT EXISTS bl_dm.bl_dm_seq_sales_channel_surr_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;


--DROP TABLE IF EXISTS bl_dm.dim_sales_channels;	
CREATE TABLE IF NOT EXISTS bl_dm.dim_sales_channels( 
		sales_channel_surr_id INT NOT NULL,
		sales_channel_src_id VARCHAR(20) NOT NULL, 
		source_system VARCHAR(6) NOT NULL, 
		source_table VARCHAR(17) NOT NULL, 
		sales_channel_name VARCHAR(7) NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_dim_sales_channels_sales_channel_surr_id
		PRIMARY KEY (sales_channel_surr_id)
		);
		
	
ALTER SEQUENCE IF EXISTS bl_dm.bl_dm_seq_sales_channel_surr_id
OWNED BY bl_dm.dim_sales_channels.sales_channel_surr_id; 	

COMMIT; 
