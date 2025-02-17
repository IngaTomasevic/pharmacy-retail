CREATE SEQUENCE IF NOT EXISTS bl_3nf.bl_3nf_seq_sales_channel_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;
	
--DROP TABLE IF EXISTS bl_3nf.ce_sales_channels;	
CREATE TABLE IF NOT EXISTS bl_3nf.ce_sales_channels( 
		sales_channel_id INT NOT NULL,
		sales_channel_src_id VARCHAR(7) NOT NULL, 
		source_system VARCHAR(10) NOT NULL, 
		source_table VARCHAR(23) NOT NULL, 
		sales_channel_name VARCHAR(7) NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_ce_sales_channels_sales_channel_id 
		PRIMARY KEY (sales_channel_id)
		);
	
ALTER SEQUENCE IF EXISTS bl_3nf.bl_3nf_seq_sales_channel_id
OWNED BY bl_3nf.ce_sales_channels.sales_channel_id; 

COMMIT; 
