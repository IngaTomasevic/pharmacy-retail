/* Table for storing last loaded data into ce_sales table. 
 * Is needed for filtering data when incremental loading. */

--DROP TABLE IF EXISTS bl_cl.mta_last_load_ce_sales;
CREATE TABLE IF NOT EXISTS bl_cl.mta_last_load_ce_sales(
		last_load_dt TIMESTAMP NOT NULL DEFAULT NOW(), -- when the last loading was performed
		last_sales_dt TIMESTAMP NOT NULL, -- last timestamp of the loaded transactions, directly is used for filtering
		rows_number INT NOT NULL -- how much rows was inserted
		);
		
COMMIT;
