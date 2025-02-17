/* Table for storing last loaded data into offline source table. 
 * Is needed for filtering data when incremental loading. */

--DROP TABLE IF EXISTS bl_cl.mta_last_load_sa_offline;
CREATE TABLE IF NOT EXISTS bl_cl.mta_last_load_sa_offline(
		last_load_dt TIMESTAMP NOT NULL DEFAULT NOW(), -- when the last loading was performed
		last_event_dt TIMESTAMP NOT NULL, -- last timestamp of the loaded transactions, directly is used for filtering
		rows_number INT NOT NULL -- how much rows was inserted
		);
		
COMMIT;
