--DROP TABLE IF EXISTS bl_cl.logs CASCADE;
CREATE TABLE IF NOT EXISTS bl_cl.logs(
		date_time TIMESTAMP NOT NULL DEFAULT NOW(), -- timestamp of the transaction that has run the PROCEDURE. E.g.: a lot of procedures can be run within same transaction
		schema_name VARCHAR(10) NOT NULL, -- schema that is affected by the procedure
		procedure_name VARCHAR(50) NOT NULL, 
		start_time TIMESTAMP NOT NULL, -- start time of the specific procedure
		end_time TIMESTAMP NOT NULL, -- end of the procedure
		run_time_sec NUMERIC NOT NULL, -- run time in seconds
		rows_inserted INT NOT NULL, -- number of inserted by the procedure rows
		rows_updated INT NOT NULL, -- number of updated by the procedure rows
		error_flag CHAR(1) NOT NULL, -- 'Y'/ 'N'
		error_code VARCHAR(20), 
		error_msg VARCHAR(300),
		user_name VARCHAR(30) NOT NULL DEFAULT CURRENT_USER -- that has run the procedure
		);
	
COMMIT; 
