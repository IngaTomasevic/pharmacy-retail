/* Create all times within a day (24 hours * 60 min * 60 sec = 86400 rows).
 * Create PK as an INT, using sequence. */

CREATE SEQUENCE IF NOT EXISTS bl_dm.bl_dm_seq_time_of_day_surr_id
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;


--DROP TABLE IF EXISTS bl_dm.dim_time_of_day
CREATE TABLE IF NOT EXISTS bl_dm.dim_time_of_day(
		time_of_day_surr_id INT NOT NULL,
		time_of_day TIME NOT NULL UNIQUE,
		hour_24 INT NOT NULL, 
		hour_12 VARCHAR(5) NOT NULL,
		minute_of_hour INT NOT NULL,
		second_of_hour INT NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		CONSTRAINT pk_dim_time_of_day_time_of_day_surr_id
		PRIMARY KEY(time_of_day_surr_id) 
		);	
	
ALTER SEQUENCE IF EXISTS bl_dm.bl_dm_seq_time_of_day_surr_id
OWNED BY bl_dm.dim_time_of_day.time_of_day_surr_id; 

COMMIT; 
