/* INT type range is big enough for storing days up to 9999 year, no need use BIGINT, safe space.
 * Create PK as a meaningful INTEGER(combination YYYYMMDD), so that to simplify joining tables:
 * By Integer is faster than by DATE */

--DROP TABLE IF EXISTS bl_dm.dim_time_day;
CREATE TABLE IF NOT EXISTS bl_dm.dim_time_day(
		time_day_id INT NOT NULL,
		time_day_dt DATE NOT NULL, 
		day_name VARCHAR(9) NOT NULL, 
		day_number_in_week INT NOT NULL,
		day_number_in_month INT NOT NULL,
		day_number_in_year INT NOT NULL,
		week_number_in_year INT NOT NULL,
		year_of_week INT NOT NULL,
		week_ending_dt DATE NOT NULL,
		month_number INT NOT NULL,
		month_name VARCHAR(9) NOT NULL,
		days_in_month INT NOT NULL,
		month_ending_dt DATE NOT NULL, 
		year_month_descr VARCHAR(7) NOT NULL,
		quarter_number INT NOT NULL,
		quarter_ending_dt DATE NOT NULL,
		quarter_descr VARCHAR(7) NOT NULL,
		year_number INT NOT NULL,
		days_in_year INT NOT NULL,
		year_ending_dt DATE NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		CONSTRAINT pk_dim_time_day_time_day_id
		PRIMARY KEY(time_day_id) 
		);
		
	
COMMIT;
