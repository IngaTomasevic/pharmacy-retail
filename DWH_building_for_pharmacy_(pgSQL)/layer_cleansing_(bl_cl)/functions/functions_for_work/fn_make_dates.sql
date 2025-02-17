/* Imserts data into dim_time_day according to the specified range */

CREATE OR REPLACE FUNCTION bl_cl.fn_make_dates(start_date DATE, end_date DATE)
RETURNS TABLE (
			time_day_id INT,
			time_day_dt DATE, 
			day_name VARCHAR(9), 
			day_number_in_week INT,
			day_number_in_month INT,
			day_number_in_year INT ,
			week_number_in_year INT,
			year_of_week INT,
			week_ending_dt DATE,
			month_number INT,
			month_name VARCHAR(9),
			days_in_month INT,
			month_ending_dt DATE, 
			year_month_descr VARCHAR(7),
			quarter_number INT,
			quarter_ending_dt DATE,
			quarter_descr VARCHAR(7),
			year_number INT,
			days_in_year INT,
			year_ending_dt DATE,
			ta_insert_dt DATE
			)
LANGUAGE plpgsql
AS $$
DECLARE
	dt DATE := start_date;
	start_week DATE;
	start_month DATE;
	start_quarter DATE;
	start_year DATE;
BEGIN 
	WHILE dt <= end_date
	LOOP
		-- simplify code readability by pre-calculated variables
		start_week := DATE_TRUNC ('Week', dt);
		start_month := DATE_TRUNC('Month', dt);
		start_quarter := DATE_TRUNC('Quarter', dt);
		start_year := DATE_TRUNC('Year', dt);
		
		time_day_id := REPLACE(dt::VARCHAR, '-', '')::INT;
		time_day_dt := 
				dt; -- time_day_dt(2022-01-01)
		day_name := 
				TO_CHAR (dt, 'Day'); -- day_name("Wednesday")
		day_number_in_week := 
				EXTRACT (DOW FROM dt); -- day_number_in_week(3)
		day_number_in_month := 
				EXTRACT (DAY FROM dt); -- day_number_in_month(31)
		day_number_in_year := 
				EXTRACT (DOY FROM dt); -- day_number_in_year(365)
		week_number_in_year := 
				EXTRACT (WEEK FROM dt); -- week_number_in_year(52)
		year_of_week := 
				EXTRACT (ISOYEAR FROM dt); -- year_of_week(2023)
		week_ending_dt := 
				(start_week + INTERVAL '6 days')::DATE; -- week_ending_dt
		month_number := 
				EXTRACT (MONTH FROM dt); -- month_number(1-12)
		month_name := 
				TO_CHAR (dt, 'Month'); -- month_name ("January")
		days_in_month := 
				EXTRACT (DAY FROM (start_month + INTERVAL '1 month' - start_month))::INT; -- days_in_month(28-31)
		month_ending_dt := 
				(start_month + INTERVAL '1 month' - INTERVAL '1 day')::DATE; -- month_ending_dt
		year_month_descr := 
				LEFT(dt::TEXT, 7); -- year_month_descr("2022-01")
		quarter_number := 
				EXTRACT (QUARTER FROM dt); -- quarter_number(1-4)
		quarter_ending_dt := 
				(start_quarter + INTERVAL '3 month' - INTERVAL '1 day')::DATE; -- quarter_ending_dt
		quarter_descr := 
				LEFT(start_quarter::TEXT, 7); -- quarter_descr("2022-03")
		year_number := 
				EXTRACT (YEAR FROM dt); -- year_number(2022)
		days_in_year := 
				EXTRACT(DAY FROM (start_year + INTERVAL '1 year' - start_year))::INT; -- days_in_year(365, 366)
		year_ending_dt := 
				(start_year + INTERVAL '1 year'- INTERVAL '1 day')::DATE; -- year_ending_dt
		ta_insert_dt := 
				CURRENT_DATE; -- ta_insert_dt
		RETURN NEXT;
	
		dt := dt + 1;
	END LOOP;
	RETURN ;
END; $$;

COMMIT; 

