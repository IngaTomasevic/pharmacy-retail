-- _____________________________________________ LOGGING LOAD PROCEDURE ________________________________________________________
/* Procedure that inserts data into log table. Will be placed at the end of each procedure that LOADS data into any of tables */

--DROP PROCEDURE IF EXISTS bl_cl.prc_load_logs;

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_logs(
		prcd VARCHAR(50),
		schm VARCHAR(10),
		start_t TIMESTAMP,
		end_t TIMESTAMP,
		run_t NUMERIC,
		rows_ins INT,
		rows_upd INT,
		er_flag CHAR(1),
		er_code VARCHAR(20), 
		er_msg VARCHAR(300)
		)
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO bl_cl.logs(
		procedure_name,
		schema_name,
		start_time,
		end_time,
		run_time_sec,
		rows_inserted,
		rows_updated,
		error_flag,
		error_code,
		error_msg,
		user_name
		)
	VALUES (prcd, schm, start_t, end_t, run_t, rows_ins, rows_upd, er_flag, er_code, er_msg, CURRENT_USER);

	/* If error - show during which procedure */
	EXCEPTION WHEN OTHERS
		THEN RAISE NOTICE 'Error during logging of %.%: %, %', schm, prcd, SQLSTATE, SQLERRM;
END; $$;

COMMIT; 
