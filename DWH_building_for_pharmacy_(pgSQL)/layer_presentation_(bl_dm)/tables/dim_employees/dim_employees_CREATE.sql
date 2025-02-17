CREATE SEQUENCE IF NOT EXISTS bl_dm.bl_dm_seq_employee_surr_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;


--DROP TABLE IF EXISTS bl_dm.dim_employees;
CREATE TABLE IF NOT EXISTS bl_dm.dim_employees( 
		employee_surr_id INT NOT NULL,
		employee_src_id VARCHAR(20) NOT NULL, 
		source_system VARCHAR(6) NOT NULL, 
		source_table VARCHAR(12) NOT NULL, 
		empl_first_name VARCHAR(50) NOT NULL, 
		empl_last_name VARCHAR(60) NOT NULL,
		empl_birth_dt DATE NOT NULL,
		empl_phone_num VARCHAR(20) NOT NULL,
		empl_gender VARCHAR(6) NOT NULL, 
		empl_email VARCHAR(255) NOT NULL,
		empl_position VARCHAR(50) NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_dim_employees_employee_surr_id 
		PRIMARY KEY (employee_surr_id)
		);
	
		
	
ALTER SEQUENCE IF EXISTS bl_dm.bl_dm_seq_employee_surr_id
OWNED BY bl_dm.dim_employees.employee_surr_id; 

COMMIT; 
