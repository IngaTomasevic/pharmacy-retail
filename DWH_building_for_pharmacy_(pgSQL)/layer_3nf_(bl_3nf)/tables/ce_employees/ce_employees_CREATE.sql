CREATE SEQUENCE IF NOT EXISTS bl_3nf.bl_3nf_seq_employee_id 
	AS INT
	INCREMENT BY 1
	MINVALUE 1
	START 1;
	
--DROP TABLE IF EXISTS bl_3nf.ce_employees;
CREATE TABLE IF NOT EXISTS bl_3nf.ce_employees( 
		employee_id INT NOT NULL,
		employee_src_id VARCHAR(50) NOT NULL, 
		source_system VARCHAR(10) NOT NULL, 
		source_table VARCHAR(23) NOT NULL, 
		empl_first_name VARCHAR(50) NOT NULL, 
		empl_last_name VARCHAR(60) NOT NULL,
		empl_birth_dt DATE NOT NULL,
		empl_phone_num VARCHAR(20) NOT NULL,
		empl_gender VARCHAR(6) NOT NULL, 
		empl_email VARCHAR(255) NOT NULL,
		empl_position VARCHAR(50) NOT NULL,
		ta_insert_dt DATE NOT NULL, 
		ta_update_dt DATE NOT NULL, 
		CONSTRAINT pk_ce_employees_employee_id 
		PRIMARY KEY (employee_id)
		);

ALTER SEQUENCE IF EXISTS bl_3nf.bl_3nf_seq_employee_id
OWNED BY bl_3nf.ce_employees.employee_id; 

COMMIT;
