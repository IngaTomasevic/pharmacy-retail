-- DROP TABLE IF EXISTS bl_cl.map_products;
CREATE UNLOGGED TABLE IF NOT EXISTS bl_cl.map_products(
	product_id INT4 NOT NULL,
	product_name VARCHAR(70) NOT NULL,
	product_src_name VARCHAR(70) NOT NULL,
	product_src_id VARCHAR(50) NOT NULL,
	product_src_subcategory VARCHAR(100),
	product_src_category VARCHAR(150),
	product_form VARCHAR(15),
	unit_mass_measurement VARCHAR(3),
	unit_mass NUMERIC(7,2),
	units_per_package INT,
	source_table VARCHAR(23) NOT NULL,
	source_system VARCHAR(10) NOT NULL, 
	start_dt DATE NOT NULL
);

			
COMMIT;
