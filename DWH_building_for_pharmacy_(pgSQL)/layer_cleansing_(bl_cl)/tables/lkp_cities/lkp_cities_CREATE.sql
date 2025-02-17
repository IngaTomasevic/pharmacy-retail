-- For correction city names (are written in different ways)

--DROP TABLE IF EXISTS bl_cl.lkp_cities;
CREATE UNLOGGED TABLE IF NOT EXISTS bl_cl.lkp_cities(
		city_name_src VARCHAR(50) NOT NULL, 
		city_name_lkp VARCHAR(50)
		);	
			
COMMIT;
