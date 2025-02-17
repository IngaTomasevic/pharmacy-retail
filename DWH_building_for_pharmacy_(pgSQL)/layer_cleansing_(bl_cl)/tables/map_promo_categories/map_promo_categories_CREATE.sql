-- DROP TABLE IF EXISTS bl_cl.map_promo_categories;
CREATE UNLOGGED TABLE IF NOT EXISTS bl_cl.map_promo_categories (
	promo_category_id int4 NOT NULL,
	promo_category_name varchar(255),
	promo_category_src_name varchar(255),
	promo_category_src_id varchar(255) NOT NULL,
	source_table varchar(23) NOT NULL,
	source_system VARCHAR(10) NOT NULL
);
			
COMMIT;

