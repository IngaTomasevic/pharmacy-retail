--DROP TABLE IF EXISTS bl_dm.fct_sales_dd;
CREATE TABLE IF NOT EXISTS bl_dm.fct_sales_dd( 
		time_day_id INT NOT NULL,
		time_of_day_surr_id INT NOT NULL,
		product_surr_id INT NOT NULL,
		supplier_surr_id INT NOT NULL,
		employee_surr_id INT NOT NULL,
		customer_surr_id INT NOT NULL,
		store_surr_id INT NOT NULL,
		payment_method_surr_id INT NOT NULL,
		promo_surr_id INT NOT NULL,
		sales_channel_surr_id INT NOT NULL,
		fct_unit_cost_dollar_amount NUMERIC(8,2), -- can be NULL, if cost ommited
		fct_regular_unit_dollar_price NUMERIC(8,2) NOT NULL,
		fct_discount_unit_dollar_price NUMERIC(8,2) NOT NULL,
		fct_sales_quantity INT NOT NULL,
		fct_extended_cost_dollar_amount NUMERIC(8,2), -- can be NULL, if cost ommited
		fct_extended_discount_dollar_amount NUMERIC(8,2) NOT NULL,
		fct_extended_sales_dollar_amount NUMERIC(8,2) NOT NULL,
		fct_profit_dollar_amount NUMERIC(8,2), -- can be NULL, if cost ommited
		ta_insert_dt DATE NOT NULL
		) PARTITION BY RANGE (time_day_id);
	
		
COMMIT; 
