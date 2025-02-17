/* Specify foreig keys as deferrable to have the ability
 * during loading SET DEFERRED to avoid triggers call 
 * to increase the performance of loading */

--DROP TABLE IF EXISTS bl_3nf.ce_sales;		
CREATE TABLE IF NOT EXISTS bl_3nf.ce_sales( 
		event_dt DATE NOT NULL,
		sales_time TIME NOT NULL, 
		product_id INT NOT NULL, 
		supplier_id INT NOT NULL, 
		employee_id INT NOT NULL,
		customer_id INT NOT NULL,
		store_id INT NOT NULL,
		payment_method_id INT NOT NULL,
		promo_id INT NOT NULL, 
		sales_channel_id INT NOT NULL, 
		unit_cost_dollar_amount NUMERIC(8,2) NOT NULL, 
		sales_quantity INT NOT NULL,
		sales_dollar_amount NUMERIC(8,2) NOT NULL, 
		ta_insert_dt DATE NOT NULL, 
		
		CONSTRAINT fk_ce_sales_supplier_id 
		FOREIGN KEY (supplier_id)
		REFERENCES bl_3nf.ce_suppliers (supplier_id)
		DEFERRABLE INITIALLY IMMEDIATE, 
		
		CONSTRAINT fk_ce_sales_employee_id 
		FOREIGN KEY (employee_id)
		REFERENCES bl_3nf.ce_employees (employee_id)
		DEFERRABLE INITIALLY IMMEDIATE,
		
		CONSTRAINT fk_ce_sales_customer_id 
		FOREIGN KEY (customer_id)
		REFERENCES bl_3nf.ce_customers (customer_id)
		DEFERRABLE INITIALLY IMMEDIATE,
		
		CONSTRAINT fk_ce_sales_store_id 
		FOREIGN KEY (store_id)
		REFERENCES bl_3nf.ce_stores (store_id)
		DEFERRABLE INITIALLY IMMEDIATE, 
		
		CONSTRAINT fk_ce_sales_payment_method_id 
		FOREIGN KEY (payment_method_id)
		REFERENCES bl_3nf.ce_payment_methods (payment_method_id)
		DEFERRABLE INITIALLY IMMEDIATE, 
		
		CONSTRAINT fk_ce_sales_promo_id 
		FOREIGN KEY (promo_id)
		REFERENCES bl_3nf.ce_promotions (promo_id)
		DEFERRABLE INITIALLY IMMEDIATE, 
		
		CONSTRAINT fk_ce_sales_sales_channel_id 
		FOREIGN KEY (sales_channel_id)
		REFERENCES bl_3nf.ce_sales_channels (sales_channel_id)
		DEFERRABLE INITIALLY IMMEDIATE
		);
	
/* Index for increasing performance when loading into BL_DM fact table partitions. 
 * When accesing ce_sales table it will use index scan instead of Seq scan. Brin because it takes less space and performance is good. */
CREATE INDEX IF NOT EXISTS idx_ce_sales_event_dt
ON bl_3nf.ce_sales USING brin(event_dt);

COMMIT;

