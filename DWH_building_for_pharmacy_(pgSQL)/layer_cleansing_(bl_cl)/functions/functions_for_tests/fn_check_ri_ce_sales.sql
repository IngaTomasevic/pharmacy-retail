/* Functions that checks if there are bad keys is tthe ce_sales table,
 * which are not related to keys from dimensions. Scans each dimension
 * using NOT EXISTS operator. */


--DROP FUNCTION IF EXISTS bl_cl.fn_check_ri_ce_sales;
CREATE OR REPLACE FUNCTION bl_cl.fn_check_ri_ce_sales()
RETURNS TABLE (
	key_name TEXT, 
	bad_key_value VARCHAR(20)
	)
LANGUAGE plpgsql
AS $$
BEGIN 
	RETURN query
	SELECT 'poduct_id:' AS key_name, product_id::VARCHAR(20)
	FROM bl_3nf.ce_sales 
	WHERE NOT EXISTS (
					SELECT 1 FROM bl_3nf.ce_products_scd ce
					WHERE ce.product_id = ce_sales.product_id AND 
					ce_sales.event_dt BETWEEN ce.start_dt AND ce.end_dt
					)
	UNION ALL 
	SELECT 'customer_id:', customer_id::VARCHAR(20)
	FROM bl_3nf.ce_sales 
	WHERE NOT EXISTS (
					SELECT 1 FROM bl_3nf.ce_customers ce
					WHERE ce.customer_id = ce_sales.customer_id
					)
	UNION ALL 
	SELECT 'employee_id:', employee_id::VARCHAR(20)
	FROM bl_3nf.ce_sales 
	WHERE NOT EXISTS (
					SELECT 1 FROM bl_3nf.ce_employees ce
					WHERE ce.employee_id = ce_sales.employee_id
					)
	UNION ALL 
	SELECT 'payment_method_id:', payment_method_id::VARCHAR(20)
	FROM bl_3nf.ce_sales 
	WHERE NOT EXISTS (
					SELECT 1 FROM bl_3nf.ce_payment_methods ce
					WHERE ce.payment_method_id = ce_sales.payment_method_id
					)
	UNION ALL 
	SELECT 'promo_id:', promo_id::VARCHAR(20)
	FROM bl_3nf.ce_sales 
	WHERE NOT EXISTS (
					SELECT 1 FROM bl_3nf.ce_promotions ce
					WHERE ce.promo_id = ce_sales.promo_id
					)
	UNION ALL 
	SELECT 'sales_channel_id:' , sales_channel_id::VARCHAR(20)
	FROM bl_3nf.ce_sales 
	WHERE NOT EXISTS (
					SELECT 1 FROM bl_3nf.ce_sales_channels ce
					WHERE ce.sales_channel_id = ce_sales.sales_channel_id
					)
	UNION ALL 
	SELECT 'store_id:', store_id::VARCHAR(20)
	FROM bl_3nf.ce_sales 
	WHERE NOT EXISTS (
					SELECT 1 FROM bl_3nf.ce_stores ce
					WHERE ce.store_id = ce_sales.store_id
					)
	UNION ALL 
	SELECT 'supplier_id:', supplier_id::VARCHAR(20)
	FROM bl_3nf.ce_sales 
	WHERE NOT EXISTS (
					SELECT 1 FROM bl_3nf.ce_suppliers ce
					WHERE ce.supplier_id = ce_sales.supplier_id
					);
				
	IF NOT FOUND
		THEN RAISE NOTICE 'NO bad key in the fact table. RI check passed';
	END IF; 

	EXCEPTION WHEN OTHERS 
		THEN RAISE NOTICE '%: %', SQLSTATE, SQLERRM;
END; $$;

COMMIT;
