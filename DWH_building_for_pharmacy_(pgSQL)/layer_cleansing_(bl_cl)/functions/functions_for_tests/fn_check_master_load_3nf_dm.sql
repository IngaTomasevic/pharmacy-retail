/* MASTER function for bunch ce-dim checks.
 * LOGIC: put all single checks into one parent function.
 * Check not only main entities, but those that are included in dim as attributes:
 * e.g. subcategories of products in dim are not a separate dim, but atrtribute of products,
 * they also can be checked by keys. */


--DROP FUNCTION IF EXISTS bl_cl.fn_check_master_load_3nf_dm;
CREATE OR REPLACE FUNCTION bl_cl.fn_check_master_load_3nf_dm()
RETURNS SETOF bl_cl.match_3nf_dm
LANGUAGE plpgsql
AS $$
DECLARE
	/* Use cursor in this function for practice. Although this query isn't huge
	 * and the cursor doesn't give a lot of benfits here. */
	check_cursor CURSOR FOR
	SELECT * FROM bl_cl.fn_check_load_3nf_dm('bl_3nf.ce_customers', 'customer_id', 'bl_dm.dim_customers', 'customer_src_id')
	UNION ALL
	SELECT * FROM bl_cl.fn_check_load_3nf_dm('bl_3nf.ce_employees', 'employee_id', 'bl_dm.dim_employees', 'employee_src_id')
	UNION ALL
	SELECT * FROM bl_cl.fn_check_load_3nf_dm('bl_3nf.ce_payment_methods', 'payment_method_id', 'bl_dm.dim_payment_methods', 'payment_method_src_id')
	UNION ALL
	SELECT * FROM bl_cl.fn_check_load_3nf_dm('bl_3nf.ce_products_scd', 'product_id', 'bl_dm.dim_products_scd', 'product_src_id')
	UNION ALL
	SELECT * FROM bl_cl.fn_check_load_3nf_dm('bl_3nf.ce_promotions', 'promo_id', 'bl_dm.dim_promotions', 'promo_src_id')
	UNION ALL
	SELECT * FROM bl_cl.fn_check_load_3nf_dm('bl_3nf.ce_sales_channels', 'sales_channel_id', 'bl_dm.dim_sales_channels', 'sales_channel_src_id')
	UNION ALL
	SELECT * FROM bl_cl.fn_check_load_3nf_dm('bl_3nf.ce_stores', 'store_id', 'bl_dm.dim_stores', 'store_src_id')
	UNION ALL
	SELECT * FROM bl_cl.fn_check_load_3nf_dm('bl_3nf.ce_suppliers', 'supplier_id', 'bl_dm.dim_suppliers', 'supplier_src_id')
	UNION ALL
	SELECT * FROM bl_cl.fn_check_load_3nf_dm('bl_3nf.ce_prod_categories', 'prod_category_id', 'bl_dm.dim_products_scd', 'prod_category_id')
	UNION ALL
	SELECT * FROM bl_cl.fn_check_load_3nf_dm('bl_3nf.ce_prod_subcategories', 'prod_subcategory_id', 'bl_dm.dim_products_scd', 'prod_subcategory_id')
	UNION ALL
	SELECT * FROM bl_cl.fn_check_load_3nf_dm('bl_3nf.ce_promo_categories', 'promo_category_id', 'bl_dm.dim_promotions', 'promo_category_id')
	UNION ALL
	SELECT * FROM bl_cl.fn_check_load_3nf_dm('bl_3nf.ce_promo_channels', 'promo_channel_id', 'bl_dm.dim_promotions', 'promo_channel_id');

	rec record;
BEGIN
	FOR rec IN check_cursor
	LOOP
		RETURN NEXT rec;
	END LOOP;

	EXCEPTION WHEN OTHERS
		THEN RAISE NOTICE '%: %', SQLSTATE, SQLERRM;
END; $$;

-- CHECK all entities using master function
-- SELECT * FROM bl_cl.fn_check_master_load_3nf_dm();

COMMIT; 
