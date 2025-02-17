/* Grant only necessary privileges. NO delete or truncate if it's not needed for sure */

GRANT SELECT, UPDATE, INSERT ON bl_3nf.ce_prod_categories TO dwh_admin;

GRANT USAGE, SELECT ON bl_3nf.bl_3nf_seq_prod_category_id TO dwh_admin;

COMMIT;
