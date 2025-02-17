/* Grant only necessary privileges. NO delete or truncate if it's not needed for sure */


GRANT SELECT, INSERT ON bl_3nf.ce_sales TO dwh_admin;

COMMIT;
