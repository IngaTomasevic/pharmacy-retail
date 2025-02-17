/* Grant only necessary privileges. NO delete or truncate if it's not needed for sure */

-- Alter is needed to set constraint deffered

GRANT SELECT, INSERT ON bl_dm.fct_sales_dd TO dwh_admin;

COMMIT;
