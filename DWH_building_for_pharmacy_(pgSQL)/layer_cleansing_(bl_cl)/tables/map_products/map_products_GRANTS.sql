/* Grant only necessary privileges. NO delete or truncate if it's not needed for sure */

GRANT SELECT, UPDATE, INSERT ON bl_cl.map_products TO dwh_admin;

COMMIT;
