/* Grant only necessary privileges. NO delete or truncate if it's not needed for sure */

GRANT SELECT, UPDATE, INSERT ON bl_cl.mta_last_load_sa_online TO dwh_admin;

COMMIT;
