/* Grant only necessary privileges. NO delete or truncate if it's not needed for sure */

GRANT SELECT, UPDATE, INSERT ON bl_dm.dim_time_of_day TO dwh_admin;

GRANT USAGE, SELECT ON bl_dm.bl_dm_seq_time_of_day_surr_id TO dwh_admin;

COMMIT;
