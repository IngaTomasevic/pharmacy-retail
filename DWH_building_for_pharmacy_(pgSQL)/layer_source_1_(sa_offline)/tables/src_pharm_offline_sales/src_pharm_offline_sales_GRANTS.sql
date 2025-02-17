/* GRANT for admin to SELECT data from sources needed for loading bl_cl, 3nf and dm tables.
 * INSERT needed for loading source data itself. NO delete or TRUNCATE privileges. */

GRANT SELECT, INSERT ON sa_offline.src_pharm_offline_sales TO dwh_admin;

COMMIT;



