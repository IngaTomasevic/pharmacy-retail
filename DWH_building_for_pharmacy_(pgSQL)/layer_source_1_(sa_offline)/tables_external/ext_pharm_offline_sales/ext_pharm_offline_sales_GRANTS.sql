--_____________________________________ PRIVILEGES FOR DWH ADMIN ________________________________________
/* Grants for selection from external table when inserting data into source table
using procedure. Role-based control priciple. */

GRANT SELECT ON sa_offline.ext_pharm_offline_sales TO dwh_admin;

COMMIT;

