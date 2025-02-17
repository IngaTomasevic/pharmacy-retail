--_____________________________________ PRIVILEGES FOR DWH ADMIN ________________________________________
/* Grants for selection from external table when inserting data into source table
using procedure. Role-based control priciple. */

GRANT SELECT ON sa_online.ext_pharm_online_sales TO dwh_admin;

COMMIT;

