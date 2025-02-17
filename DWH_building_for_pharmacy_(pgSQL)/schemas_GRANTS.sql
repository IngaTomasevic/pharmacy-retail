--_____________________________________ PRIVILEGES FOR DWH ADMIN ________________________________________
-- Role and user creation. Role-based control principle.
DO $$
BEGIN
	CREATE ROLE dwh_admin;
	CREATE USER inga_tomesevic;

	EXCEPTION WHEN duplicate_object
		THEN RAISE NOTICE 'Already exists: %', SQLERRM;
	WHEN OTHERS
		THEN RAISE NOTICE 'Skipping: %', SQLERRM;
END; $$;


/* Separate script for USAGE grants for schemas. For each objects grants
 * are defined separately in appropriate folders related to objects */

GRANT dwh_admin TO inga_tomesevic;

GRANT USAGE ON SCHEMA sa_online TO dwh_admin;
GRANT USAGE ON SCHEMA sa_offline TO dwh_admin;
GRANT USAGE ON SCHEMA bl_cl TO dwh_admin;
GRANT USAGE ON SCHEMA bl_3nf TO dwh_admin;
GRANT USAGE ON SCHEMA bl_dm TO dwh_admin;

GRANT CREATE ON SCHEMA bl_cl TO dwh_admin; -- for functions, procedures creation

COMMIT;
