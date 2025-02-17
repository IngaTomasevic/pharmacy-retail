/* Statements that were performed when analyzing sources at ETL stage: PROFILING, 
 * are not included in procedures, because it's needed once when analyzing and requires 
 * manual control and possible modifications, analyzing by results observation */
 



--________________________ STRUCTURE SCREEN: CUSTOMERS _______________________________________
/* Check if each cust_id corresponds to exactly one customer (name, phone num, email, address).
 * If no: see details and create map table. */

DO $$
DECLARE 
	cnt INT;
BEGIN 
	SELECT COUNT(*)
	FROM (
		SELECT 
			cust_id, 
			COUNT(DISTINCT cust_full_name) AS names_count, 
			COUNT(DISTINCT cust_phone) AS phones_count, 
			COUNT(DISTINCT cust_email) AS email_count,
			COUNT(DISTINCT cust_gender) AS gender_count,
			COUNT(DISTINCT cust_birthdate) AS birthdate_count,
			COUNT(DISTINCT user_registration) AS reg_date_count,
			COUNT(DISTINCT cust_address_id) AS address_count,
			COUNT(DISTINCT cust_city) AS city_count, 
			COUNT(DISTINCT cust_state) AS state_count, 
			COUNT(DISTINCT cust_postal_code) AS zip_count, 
			COUNT(DISTINCT cust_street_num) AS street_count, 
			COUNT(DISTINCT cust_street_name) AS street_names_count
		FROM sa_online.src_pharm_online_sales
		GROUP BY cust_id
		ORDER BY cust_id::INT
		)
	WHERE names_count > 1
	OR phones_count > 1
	OR email_count > 1
	OR birthdate_count > 1
	OR gender_count > 1
	OR reg_date_count > 1
	OR address_count > 1
	OR city_count > 1
	OR state_count > 1
	OR zip_count > 1
	OR street_count > 1
	OR street_names_count > 1
	INTO cnt;

	RAISE NOTICE '% bad rows', cnt;
END; $$;
-- screen passed




--________________________ STRUCTURE SCREEN: EMPLOYESS _______________________________________
/* Check if each empl_id corresponds to exactly one employee (name, phone num, email...).
 * If no - create map table */
DO $$
DECLARE 
	cnt INT;
BEGIN 
	SELECT COUNT(*)
	FROM (
		SELECT 
			empl_id, 
			count(DISTINCT UPPER(empl_full_name)) AS names_count, 
			count(DISTINCT empl_date_of_birth) AS birth_date_count, 
			count(DISTINCT empl_phone) AS phone_count,
			count(DISTINCT UPPER(empl_gender)) AS gender_count,
			count(DISTINCT UPPER(empl_email)) AS email_count,
			count(DISTINCT "role") AS role_count
		FROM sa_offline.src_pharm_offline_sales
		GROUP BY empl_id
		ORDER BY empl_id::int
		)
	WHERE names_count > 1
	OR birth_date_count > 1
	OR phone_count > 1
	OR gender_count > 1
	OR email_count > 1
	OR role_count > 1
	INTO cnt;

	RAISE NOTICE '% bad rows', cnt;
END; $$;
-- screen passed




--________________________ STRUCTURE SCREEN: ADDRESSES _______________________________________
/* Check if each address corresponds to exactly one (street number and name, city, state...).
 * If no - see details and think about solution */
SELECT *
FROM (
	SELECT 
		address_id, 
		count(DISTINCT UPPER(city)) AS city_count, 
		count(DISTINCT UPPER(state)) AS state_count,
		count(DISTINCT UPPER(postal_code)) AS zip_count,
		count(DISTINCT UPPER(street_num)) AS street_num_count,
		count(DISTINCT UPPER(street)) AS street_name_count
	FROM sa_offline.src_pharm_offline_sales
	GROUP BY address_id
	ORDER BY address_id::int
	)
WHERE city_count > 1
OR state_count > 1
OR zip_count > 1
OR street_num_count > 1
OR street_name_count > 1;
-- screen NOT passed


/* See details: what is the issue with street numbers? */
SELECT * 
FROM (
	SELECT 	address_id, city, state, postal_code, street_num, street
	FROM sa_offline.src_pharm_offline_sales
	GROUP BY address_id, city, state, postal_code, street_num, street
	UNION ALL 
	SELECT 	cust_address_id, cust_city, cust_state, cust_postal_code, cust_street_num, cust_street_name
	FROM sa_online.src_pharm_online_sales
	GROUP BY cust_address_id, cust_city, cust_state, cust_postal_code, cust_street_num, cust_street_name
)
GROUP BY address_id, city, state, postal_code, street_num, street
ORDER BY address_id::INT;
-- screen conclusion: take into account numeric values when converting




--________________________ HIERARCHY SCREEN: ADDRESSES _______________________________________
/* Analyze addresses hierarchy: if each street belong to one city? City to state?*/
WITH addr AS (
	SELECT 
		cust_address_id AS address_id, 
		cust_city AS city, 
		cust_state AS state, 
		cust_postal_code AS postal_code, 
		cust_street_name AS street
	FROM sa_online.src_pharm_online_sales
	GROUP BY cust_address_id, cust_city, cust_state, cust_postal_code, cust_street_name
)
SELECT 'COUNT CITIES BY ADDRESS', -1
UNION ALL

SELECT street, COUNT(DISTINCT city)
FROM addr
GROUP BY street
HAVING COUNT(DISTINCT city) > 1

UNION ALL 
SELECT 'COUNT SATATES BY CITIES', -1
UNION ALL 

SELECT city, COUNT(DISTINCT state)
FROM addr
GROUP BY city
HAVING COUNT(DISTINCT state) > 1

UNION ALL 
SELECT 'COUNT IDs BY (street, city, state, postal_code)', -1
UNION ALL 

SELECT street, COUNT(DISTINCT address_id)
FROM addr
GROUP BY street, city, state, postal_code
HAVING COUNT(DISTINCT address_id) > 1

UNION ALL 
SELECT 'COUNT ALL BY ADDRESS ID', -1
UNION ALL 

SELECT address_id, COUNT(addr.*)
FROM addr
GROUP BY address_id
HAVING COUNT(DISTINCT addr.*) > 1;

-- screen conclusion: several states have same city names 
-- several cities have same address names, ame street have 2 IDs




--________________________ HIERARCHY SCREEN: PRODUCTS _______________________________________
/* Analyze producst hierarchy: if each subcategory name belong to one category? 
 * Product name to subcategory? Product_id to one product? */
WITH prod AS (
		SELECT medicine_id AS prod_id, medicine AS prod, subcategory AS subcat, category AS cat
		FROM sa_online.src_pharm_online_sales
		GROUP BY medicine_id, medicine, subcategory, category
		UNION ALL
		SELECT prod_id, prod_name, subclass, class_name
		FROM sa_offline.src_pharm_offline_sales
		GROUP BY prod_id, prod_name, subclass, class_name
)
SELECT 'COUNT SUBCATS BY CATS' AS entity1, '' AS count_entity2
UNION ALL

SELECT UPPER(subcat), 'cats count:  '||COUNT(DISTINCT UPPER(cat)) AS counts
FROM prod
GROUP BY UPPER(subcat)
HAVING COUNT(DISTINCT cat) > 1

UNION ALL
SELECT 'COUNT PROD_ID BY PROD', ''
UNION ALL

SELECT UPPER(prod), 'prod_id count:  '||COUNT(DISTINCT prod_id)
FROM prod
GROUP BY UPPER(prod)
-- filter count like >2 because 2 means the same product have diff id in diff sources
-- but we also are interesting in if there are duplicated id accross one source
HAVING COUNT(DISTINCT prod_id) > 2

UNION ALL
SELECT 'COUNT PROD BY PROD_ID', ''
UNION ALL

SELECT UPPER(prod_id), 'prod count:  '||COUNT(DISTINCT UPPER(prod))
FROM prod
GROUP BY UPPER(prod_id)
HAVING COUNT(DISTINCT prod) > 2

UNION ALL
SELECT 'COUNT SUBCATS BY PROD_ID', ''
UNION ALL

SELECT UPPER(prod_id), 'subcats count:  '||COUNT(DISTINCT UPPER(subcat))
FROM prod
GROUP BY UPPER(prod_id)
HAVING COUNT(DISTINCT UPPER(subcat)) > 2
;
/* CONCLUSIOINS:
1) Different categories have same subcategories names 
2) Same products names (rare) belong to different subcategoris and thus have diff IDs
3) One prod_id belongs to unique product (but within its source)
4) Taking into account point 2 and 3, prod name, its subcategory and category is one unique mapped product
3) Subcategories should be brought to unified format (singular) */



	
--________________________ FIND NULLS IN SOURCES _______________________________________
/* Using function created aerlier find columns that contain NULLs */

SELECT bl_cl.fn_screen_nulls('src_pharm_offline_sales', 'sa_offline');
SELECT bl_cl.fn_screen_nulls('src_pharm_online_sales', 'sa_online');




--________________________ PROFILING: DATA TYPES MISMATCH _______________________________________
/* Using function created earlier find data types mismatched in sources */

/* we are interested in types DATE, NUMERIC, INT, TIME. Varchar always matches. */ 
SELECT bl_cl.screen_data_types('sa_offline.src_pharm_offline_sales', 'day', 'date');
SELECT bl_cl.screen_data_types('sa_offline.src_pharm_offline_sales', 'time', 'time');
SELECT bl_cl.screen_data_types('sa_offline.src_pharm_offline_sales', 'empl_date_of_birth', 'date');
SELECT bl_cl.screen_data_types('sa_offline.src_pharm_offline_sales', 'registration_date', 'date');
SELECT bl_cl.screen_data_types('sa_offline.src_pharm_offline_sales', 'floor_space', 'numeric');
SELECT bl_cl.screen_data_types('sa_offline.src_pharm_offline_sales', 'discount', 'int');
SELECT bl_cl.screen_data_types('sa_offline.src_pharm_offline_sales', 'unit_cost', 'numeric');
SELECT bl_cl.screen_data_types('sa_offline.src_pharm_offline_sales', 'unit_price', 'numeric');
SELECT bl_cl.screen_data_types('sa_offline.src_pharm_offline_sales', 'quantity', 'int');
SELECT bl_cl.screen_data_types('sa_offline.src_pharm_offline_sales', 'final_sales_amount', 'numeric');

SELECT bl_cl.screen_data_types('sa_online.src_pharm_online_sales', 'date', 'date');
SELECT bl_cl.screen_data_types('sa_online.src_pharm_online_sales', 'time', 'time');
SELECT bl_cl.screen_data_types('sa_online.src_pharm_online_sales', 'cust_birthdate', 'date');
SELECT bl_cl.screen_data_types('sa_online.src_pharm_online_sales', 'user_registration', 'date');
SELECT bl_cl.screen_data_types('sa_online.src_pharm_online_sales', 'promo_discount', 'int');
SELECT bl_cl.screen_data_types('sa_online.src_pharm_online_sales', 'cost', 'numeric');
SELECT bl_cl.screen_data_types('sa_online.src_pharm_online_sales', 'price', 'numeric');
SELECT bl_cl.screen_data_types('sa_online.src_pharm_online_sales', 'quantity', 'int');
SELECT bl_cl.screen_data_types('sa_online.src_pharm_online_sales', 'sales_amount', 'numeric');




--________________________ PROFILING: PHONE NUMBER LENGTH _______________________________________
/* Function that checks length of phone number. Count only digits in number, 
 * since numbers often are written with '()-' symbols and spaces*/
CREATE OR REPLACE FUNCTION bl_cl.screen_phone_num_len(
			table_n TEXT, column_n TEXT, len TEXT
			)
RETURNS VOID 
LANGUAGE plpgsql 
AS $$
DECLARE
	rows_count INT := 0;
	extract_len TEXT;
BEGIN 
	extract_len := 'length(regexp_replace('||column_n||', ''\D'', '''', ''g''))';
	EXECUTE 
	'SELECT count('||column_n||') 
	FROM '||table_n||'
	WHERE '||extract_len||' != '||len
	INTO rows_count; 
	IF rows_count > 0 THEN 	
		RAISE NOTICE '% wrog phone numbers in %.%', rows_count, table_n, column_n;
	END IF;
END; 
$$;

-- all numbers according to business region specific has length 10
SELECT bl_cl.screen_phone_num_len('sa_offline.src_pharm_offline_sales', 'empl_phone', '10');
SELECT bl_cl.screen_phone_num_len('sa_offline.src_pharm_offline_sales', 'pharmacy_phone', '10');
SELECT bl_cl.screen_phone_num_len('sa_offline.src_pharm_offline_sales', 'supplier_phone', '10');
SELECT bl_cl.screen_phone_num_len('sa_online.src_pharm_online_sales', 'cust_phone', '10');
SELECT bl_cl.screen_phone_num_len('sa_online.src_pharm_online_sales', 'supplier_phone', '10');


-- see details of problematic columns
SELECT * FROM (
SELECT pharmacy_phone AS phone
FROM sa_offline.src_pharm_offline_sales
UNION ALL 
SELECT cust_phone
FROM sa_online.src_pharm_online_sales
)
WHERE length(regexp_replace(phone, '\D', '', 'g')) != 10;




--________________________ CLEANSING: CITIES LOOKUP _______________________________________
/* Check how cities are named. Bring them to uniformed cleaned format */
SELECT cust_city AS city, cust_state AS state
FROM sa_online.src_pharm_online_sales
GROUP BY cust_city, cust_state
UNION ALL 
SELECT city, state
FROM sa_offline.src_pharm_offline_sales
GROUP BY city, state
ORDER BY city, state;

COMMIT;
