--is_available_role.sql
--Snowflake Javascript function to determine if the current_user() has a role granted to them

CREATE OR REPLACE FUNCTION is_available_role ( role varchar )
returns boolean
as 'SELECT NVL(return_me, FALSE)
      FROM (SELECT booland(( SELECT 1 FROM TABLE(FLATTEN(input => parse_json(current_available_roles())))
            WHERE regexp_replace(VALUE, char(34), '''') = role ), 1) as return_me)';
  
SELECT is_available_role('ACCOUNTADMIN'); --true
SELECT is_available_role('MASK1');    --true
SELECT is_available_role('DAMN');   --false
