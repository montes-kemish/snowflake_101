




----- Last exercise - -----
USE ROLE ACCOUNTDMIN;
USE DATABASE DEMO_DB;

create database DEMO_DB;

USE WAREHOUSE COMPUTE_WH;
 
CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.PART
AS
SELECT * FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."PART";
 
SELECT * FROM PART
ORDER BY P_MFGR DESC;



-----

    UPDATE DEMO_DB.PUBLIC.PART
    SET P_MFGR='Manufacturer#CompanyX'
    WHERE P_MFGR='Manufacturer#5';
     
    ----> Note down query id here:
     
    SELECT * FROM PART
    ORDER BY P_MFGR DESC;

    SELECT * FROM  PART AT(OFFSET => -60*3);

    select * from PART before(statement => '01b518d5-0001-e49a-0005-e22200028016');


    ----------
    -- undrop

        CREATE DATABASE TIMETRAVEL_EXERCISE;
    CREATE SCHEMA TIMETRAVEL_EXERCISE.COMPANY_X;


    CREATE TABLE CUSTOMER AS
    SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
    LIMIT 200;


    DROP SCHEMA TIMETRAVEL_EXERCISE.COMPANY_X;

    undrop schema timetravel_exercise.company_x;


    select * from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY order by start_time desc;
    
