-- Create stage and integrations

CREATE OR REPLACE DATABASE SNOWPIPE;

-- create integration object that contains the access information
CREATE OR REPLACE STORAGE INTEGRATION azure_snowpipe_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID =  '<your-tenant-id>'
  STORAGE_ALLOWED_LOCATIONS = ( 'https://<your-container-url>');

  
  
-- Describe integration object to provide access
DESC STORAGE integration azure_snowpipe_integration;

---- Create file format & stage objects ----

-- create file format

create or replace file format snowpipe.public.fileformat_azure
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;

-- create stage object
create or replace stage snowpipe.public.stage_azure
    STORAGE_INTEGRATION = azure_snowpipe_integration
    URL = 'https://<your-container-url>'
    FILE_FORMAT = fileformat_azure;
    

-- list files
LIST @snowpipe.public.stage_azure;


--- notification integration object


CREATE OR REPLACE NOTIFICATION INTEGRATION snowpipe_event
  ENABLED = true
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://<your-container-url>'
  AZURE_TENANT_ID = '<your-tenant-id>';
  
  
  -- Register Integration
  
  DESC notification integration snowpipe_event;
  
  --Create the pipe in Snowflake
--query file
SELECT 
$1,
$2,
$3,
$4,
$5,
$6,
$7,
$8,
$9,
$10,
$11,
$12,
$13,
$14,
$15,
$16,
$17,
$18,
$19,
$20
FROM @snowpipe.public.stage_azure;


-- create destination table
create or replace table snowpipe.public.happiness (
    country_name varchar,
    regional_indicator varchar,
    ladder_score number(4,3),
    standard_error number(4,3),
    upperwhisker number(4,3),
    lowerwhisker number(4,3),
    logged_gdp number(5,3),
    social_support number(4,3),
    healthy_life_expectancy number(5,3),
    freedom_to_make_life_choices number(4,3),
    generosity number(4,3),
    perceptions_of_corruption number(4,3),
    ladder_score_in_dystopia number(4,3),
    explained_by_log_gpd_per_capita number(4,3),
    explained_by_social_support number(4,3),
    explained_by_healthy_life_expectancy number(4,3),
    explained_by_freedom_to_make_life_choices number(4,3),
    explained_by_generosity number(4,3),
    explained_by_perceptions_of_corruption number(4,3),
    dystopia_residual number (4,3));
    


COPY INTO HAPPINESS
FROM @snowpipe.public.stage_azure;

SELECT * FROM snowpipe.public.happiness;

TRUNCATE TABLE snowpipe.public.happiness;

 
-- create pipe
  create pipe azure_pipe
  auto_ingest = true
  integration = 'SNOWPIPE_EVENT'
  as
  copy into snowpipe.public.happiness
  from @snowpipe.public.stage_azure;
  


  

  SELECT SYSTEM$PIPE_STATUS( 'AZURE_PIPE' );
  

  
  

