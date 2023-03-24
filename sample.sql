USE FINAL_TASK;
CREATE or REPLACE NOTIFICATION INTEGRATION FINAL_TASK_PIPE_EVENT
ENABLED = TRUE
TYPE = QUEUE
NOTIFICATION_PROVIDER=AZURE_STORAGE_QUEUE
AZURE_STORAGE_QUEUE_PRIMARY_URI='****'
AZURE_TENANT_ID='ddfa3cce-e83c-4f6f-a6eb-39b4e62a19f2';

SHOW INTEGRATIONS;

desc NOTIFICATION INTEGRATION FINAL_TASK_PIPE_EVENT;

create or replace storage integration azure_int
  type = external_stage
  storage_provider = azure
  enabled = true
  azure_tenant_id = 'ddfa3cce-e83c-4f6f-a6eb-39b4e62a19f2'
  storage_allowed_locations = ('azure://torageitech.blob.core.windows.net/terraform-data-lakev2/output');


desc storage integration azure_int;

CREATE OR REPLACE file format json_format
type=json
COMPRESSION = 'AUTO'
ENABLE_OCTAL = FALSE
ALLOW_DUPLICATE = FALSE
STRIP_OUTER_ARRAY = FALSE
STRIP_NULL_VALUES = FALSE
IGNORE_UTF8_ERRORS = FALSE;

CREATE or REPLACE STAGE FINAL_TASK_STAGE
url = 'azure://storageitech.blob.core.windows.net/terraform-data-lakev2/output'
storage_integration = azure_int
file_format = json_format;
-- credentials = (azure_sas_token='?sv=2021-06-08&ss=bfqt&srt=co&sp=rwlacx&se=2022-08-02T18:06:45Z&st=2022-07-26T10:06:45Z&spr=https&sig=Kn1PQwjAFgijPEqIHiT6K4olmmIKZTHuTUQtuOHW0KI%3D')
drop stage final_task_stage;
show stages;

list @FINAL_TASK_STAGE;
select COUNT($1) from @FINAL_TASK_STAGE;

-- CREATE TASK mytask_hour
--   WAREHOUSE = COMPUTE_WH,
--   SCHEDULE = '60 MINUTE'
-- AS
-- REMOVE @FINAL_TASK_STAGE;

SELECT $1:"_id":"$oid"::string,
        $1:Place::string,
        $1:"Solid particles-10 - PDK"::decimal,
        $1:"Sulphur dioxide - PDK":decimal,
        $1:"Nitrogen oxide - PDK"::decimal,
        $1:"Carbon monoxide - PDK"::decimal,
        $1:"Air temperature - °С"::decimal,
        $1:"Pressure - gPa" ::decimal,
        $1:"Direction of the wind - degree"::decimal,
        $1:"Wind speed - m/s"::decimal,
        $1:"O-Xylene - mgr/m3"::decimal,
        $1:"Ground-level ozone",
        $1:"time_published"::timestamp from @FINAL_TASK_STAGE limit 100;

CREATE TABLE IF NOT EXISTS table_flatten(
    id VARCHAR(256),
    Place VARCHAR(256),
    "Solid particles-10 - PDK" decimal (8,2),
    "Sulphur dioxide - PDK" decimal (8,2),
    "Nitrogen oxide - PDK" decimal(8,2),
    "Carbon monoxide - PDK" decimal(8,2),
    "Air temperature - °С" decimal(8,2),
    "Pressure - gPa"  decimal(8,2),
    "Direction of the wind - degree" decimal(8,2),
    "Wind speed - m/s" decimal(8,2),
    "O-Xylene - mgr/m3" decimal(8,2),
    "Ground-level ozone" decimal(8,2),
    "time_published" TIMESTAMP);

CREATE TABLE json_data(
 data variant);

CREATE or replace pipe "json_test_PIPE"
auto_ingest = true
integration = 'FINAL_TASK_PIPE_EVENT'
as
COPY INTO json_data FROM @FINAL_TASK_STAGE;

select count(*) from json_data;

CREATE or replace pipe "FINAL_TASK_PIPE"
auto_ingest = true
integration = 'FINAL_TASK_PIPE_EVENT'
as
COPY INTO TABLE_FLATTEN FROM (SELECT $1:"_id":"$oid"::string,
        $1:Place::string,
        $1:"Solid particles-10 - PDK"::decimal,
        $1:"Sulphur dioxide - PDK":decimal,
        $1:"Nitrogen oxide - PDK"::decimal,
        $1:"Carbon monoxide - PDK"::decimal,
        $1:"Air temperature - °С"::decimal,
        $1:"Pressure - gPa" ::decimal,
        $1:"Direction of the wind - degree"::decimal,
        $1:"Wind speed - m/s"::decimal,
        $1:"O-Xylene - mgr/m3"::decimal,
        $1:"Ground-level ozone",
        $1:"time_published"::timestamp from @FINAL_TASK_STAGE);


SELECT COUNT(*) FROM TABLE_FLATTEN;

SELECT * FROM TABLE_FLATTEN ORDER BY "time_published" desc LIMIT 100;

SELECT id, COUNT(id)
FROM TABLE_FLATTEN
GROUP BY id
HAVING COUNT(id) > 1
ORDER BY 2 DESC;


ALTER PIPE FINAL_TASK_PIPE REFRESH;

SELECT SYSTEM$PIPE_STATUS( 'FINAL_TASK_PIPE' );

TRUNCATE TABLE_FLATTEN;

REMOVE @FINAL_TASK_STAGE;

//for accessing history
SELECT *
FROM table(information_schema.copy_history(table_name=>'TABLE_FLATTEN', start_time=> dateadd(hour, -1,current_timestamp())));

select *
from TABLE_FLATTEN
where timestamp >= dateadd(hour, -1 ,sysdate());

select sysdate(), current_timestamp();
