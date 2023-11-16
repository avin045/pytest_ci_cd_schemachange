
/* Medium Article for DYNAMIC TABLE ==> https://medium.com/snowflake/4-new-table-types-in-2022-by-snowflake-a-summary-301fb4fcdf60 */

use warehouse compute_wh;
use role accountadmin;

create or replace database hr_db;
create or replace schema hr_schema;

use database hr_db;
use schema hr_schema;

/* STAGE for "EMPLOYEE DETAILS" */
create or replace stage hr_data_stage
url = 's3://data-in-bucket/Human-Resources-Data-Set/'
CREDENTIALS = (AWS_KEY_ID = 'AWS_KEY_ID'
               AWS_SECRET_KEY = 'AWS_SECRET_KEY');


/* FILE FORMAT */
create or replace file format hr_file_fmt
type = 'CSV'
skip_header = 1
field_delimiter = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"';

ls @hr_data_stage;

select SPLIT_PART(metadata$filename, '/', 0) from @hr_data_stage (file_format => hr_file_fmt, pattern=>'.*Human Resources Data Set.*[.]csv');


-- Create External Table without Defining Columns
/*
CREATE OR REPLACE EXTERNAL TABLE ibm_ext_tbl WITH
LOCATION = @ibm_dataset
FILE_FORMAT = ibm_file_format
PATTERN='.*Human Resources Data Set.*[.]csv';
*/

-- Create External Table with Column Definition
CREATE OR REPLACE EXTERNAL TABLE EMPLOYEE_EXTN (
Employee_Name varchar            as (value:c1::varchar) ,
EmpID int                        as (value:c2::int)     ,
MarriedID int                    as (value:c3::int)     ,
MaritalStatusID int              as (value:c4::int)     ,
GenderID int                     as (value:c5::int)     ,
EmpStatusID int                  as (value:c6::int)     ,
DeptID int                       as (value:c7::int)     ,
PerfScoreID int                  as (value:c8::int)     ,
FromDiversityJobFairID int       as (value:c9::int)     ,
Salary bigint                    as (value:c10::bigint) ,
Termd int                        as (value:c11::int)    ,
PositionID int                   as (value:c12::int)    ,
Position varchar                 as (value:c13::varchar),
State varchar                    as (value:c14::varchar),
Zip bigint                       as (value:c15::bigint) ,
DOB date                         as (value:c16::date)   ,
Sex  varchar                     as (value:c17::varchar),
MaritalDesc varchar              as (value:c18::varchar),
CitizenDesc varchar              as (value:c19::varchar),
HispanicLatino varchar           as (value:c20::varchar),
RaceDesc varchar                 as (value:c21::varchar),
DateofHire date                  as (value:c22::date)   ,
DateofTermination date           as (value:c23::date)   ,
TermReason varchar               as (value:c24::varchar),
EmploymentStatus varchar         as (value:c25::varchar),
Department varchar               as (value:c26::varchar),
ManagerName varchar              as (value:c27::varchar),
ManagerID int                    as (value:c28::int)    ,
RecruitmentSource varchar        as (value:c29::varchar),
PerformanceScore varchar         as (value:c30::varchar),
EngagementSurvey float           as (value:c31::float)  ,
EmpSatisfaction varchar          as (value:c32::varchar),
SpecialProjectsCount int         as (value:c33::int)    ,
LastPerformanceReview_Date date  as (value:c34::date)   ,
DaysLateLast30 int               as (value:c35::int)    ,
Absences int                     as (value:c36::int)
)WITH
LOCATION = @hr_data_stage
FILE_FORMAT = hr_file_fmt
PATTERN='.*Human Resources Data Set.*[.]csv';


/* Employee Records from Cloud using COPY INTO ==> Consider as SOURCE */
CREATE OR REPLACE TABLE employee_copy_tbl(
Employee_Name varchar          ,
EmpID int                      ,
MarriedID int                  ,
MaritalStatusID int            ,
GenderID int                   ,
EmpStatusID int                ,
DeptID int                     ,
PerfScoreID int                ,
FromDiversityJobFairID int     ,
Salary bigint                  ,
Termd int                      ,
PositionID int                 ,
Position varchar               ,
State varchar                  ,
Zip bigint                     ,
DOB date                       ,
Sex  varchar                   ,
MaritalDesc varchar            ,
CitizenDesc varchar            ,
HispanicLatino varchar         ,
RaceDesc varchar               ,
DateofHire date                ,
DateofTermination date         ,
TermReason varchar             ,
EmploymentStatus varchar       ,
Department varchar             ,
ManagerName varchar            ,
ManagerID int                  ,
RecruitmentSource varchar      ,
PerformanceScore varchar       ,
EngagementSurvey float         ,
EmpSatisfaction varchar        ,
SpecialProjectsCount int       ,
LastPerformanceReview_Date date,
DaysLateLast30 int             ,
Absences int  
);

-- Making Changes in SOURCE
update employee_copy_tbl set LASTPERFORMANCEREVIEW_DATE = current_date() where empid in (10084,10196,10088);
-- delete from ibm_det_copy_tbl where department like '%Research & Development%';

COPY INTO employee_copy_tbl FROM @hr_data_stage
FILE_FORMAT = hr_file_fmt
PATTERN = '.*Human Resources Data Set.*[.]csv'
FORCE = FALSE;


/* Creating DYNAMIC TABLE => Combination of Stream and Task with Table*/
CREATE OR REPLACE DYNAMIC TABLE EMPLOYEE_DETAILS_DYNAMIC
TARGET_LAG = '10 minutes'
WAREHOUSE = COMPUTE_WH AS
select 
SPLIT(TRIM(split(EMPLOYEE_NAME,',')[1]::varchar),' ')[0]::text as FIRSTNAME,TRIM(split(EMPLOYEE_NAME,',')[0]::varchar) as LASTNAME,
-- EMPLOYEE_NAME,
EMPID,SALARY,POSITION,STATE,ZIP,DOB,SEX,DATEOFHIRE,DATEOFTERMINATION,EMPLOYMENTSTATUS,DEPARTMENT,MANAGERNAME,RECRUITMENTSOURCE,PERFORMANCESCORE,LASTPERFORMANCEREVIEW_DATE 
from employee_copy_tbl;

-- Suspending the EMPLOYEE_DETAILS_DYNAMIC (DYNAMIC TABLE) to stop the SYNC
alter dynamic table EMPLOYEE_DETAILS_DYNAMIC suspend;
-- alter dynamic table EMPLOYEE_DETAILS_DYNAMIC resume;