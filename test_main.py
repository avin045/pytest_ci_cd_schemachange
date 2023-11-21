from snowflake.snowpark import Session
from snowflake.snowpark.dataframe import col
import os
import json
import pytest
import sys
import logging
import logging.handlers
from query_extractor import extract_query
from vars import *


# Parse the JSON string into a Python dictionary
json_str = open('./config/config.json','r+').read()
config_dict = json.loads(json_str)


# Remove or truncate the existing log file
log_file = "pytest.log"
if os.path.exists(log_file):
    with open(log_file, "w"):  # Open the file in "write" mode, which truncates it
        pass

# Create and configure logger
logger = logging.getLogger(__name__) # Github Stored in Log as UTC Timezone | Example => IST : 4:13PM UTC : 10:43 AM (-5.5 hrs)
logger.setLevel(logging.DEBUG)
logger_file_handler = logging.handlers.RotatingFileHandler(
    filename="pytest.log",
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)
logger.addHandler(logger_file_handler)


# # ---------------------- Getting Data From JSON - Start ---------------------- #
# user = os.environ.get("snow_user")
# password = os.environ.get("snow_pwd")
# account = os.environ.get("snow_acc")
# warehouse = config_dict['warehouse']
# role = config_dict['role']

# expected_column_count = config_dict["column count"]
# database_from_config = config_dict['database']
# schema_from_config = config_dict['schema']
# view_table_name_from_config = config_dict['view/table name']

# folder_name = config_dict["view/table name"].split(".")[0]
# file_name = f'''{config_dict['view/table name'].split('.')[1].replace('"','')}.sql'''
# # ---------------------- Getting Data From JSON - End ---------------------- #


# Session Creation
connection_parameters = {
    "user": user,
    "password" : password,
    "account": account,
    "role": role,
    "warehouse": warehouse
 }

session = Session.builder.configs(connection_parameters).create()

# Environment Set-up
session.use_database(database=config_dict['database'])
session.use_schema(schema=config_dict['schema'])
table_ = session.table(config_dict['view/table name'])


# TEST CASES
def test_count_check():
    logger.info(f"ROWCOUNT => The count of Source : {table_.count()}" if table_.count() > 0 else False)
    assert table_.count() > 0

def test_column_validation():
    column_validation = len(table_.columns) == expected_column_count or not(len(table_.columns) < expected_column_count)
    log_res = "PASSED, The Column Validation" if column_validation else 'NOT PASSED'
    logger.info(f"COLUMN VALIDATION : {log_res}")
    assert column_validation == True

def test_duplication():
    group_by_ = table_.group_by(table_.columns).count().filter(col('count') > 1).collect()
    log_res = "No Duplicates Found" if not(bool(group_by_)) else "Duplicates Found"
    logger.info(f"DUPLICATION : {log_res}")
    assert not(bool(group_by_)) == True

def check_pytest():
  exitcode = pytest.main() # To execute pytest and get the exit code
  if exitcode == 0:
    # Getting query without DB Name from snofwflake
    query = extract_query(
    database_name=database_from_config,
    schema_name=schema_from_config,
    table_name=view_table_name_from_config,
    session_=session
    )

    # If that Schema not present in FOLDER then Create it as a folder
    if folder_name not in os.listdir(r'./EnterpriseDatabase/'):
        os.mkdir(fr'./EnterpriseDatabase/{folder_name}')

    # Writing the Query which gets from Snowflake
    with open(fr"./EnterpriseDatabase/{folder_name}/{file_name}",'w') as file_:
        file_.write(query)
        log_res = 'File Written Successfully'
        logger.info(f"GET QUERY FROM SNOWFLAKE : {log_res}")
    

# result = check_pytest()
if 'pytest' in sys.modules:
    check_pytest()