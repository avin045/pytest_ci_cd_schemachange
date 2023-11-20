from snowflake.snowpark import Session
from snowflake.snowpark.dataframe import col
import logging
import logging.handlers
import os
import json
# import pytest


# Parse the JSON string into a Python dictionary
json_str = open('./config/config.json','r+').read()
config_dict = json.loads(json_str)


# pytest -v -s pytest_cicd.py
# pytest -o log_cli=true -v -s pytest_cicd.py
# pytest -o log_cli=true -v -s pytest_cicd.py
# pytest --log-file=test_log.txt test_example.py

# """
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

# logger = setup_logger()

user = os.environ.get("snow_user")
password = os.environ.get("snow_pwd")
account = os.environ.get("snow_acc")
warehouse = config_dict['warehouse']
role = config_dict['role']


connection_parameters = {
    "user": user,
    "password" : password,
    "account": account,
    "role": role,
    "warehouse": warehouse
 }

session = Session.builder.configs(connection_parameters).create()

session.use_database(database=config_dict['database']);
session.use_schema(schema=config_dict['schema']);
session.use_warehouse(warehouse=config_dict['warehouse']);

file_path = config_dict['src'];
file_content = open(file=file_path,mode='r+');
query = file_content.read();
# print(query)

# """
# '''
# source & target CHECK
src = session.sql(query=query.replace(';',''));
target = session.table(name=config_dict['target']);

# INPUT for Getting COLUMNS LIST
# columns_list = input("Enter the column list with ',' seperated : ").split(',')
columns_list = config_dict['columns_list_duplicates_or_not'].split(',')
list_of_cols = [col_.strip() for col_ in columns_list]

columns_list_null_check = config_dict["columns_list_null_check"].split(',')
list_of_cols_nc = [col_.strip() for col_ in columns_list]

def test_rowcount():
    logger.info(f"ROWCOUNT => The count of Source : {src.count()} and Target : {target.count()} Matching : {src.count() == target.count()}")
    assert src.count() == target.count()

def test_data_mismatch():
    mismatch_cols = []
    matched_cols = [tgt_col for src_col,tgt_col in zip(sorted(src.columns),sorted(target.columns)) if src_col == tgt_col]
    result = src.select(matched_cols).minus(target.select(matched_cols)).collect()
    log_res = 'PASSED' if not bool(result) == True else 'NOT PASSED'
    logger.info(f"DATA MISMATCH : {log_res}")
    assert not bool(result) == True,f"The Mismatching count is {len(mismatch_cols)} \n and the columns are {','.join(mismatch_cols)}"

def test_duplicates_or_not():
    # EMPLOYEE_NUMBER, EMPLOYEE_FULL_NAME, EMPLOYEE_STATUS_CODE, DEPARTMENT_NUMBER, SUPERVISOR_NUMBER
    # EMPLOYEE_NUMBER, EMPLOYEE_FULL_NAME, EMPLOYEE_STATUS_CODE, DEPARTMENT_NUMBER # current one
    # EMPID,DEPARTMENT,POSITION
    result = target.select(list_of_cols).group_by(list_of_cols).count().filter(col('count') > 1).collect() # []
    log_res = 'PASSED' if not bool(result) == True else 'NOT PASSED'
    logger.info(f"DUPLICATES OR NOT : {log_res}")
    assert not bool(result) == True

def test_null_check():
    null_columns = ','.join([
    col_ for col_ in target.select(list_of_cols_nc).columns if target.select(list_of_cols_nc).where(target.select(list_of_cols_nc).col(col_).is_null()).collect()
    ])
    log_res = 'PASSED' if target.count() == target.select(list_of_cols_nc).dropna().count() else 'NOT PASSED'
    logger.info(f"NULL CHECK : {log_res}")
    assert True if target.count() == target.select(list_of_cols_nc).dropna().count() else False, f"The Null Columns are {null_columns}"

# def test_column_match_sttm():
#     count_ = 0
#     col_in_tgt_not_in_sttm = []
#     sttm_table = session.table(name=config_dict["sttm_table"]).select("COLUMN NAME").collect();
#     sttm_table_col_list = [f"{cols[0]}".lower() for cols in sttm_table]
#     for column_ in target.columns:
#         if f"{column_}".lower() in sttm_table_col_list:
#             count_ += 1
#         if f"{column_}".lower() not in sttm_table_col_list:
#                 col_in_tgt_not_in_sttm.append(column_)

#     cols_in_sttm_not_in_tgt = [col_name for col_name in sttm_table_col_list if col_name not in target.columns]
 
#     if len(cols_in_sttm_not_in_tgt) != 0:
#         result_ = f"COLUMN PRESENT IN STTM BUT NOT IN TARGET : {','.join(cols_in_sttm_not_in_tgt)}"
    
#     if count_ != len(sttm_table_col_list):
#         res_ = f"Columns present in Target not in STTM {','.join(col_in_tgt_not_in_sttm)}"

#     log_res = 'PASSED' if count_ == len(sttm_table_col_list) else 'NOT PASSED'
#     logger.info(f"COLUMN MATCH WITH STTM :  {log_res}")
#     assert count_ == len(sttm_table_col_list),f"{res_} {result_}"

# '''