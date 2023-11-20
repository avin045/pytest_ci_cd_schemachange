# from snowflake.snowpark import Session
import os
import json
import re

# Parse the JSON string into a Python dictionary
json_str = open('./config/config.json','r+').read()
config_dict = json.loads(json_str)


# user = os.environ.get("snow_user")
# password = os.environ.get("snow_pwd")
# account = os.environ.get("snow_acc")
# warehouse = config_dict['warehouse']
# role = config_dict['role']


# connection_parameters = {
#     "user": user,
#     "password" : password,
#     "account": account,
#     "role": role,
#     "warehouse": warehouse
#  }

# session_ = Session.builder.configs(connection_parameters).create()

'''
    session.use_database(database=config_dict['database'])
    session.use_schema(schema=config_dict['schema'])
    table = session.table(config_dict['view/table name'])
'''

def extract_query(database_name,schema_name,table_name,session_):
    session_.use_database(database=database_name)
    session_.use_schema(schema=schema_name)
    # table = session_.table(name=table_name)
    df1 = session_.sql(f'''SELECT GET_DDL('table','{table_name}',true)''')
    pattern = r'(\w+)\.' # select Database Name
    result_query = df1.collect()[0][0]
    match = re.search(pattern, result_query)
    if match:
        database_name = f"{match.group(1)}."
    return result_query.replace(database_name,'')