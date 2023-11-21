# ---------------------- Getting Data From JSON - Start ---------------------- #
user = os.environ.get("snow_user")
password = os.environ.get("snow_pwd")
account = os.environ.get("snow_acc")
warehouse = config_dict['warehouse']
role = config_dict['role']

expected_column_count = config_dict["column count"]
database_from_config = config_dict['database']
schema_from_config = config_dict['schema']
view_table_name_from_config = config_dict['view/table name']

folder_name = config_dict["view/table name"].split(".")[0]
file_name = f'''{config_dict['view/table name'].split('.')[1].replace('"','')}.sql'''
# ---------------------- Getting Data From JSON - End ---------------------- #

env_file = os.getenv('GITHUB_ENV')

with open(env_file, "a") as myfile:
    myfile.write(f"MY_VAR={warehouse:Any}")