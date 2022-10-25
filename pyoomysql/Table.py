# General Imports
from posixpath import split
import string
from mysql.connector import errorcode
from mysql.connector import FieldType
from argparse import ArgumentParser
from datetime import datetime
from datetime import timedelta
from time import sleep
import json
import logging
from logging import DEBUG
from logging import CRITICAL
from logging import ERROR
from logging import FATAL
from logging import INFO
from logging import WARNING

from .util.sql import *

# Format and Get root logger
logging.basicConfig(
    format='[%(asctime)s][%(levelname)-8s] %(message)s',
    level=INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

# Table class
class Table:
    # Constructor
    def __init__(self, schema, name):
        self.name = name
        self.schema = schema
        self.database = schema.database
        self.fqn = f"{self.schema.name}.{self.name}"
        result = self.database.execute(command = f"SELECT table_schema, table_name, table_type, table_collation FROM information_schema.TABLES WHERE table_schema = '{self.schema.name}' AND table_name = '{self.name}'")
        self.ddl = ""
        self.columns = []
        self.keys = {}
        if result["rowcount"] == 0:
            self.exists = False
        else:
            self.exists = True
            self.collation = result["rows"][0]["table_collation"]
            self.load_table()

    def __str__(self):
        column_list = []
        for column in self.columns:
            column_list.append(f"{column.name} {column.type}({column.length})")
        return json.dumps({
            "database": f"{self.database.hostname}:{self.database.port}",
            "schema": f"{self.schema.name}",
            "name": self.name,
            "fqn": self.fqn,
            "collation": self.collation,
            "columns": column_list,
            "keys": self.keys,
            "ddl": self.ddl
        },indent=2)

    # Attributes and methods getters
    def get_attributes(self):
        return ['columns', 'database', 'exists', 'fqn', 'name', 'schema']

    def get_methods(self):
        return ['compare_data', 'delete', 'get_attributes', 'get_columns', 'get_insert_statement', 'get_methods', 'get_rowcount', 'insert', 'truncate', 'update']

    # Methods
    def get_columns(self):
        # logger.log(DEBUG, f"Table is: {table_name}")
        result = self.database.execute(f"SELECT column_name, ordinal_position, data_type, column_default, is_nullable, column_type, character_set_name, collation_name FROM information_schema.columns WHERE table_schema = '{self.schema.name}' AND table_name = '{self.name}' ORDER BY ordinal_position")
        column_list = []
        for column in result['rows']:
            # logger.log(DEBUG, column)
            column_list.append(
                Column(
                    table               = self,
                    name                = column['column_name'],
                    ordinal_position    = column['ordinal_position'],
                    type                = column['data_type'], 
                    default             = column['column_default'],
                    nullable            = column['is_nullable'], 
                    charset             = column['character_set_name'],
                    length              = 0
                )
            )
        return column_list

    def get_rowcount(self):
        logger.log(DEBUG,'Getting ROWCOUNT')
        return self.database.execute(f'SELECT count(1) AS rowcount FROM {self.fqn}')['rows'][0]

    def get_insert_statement(self, columns: list = None, values: list = None):
        if columns is None:
            columns = list(self.get_columns().keys())
        sql = f"INSERT INTO {self.fqn} ({columns})"
        for value in values:
            if type(value) == 'datetime':
                sql += f"'{value.isoformat().split('.')[0].replace('T',' ')}',"
            if type(value) == 'str':
                sql += f"{value},"
            else:
                sql += f"{value},"
        sql = sql.strip(",")
        sql += ")"
        return sql

    def load_table(self):
        result = self.database.execute(f"SHOW CREATE TABLE {self.fqn}")
        if len(result["rows"]) > 0:
            if "Create Table" in result["rows"][0].keys():
                original_ddl = result["rows"][0]["Create Table"]
            else:
                logger.error(f"Something went wrong with this resultset {result}")
        if original_ddl.find("USING") != -1:
            logger.debug("Original_ddl: {original_ddl}")
        self.ddl = f"CREATE TABLE {self.fqn} ("
        for line in original_ddl.replace("`","").split("\n"):
            column_pos = 0
            for item in line.strip(" ").split(","):
                if item.find("CREATE") == -1:
                    item_array = item.split(" ")
                    if item_array[0] not in ["PRIMARY","UNIQUE","KEY","ENGINE","AUTO_INCREMENT","DEFAULT","CHARSET",")"] and len(item_array) > 1:
                        column_pos+=1
                        index = 0
                        column_nullable = True
                        previous_word = ""
                        column_unsigned = False
                        column_autoincrement = False
                        column_default = None
                        column_length = 0
                        next_default = False
                        index = 0
                        for word in item_array:
                            if index == 0:
                                column_name=word
                                index+=1
                            elif index == 1:
                                if word.find("(") != -1:
                                    column_type=word.split("(")[0]
                                    column_length=word.split("(")[1].split(")")[0]
                                    index+=1
                                else:
                                    column_type = word
                            elif index > 1:
                                if word == "unsigned":
                                    column_unsigned: True
                                elif word == "NOT":
                                    previous_word = word
                                elif word == "NULL" and previous_word == "NOT":
                                    column_nullable = False
                                elif word == "DEFAULT":
                                    column_default = "next"
                                elif word == "AUTO_INCREMENT":
                                    column_autoincrement = True
                                else:
                                    if column_default == "next":
                                        column_default = word
                        self.columns.append(
                            Column(
                                table=self,
                                name=column_name,
                                ordinal_position= column_pos,
                                type=column_type,
                                length=column_length,
                                default = column_default,
                                nullable = column_nullable,
                                autoincrement = column_autoincrement
                            )
                        )
                        if len(self.columns) > 1:
                            self.ddl+=", "
                        self.ddl += f"{column_name} {column_type}({column_length})"
                        if not column_nullable:
                            self.ddl+=f" NOT NULL"
                        if column_default is not None:
                            self.ddl+=f" DEFAULT {column_default}"
                        if column_autoincrement:
                            self.ddl+=" AUTO_INCREMENT"
                    else:
                        if item_array[0] == "PRIMARY":
                            logger.debug("Current DDL: {self.ddl}\nProcessing {item_array}")
                            self.keys["PRIMARY"] = {}
                            self.keys["PRIMARY"]["columns"] = []
                            self.keys["PRIMARY"]["type"] = "primary"
                            self.ddl+=", PRIMARY KEY ("
                            for segment in item_array:
                                if segment[0] == "(":
                                    for key in segment.split("(")[1].split(")")[0].split(","):
                                        self.keys["PRIMARY"]["columns"].append(key)
                                        self.ddl+=f"{key},"
                                    self.ddl = f"{self.ddl[:-1]})"
                        elif item_array[0] == "UNIQUE":
                            logger.debug("Current DDL: {self.ddl}\nProcessing {item_array}")
                            self.keys[item_array[2]] = {}
                            self.keys[item_array[2]]["columns"] = []
                            self.keys[item_array[2]]["type"] = "unique"
                            self.ddl+=", UNIQUE KEY ("
                            for segment in item_array:
                                if segment[0] == "(":
                                    for key in segment.split("(")[1].split(")")[0].split(","):
                                        self.keys[item_array[2]]["columns"].append(key)
                                        self.ddl+=f"{key},"
                                    self.ddl = f"{self.ddl[:-1]})"
                        elif item_array[0] == "KEY":
                            logger.debug("Current DDL: {self.ddl}\nProcessing {item_array}")
                            self.keys[item_array[2]] = {}
                            self.keys[item_array[2]]["columns"] = []
                            if "USING" in item_array:
                                self.keys[item_array[2]]["type"] = item_array[item_array.index("USING")+1]
                            else:
                                self.keys[item_array[2]]["type"] = "non-unique"
                            self.ddl+=", KEY ("
                            for segment in item_array:
                                if segment[0] == "(":
                                    for key in segment.split("(")[1].split(")")[0].split(","):
                                        self.keys[item_array[2]]["columns"].append(key)
                                        self.ddl+=f"{key},"
                                    if "USING" in item_array:
                                        self.ddl = f"{self.ddl[:-1]}) USING {item_array[item_array.index('USING')+1]}"
                                    else:
                                        self.ddl = f"{self.ddl[:-1]})"
                        elif item_array[0] == ")" or item_array[0].find("ENGINE") != -1:
                            logger.debug("Current DDL: {self.ddl}\nProcessing {item_array}")
                            for segment in item_array:
                                if segment.find(")") == 0:
                                    self.ddl+=")"
                                elif segment.find("ENGINE") != -1:
                                    self.ddl+=f" {segment},"
                                elif segment.find("AUTO_INCREMENT") != -1:
                                    self.ddl+=f" {segment},"
                                elif segment.find("CHARSET") != -1:
                                    if segment.split("=")[1] == "utf8":
                                        def_charset = "utf8mb4"
                                    else:
                                        def_charset = segment.split("=")[1]
                                    self.ddl+=f" DEFAULT CHARSET {def_charset},"
                            self.ddl = self.ddl[:-1]

    # DDL
    def create(self):
        if self.exists:
            logger.warn(msg=f"The table {self.fqn} already exists.")
        else:
            self.database.execute = self.ddl

    def drop(self):
        self.database.execute(f"DROP TABLE {self.fqn}")
        self.exists = False

    # DML
    def truncate(self):
        return self.database.execute(f'TRUNCATE TABLE {self.fqn}')

    """
    Table.select(filters: < List_of_Conditions >, batch_size, delay)

    List_of_conditions is a list of dictionaries with the following structure:
    {
        "prefix": optional prefix and/or,
        "column": <name of the column>,
        "operator": [SQL operator: "BETWEEN", "IN", "LIKE", "=", "<", ">", ...],
        "value": <values> or list_of_values
    }
    """
    def select(self, columns="*", filters=[]):
        if columns == "*":
            column_list=""
            for column_name in self.get_columns().keys():
                column_list+=f"{column_name}, "
            column_list = column_list[:(len(column_list)-2)]
        else:
            column_list = list_to_column_list(columns)
        if len(filters) == 0:
            logger.warning("No Filterss found! Selecting all rows")
        else:
            condition_count = 0
            sql = f"SELECT {column_list} FROM {self.fqn} "
            for condition in filters:
                condition_count+=1
                logger.debug(f"Condition value type is: {type(condition['value'])}")
                if condition_count == 1 and "prefix" not in condition.keys():
                    sql+=f"WHERE {parse_condition(condition)} "
                elif condition_count == 1 and "prefix" in condition.keys():
                    sql+=f"WHERE {parse_condition(condition)[len(condition['prefix']):]} "
                elif condition_count > 1 and "prefix" not in condition.keys():
                    sql+=f"AND {parse_condition(condition)}"
                else:
                    sql+=f"{parse_condition(condition)} "
            logger.debug(f"Full command: {sql};")
            return self.database.execute(command=f"{sql};")
        

    def insert(self, rows: dict):
        pass

    """
    Table.delete(filters: < List_of_Conditions >, batch_size, delay)

    List_of_conditions is a list of dictionaries with the following structure:
    {
        "prefix": optional prefix and/or,
        "column": <name of the column>,
        "operator": [SQL operator: "IN", "LIKE", "=", "<", ">", ...],
        "values": <value> or list_of_values
    }
    """
    def delete(self, filters: list):
        full_result = {
            "rows": []
        }
        if len(filters) == 0:
            logger.warning("No Filterss found!")
            logger.warning("Please use Table.truncate() if you want to delete every record in the table")
            full_result["rowcount"] = 0
        else:
            condition_count = 0
            for condition in filters:
                sql = f"DELETE FROM {self.fqn} "
                condition_count+=1
                logger.debug(f"Condition value type is: {type(condition['value'])}")
                if type(condition["value"]) is str:
                    if condition_count == 1:
                        sql+=f"WHERE {condition['column']} {condition['operator']} '{condition['value']}' "
                    else:
                        sql+=f" AND {condition['column']} {condition['operator']} '{condition['value']}' "
                elif type(condition["value"]) is list and condition["operator"].upper() == "IN":
                    sql+=f"WHERE {condition['column']} {condition['operator']} {list_to_sql(condition['value'])} "
                elif type(condition["value"]) is list and condition["operator"].upper() == "BETWEEN":
                    sql+=f"WHERE {condition['column']} {condition['operator']} {list_to_sql(condition['value'][0])} AND {list_to_sql(condition['value'][1])} "
                else:
                    if condition_count > 1:
                        sql+=f"AND {condition['column']} {condition['operator']} {condition['value']} "
                    else:
                        sql+=f" WHERE {condition['column']} {condition['operator']} {condition['value']} "
                logger.debug(f"Full command: {sql}; COMMIT;")
                result = self.database.execute(command=f"{sql}; COMMIT;")
                full_result["rows"].append(result)
        return(full_result)

    def batch_delete(self, ids: list, batch_size=2, delay=0):
        if batch_size > 1:
            str_conditions = ""
            counter = 0
            full_result= {
                "rows": []
            }
            for key in ids:
                conditions = []
                for condition in conditions:
                    str_conditions += f"{condition[1]},"
                counter+=1
                if counter == batch_size or key == ids[len(ids)-1]:
                    str_conditions = str_conditions[:(len(str_conditions)-1)]
                    cond_string = f"{condition[0]} IN ('" + str_conditions.replace(",","','") + "')"
                    full_command = f"DELETE FROM {self.fqn} WHERE {cond_string}; COMMIT;"
                    # logger.debug(f"Full command: {full_command}")
                    logger.debug("Executing DELETE!")
                    result=self.database.execute(command=full_command)
                    counter=0
                    str_conditions = ""
                    full_result["rows"].append(result)
                    if delay > 0:
                        logger.debug("Found delay")
                        t = delay
                        while t > 0:
                            print(f"Waiting for {t} seconds...", end="\r")
                            sleep(1)
                            t -= 1
        else:
            logger.error("If you want to delete row-by-row, please use Table.delete(filters: dict) instead")

    def update(self):
        None

    def compare_data(self, table, batch_size=10000,print_to_console=False,fix_script=False,fix=False):
        column_names = ""
        if self.database.is_connected() and table.database.is_connected:
            logger.log(INFO,f'Comparing {self.fqn} on {self.database.hostname} and {table.database.hostname}')
            logger.log(DEBUG,'Connections verified, starting comparison')
            local_rowcount = self.get_rowcount()['rowcount']
            logger.log(DEBUG,f'Local Rows: {local_rowcount}')
            remote_rowcount = table.get_rowcount()['rowcount']
            logger.log(DEBUG,f'Remote Rows: {remote_rowcount}')
            if local_rowcount != remote_rowcount:
                logger.log(WARNING,f'Local and remote row counts differ!')
            processed = 0
            if fix_script:
                local_script = ""
                remote_script = ""
            while processed < local_rowcount or processed < remote_rowcount:
                local_result = self.database.execute(f'SELECT * FROM {self.schema.name}.{self.name} ORDER BY id LIMIT {processed}, {processed + batch_size}')['rows']
                # logger.log(DEBUG,local_result)
                remote_result = table.database.execute(f'SELECT * FROM {table.schema.name}.{table.name} ORDER BY id LIMIT {processed}, {processed + batch_size}')['rows']
                for index in range(batch_size):
                    if index < len(local_result) or index < len(remote_result):
                        if column_names == "":
                            for column in local_result[index].keys():
                                column_names += f"{column},"
                            column_names = column_names.strip(',')
                        if index < len(local_result):
                            local_row = local_result[index]
                        else:
                            local_row = None
                        if index < len(remote_result):
                            remote_row = local_result[index]
                        else: remote_row = None
                        if local_row is not None and remote_row is not None:
                            if  local_row != remote_row:
                                logger.debug("Difference catched")
                                if print_to_console:
                                    print(f'Conflict Local: {local_result[index]}')
                                    print(f'Conflict Remote: {remote_result[index]}')
                        else:
                            if local_row is not None:
                                if print_to_console:
                                    print(f'local: {local_result[index]}')
                                    print(f'Missing at {table.database.hostname}')
                                if fix_script:
                                    logger.debug('Adding line to remote script')
                                    values = ""
                                    for value in local_result[index].values():
                                        if type(value) == 'datetime':
                                            values += f"'{value.isoformat().split('.')[0].replace('T',' ')}',"
                                        if type(value) == 'str':
                                            values += f"{value},"
                                        else:
                                            values += f"{value},"
                                    values = values.strip(",")
                                    logger.debug(values)
                                    remote_script += f"\nINSERT INTO {self.schema.name}.{self.name} ({column_names}) VALUES ({values});"
                            if remote_row is not None:
                                if print_to_console:
                                    print(f'Missing at {self.database.hostname}')
                                    print(f'remote: {remote_result[index]}')
                                if fix_script:
                                    logger.debug('Adding line to local script')
                                    local_script += f"\nINSERT INTO {self.schema.name}.{self.name} ({remote_result[index].keys()}) VALUES ({remote_result[index].values()});"
                    else:
                        break
                logger.log(INFO,f'{processed + index} of {local_rowcount} rows processed')
                if processed % batch_size == 0 and processed != 0:
                    processed += batch_size
                else:
                    processed += batch_size - 1
            if fix_script:
                local_script += '\ncommit;'
                remote_script += '\ncommit;'
                if local_script:
                    print(f'Run on {self.database.hostname}:{local_script}')
                if remote_script:
                    print(f'Run on {table.database.hostname}:{remote_script}')
        return None

class Column:
    # Constructor
    def __init__(self, 
        table: Table, 
        name: str, 
        ordinal_position: int, 
        type: str, 
        length: int, 
        precision: int = 0, 
        default = None, 
        nullable: bool = True, 
        autoincrement: bool = False,
        unsigned = False,
        charset: str = 'utf8mb4'
    ):
        self.table = table
        self.name = name
        self.ordinal_position = ordinal_position
        self.type = type
        self.length = length
        self.precision = precision
        self.default = default
        self.nullable = nullable
        self.autoincrement = autoincrement
        self.unsigned = unsigned
        if charset is None:
            charset = self.table.schema.charset
        else:
            self.charset = charset

    # Python Object Overrides
    def __str__(self):
        stringed = f"{self.name} {self.type}({self.length}"
        if self.precision != 0:
            stringed+=f",{self.precision})"
        else:
            stringed+=")"
        if not self.nullable:
            stringed+=f" NOT NULL"
        if self.default is not None:
            stringed+=f"DEFAULT {self.default}"
        if self.autoincrement:
            stringed+=" AUTOINCREMENT"
        return stringed

    # Attributes and methods getters
    def get_attributes(self):
        return ['name', 'length', 'precision', 'nullable', 'autoincrement']

    def get_methods(self):
        return ['get_attributes', 'get_columns']
