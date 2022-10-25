# General Imports
from mysql.connector import errorcode
from mysql.connector import FieldType
from argparse import ArgumentParser
from datetime import datetime
from datetime import timedelta
import json
import logging
from .Table import Table

# Format and Get root logger
logging.basicConfig(
    format='[%(asctime)s][%(levelname)-8s] %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

# Schema class
class Schema:
    def __init__(self, database, name):
        self.database = database
        if self.database.is_connected():
            schema = self.database.get_schema(name)
            if schema and len(schema) > 0:
                self.name = schema['name']
                self.charset = schema['charset']
                self.collation = schema['collation']
                self.tables = []
                self.exists = True
            else:
                self.name = name
                self.charset = "utf8mb4" # Best default option
                self.collation = "utf8mb4_general_ci"
                self.tables = []
                self.exists = False
        else:
            logger.error("No connection to the database found. Please connect first")
    # Python Object Overrides
    def __str__(self):
        if len(self.tables) == 0:
            self.load_tables()
        return json.dumps({
            "database": f"{self.database.hostname}:{self.database.port}",
            "name": self.name,
            "charset": self.charset,
            "collation": self.collation,
            "tables": [self.tables]
        },indent=2)

    # Attributes and methods getters
    def get_attributes(self):
        return ['database', 'name', 'charset', 'collation', 'tables']

    def get_methods(self):
        return ['compare', 'create', 'drop', 'get_attributes', 'get_methods', 'get_table', 'get_tables', 'load_tables']

    # Methods
    def create(self, charset="utf8mb4", collation="utf8mb4_general_ci"):
        if not self.exists:
            logger.debug("Schema not found")
            logger.info("Creating schema...")
            sql = f"CREATE SCHEMA {self.name} DEFAULT CHARACTER SET = '{self.charset}' COLLATE = '{self.collation}';"
            self.database.execute(command=sql)
            result = {
                "rows": [],
                "rowcount": 1,
                "status": "ERROR"
            }
            for schema in self.database.get_schemas().keys():
                if self.name == schema:
                    self.exists = True
                    result = {
                        "rows": [],
                        "rowcount": 1,
                        "status": "OK"
                    }
            return result
        else:
            logger.warning("Schema already exists in the database. Please choose a different name")

    def drop(self):
        if self.exists:
            logger.debug("Schema found")
            logger.info("Droping schema...")
            sql = f"DROP SCHEMA {self.name};"
            self.database.execute(command=sql)
            self.exists = False
            result = {
                "rows": [],
                "rowcount": 1,
                "status": "OK"
            }
            for schema in self.database.get_schemas().keys():
                if self.name == schema:
                    self.exists = True
                    result = {
                        "rows": [],
                        "rowcount": 1,
                        "status": "ERROR"
                    }
            return result
        else:
            logger.warning("Schema does not exist in the database. Nothing to do.")

    def load_tables(self):
        if self.exists:
            self.tables = self.get_tables()

    def get_tables(self):
        if self.exists:
            result = self.database.execute(f"SELECT table_schema AS schema_name, table_name, table_type, table_rows, avg_row_length, max_data_length FROM information_schema.tables WHERE table_schema = '{self.name}' ORDER by 1,2")
            tables = {}
            for row in result['rows']:
                tables[f"{row['table_name']}"] = {
                    'schema_name': row['schema_name'],
                    'table_name': row['table_name'],
                    'full_name': f"{row['schema_name']}.{row['table_name']}",
                    'table_type': row['table_type'],
                    'table_rows': row['table_rows'],
                    'avg_row_length': row['avg_row_length'],
                    'max_data_length': row['max_data_length']
                }
            # logger.debug(f"Tables is: {tables}")
            for table_name in tables.keys():
                table = Table(schema=self, name=table_name)
                # tables[table_name]['columns'] = table.get_columns()
            return tables

    def get_table(self, table_name):
        if self.exists:
            result = self.database.execute(f"SELECT table_schema AS schema_name, table_name, table_type, table_rows, avg_row_length, max_data_length FROM information_schema.tables WHERE table_schema = '{self.name}' AND table_name = '{table_name}' ORDER by 1,2")
            table = {}
            if len(result) > 0:
                table[f"{result['table_name']}"] = result[0]
            # logger.debug(f"Table is: {table}")
            table_obj = Table(schema=self, name=table_name)
            table['columns'] = table_obj.get_columns()
            return table

    def compare(self, schema, gen_fix_script=False):
        if self.exists:
            # Check there is a valid connection in both databases
            logger.debug(f'Checking connectivity to {self.database.hostname} and {schema.database.hostname}')
            if self.database.is_connected() and schema.database.is_connected():
            # Check that the schema exists in both databases
                logger.debug('Creating schema objects for both databases')
                local_schema = self
                remote_schema = schema
                if remote_schema.name != 'NotFound':
                    logger.debug(f'Remote Schema is: {remote_schema.name}')
                    # Get colunms definitions and compare
                    local_schema.load_tables()
                    # logger.debug(f'Local Schema Tables: {local_schema.tables}')
                    remote_schema.load_tables()
                    # logger.debug(f'Remote Schema Tables: {remote_schema.tables}')
                    diff_dict = {
                        'differences': [],
                        'fix_commands': []
                    }
                    for table_entry in local_schema.tables.keys():
                        if table_entry in remote_schema.tables.keys():
                            logger.debug(f"Checking table {table_entry}")
                            for column in local_schema.tables[table_entry]['columns'].keys():
                                if column in remote_schema.tables[table_entry]['columns'].keys():
                                    # logger.debug(f"Checking column {column}")
                                    for key in local_schema.tables[table_entry]['columns'][column].keys():
                                        if key != 'ordinal_position':
                                            if local_schema.tables[table_entry]['columns'][column][key] != remote_schema.tables[table_entry]['columns'][column][key]:
                                                fix_command = f"ALTER TABLE {table_entry} MODIFY COLUMN "
                                                fix_command += f"{column} {local_schema.tables[table_entry]['columns'][column]['column_type']}"
                                                if local_schema.tables[table_entry]['columns'][column]['is_nullable'] == 'NO':
                                                    fix_command += ' NOT NULL'
                                                if local_schema.tables[table_entry]['columns'][column]['column_default'] is not None:
                                                    if local_schema.tables[table_entry]['columns'][column]['data_type'] == 'varchar':
                                                        fix_command += f" DEFAULT '{local_schema.tables[table_entry]['columns'][column]['column_default']}'"
                                                    else:
                                                        fix_command += f" DEFAULT {local_schema.tables[table_entry]['columns'][column]['column_default']}"
                                                fix_command += ";"
                                                diff_dict['differences'].append({
                                                    table_entry: {
                                                        column:{
                                                            local_schema.database.hostname: {
                                                                key: local_schema.tables[table_entry]['columns'][column][key]
                                                            },
                                                            remote_schema.database.hostname: {
                                                                key: remote_schema.tables[table_entry]['columns'][column][key]
                                                            }
                                                        }
                                                    }
                                                })
                                                diff_dict['fix_commands'].append(fix_command)
                                else:
                                    fix_command = f"ALTER TABLE {table_entry} ADD COLUMN "
                                    fix_command += f"{column} {local_schema.tables[table_entry]['columns'][column]['column_type']}"
                                    if local_schema.tables[table_entry]['columns'][column]['is_nullable'] == 'NO':
                                        fix_command += ' NOT NULL'
                                    if local_schema.tables[table_entry]['columns'][column]['column_default'] is not None:
                                        if local_schema.tables[table_entry]['columns'][column]['data_type'] == 'varchar':
                                            fix_command += f" DEFAULT '{local_schema.tables[table_entry]['columns'][column]['column_default']}'"
                                        else:
                                            fix_command += f" DEFAULT {local_schema.tables[table_entry]['columns'][column]['column_default']}"
                                    fix_command += ";"
                                    diff_dict['differences'].append({
                                        table_entry: {
                                            column:{
                                                local_schema.database.hostname: {
                                                    'column_exists': True
                                                },
                                                remote_schema.database.hostname: {
                                                    'column_exists': False
                                                }
                                            }
                                        }
                                    })
                                    diff_dict['fix_commands'].append(fix_command)
                        else:
                            diff_dict['differences'].append({
                                self.name: {
                                    local_schema.database.hostname: {
                                        'schema_exists': True
                                    },
                                    remote_schema.database.hostname: {
                                        'schema_exists': False
                                    }
                                }
                            })
                    return diff_dict
            # TO-DO: Get functions definitions and compare
            return None
