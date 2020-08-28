# Intra-package dependencies
from . import Table

# General Imports
import mysql.connector
from mysql.connector import errorcode
from mysql.connector import FieldType
from argparse import ArgumentParser
from datetime import datetime
from datetime import timedelta
import json
import getpass
import logging
from logging import DEBUG
from logging import CRITICAL
from logging import ERROR
from logging import FATAL
from logging import INFO
from logging import WARNING

# Format and Get root logger
logging.basicConfig(
    format='[%(asctime)s][%(levelname)-8s] %(message)s',
    level=INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

# Schema class
class Schema:
    def __init__(self, database, name):
        self.database = database
        schema = self.database.get_schema(name)
        if schema and len(schema) > 0:
            self.name = schema[0]['schema_name']
            self.charset = schema[0]['charset']
            self.collation = schema[0]['collation']
            self.tables = []
        else:
            self.name = 'NotFound'
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

    def load_tables(self):
        self.tables = self.get_tables()

    def get_tables(self):
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
        # logger.log(DEBUG, f"Tables is: {tables}")
        for table_name in tables.keys():
            table = Table(schema = self, name=table_name)
            tables[table_name]['columns'] = table.get_columns()
        return tables

    def get_table(self, table_name):
        result = self.database.execute(f"SELECT table_schema AS schema_name, table_name, table_type, table_rows, avg_row_length, max_data_length FROM information_schema.tables WHERE table_schema = '{self.name}' AND table_name = '{table_name}' ORDER by 1,2")
        table = {}
        if len(result) > 0:
            table[f"{result['table_name']}"] = result[0]
        # logger.log(DEBUG, f"Table is: {table}")
        table_obj = Table(self, table_name)
        table['columns'] = table_obj.get_columns()
        return table

    def compare(self, schema, gen_fix_script=False):
        # Check there is a valid connection in both databases
        logger.log(DEBUG, f'Checking connectivity to {self.database.hostname} and {schema.database.hostname}')
        if self.database.is_connected() and schema.database.is_connected():
        # Check that the schema exists in both databases
            logger.log(DEBUG, 'Creating schema objects for both databases')
            local_schema = self
            remote_schema = schema
            if remote_schema.name != 'NotFound':
                logger.log(DEBUG, f'Remote Schema is: {remote_schema.name}')
                # Get colunms definitions and compare
                local_schema.load_tables()
                # logger.log(DEBUG, f'Local Schema Tables: {local_schema.tables}')
                remote_schema.load_tables()
                # logger.log(DEBUG, f'Remote Schema Tables: {remote_schema.tables}')
                diff_dict = {
                    'differences': [],
                    'fix_commands': []
                }
                for table_entry in local_schema.tables.keys():
                    if table_entry in remote_schema.tables.keys():
                        logger.log(DEBUG, f"Checking table {table_entry}")
                        for column in local_schema.tables[table_entry]['columns'].keys():
                            if column in remote_schema.tables[table_entry]['columns'].keys():
                                # logger.log(DEBUG, f"Checking column {column}")
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
