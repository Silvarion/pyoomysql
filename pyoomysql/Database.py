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

# Database class
class Database:
    # Attributes
    connection = None

    # Creator
    def __init__(self, hostname, port=3306, schema='information_schema'):
        self.hostname = hostname
        self.port = port
        self.schema = schema
        self.auth_plugin = None
        logger.setLevel(logging.INFO)

    # Python Object overrides
    def __str__(self):
        return json.dumps({
            "hostname": self.hostname,
            "port": self.port,

        },indent=2)

    # Attributes and methods getters
    def get_attributes(self):
        return ['auth_plugin', 'connection', 'hostname', 'password', 'port', 'schema', 'user']

    def get_methods(self):
        return ['connect', 'disconnect', 'execute', 'flush_privileges', 'get_attributes', 'get_methods', 'get_schema', 'get_schemas', 'get_user_by_name', 'get_user_by_name_host', 'get_version', 'is_connected', 'load_schemas', 'reconnect']


    # Methods
    def connect(self, user, password, schema='',auth_plugin=None,nolog=False, log_level=logging.INFO):
        logger.setLevel(log_level)
        self.log_level = log_level
        cnx = None
        try:
            if auth_plugin:
                logger.debug("Using auth_plugin for authentication")
                cnx = mysql.connector.connect(user=user, password=password, host=self.hostname, port=self.port, database=schema,auth_plugin=auth_plugin)
            else:
                logger.debug("Using defaults for authentication")
                cnx = mysql.connector.connect(user=user, password=password, host=self.hostname, port=self.port, database=schema)
            if not nolog:
                logger.info(f'Database {self.schema} on {self.hostname} connected')
            self.user = user
            self.password = password
            self.auth_plugin = auth_plugin
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.log(CRITICAL, "Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.log(CRITICAL, "Database does not exist")
            else:
                logger.log(CRITICAL, err)
        finally:
            if cnx:
                self.connection = cnx

    def get_version(self):
        if self.is_connected():
            response = self.execute(command="SELECT version()")
            self.version = response["rows"][0]["version()"]
            return self.version
        else:
            logger.error("Connect first!")

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
                logger.log(INFO,f'Database {self.schema} on {self.hostname} disconnected')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.log(CRITICAL, "Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.log(CRITICAL, "Database does not exist")
            else:
                logger.log(CRITICAL, err)
        finally:
            self.connection = None

    def reconnect(self):
        try:
            self.connection.ping(reconnect=True, attempts=3, delay=5)
        except:
            if "user" in dir(self) and "password" in dir(self):
                if self.auth_plugin:
                    self.connect(user=self.user, password=self.password, auth_plugin=self.auth_plugin, nolog=True, log_level = self.log_level)
                else:
                    self.connect(user=self.user, password=self.password, nolog=True, log_level = self.log_level)

    # Execute single command
    def execute(self,command):
        if not self.is_connected():
            self.connection.ping(reconnect=True, attempts=3, delay=5)
        resultset = {
            'rows': []
        }
        if self.is_connected():
            logger.debug('Database is connected. Trying to create cursor')
            try:
                cursor = self.connection.cursor(buffered=True,dictionary=True)
                logger.debug('Cursor created')
                logger.debug(f'command: {command}')
                sql = f"{command.strip(';')};"
                logger.debug(f'sql: "{sql}"')
                timer_start = datetime.now()
                cursor.execute(sql)
                timer_end = datetime.now()
                timer_elapsed = timer_end - timer_start
                logger.debug('Command executed')
                if command.upper().find("SELECT") == 0:
                    rows = cursor.fetchall()
                    logger.debug(f'Fetched {cursor.rowcount} rows')
                    columns = cursor.column_names
                    for row in rows:
                        row_dic = {}
                        for column in columns:
                            row_dic[column] = row[column]
                        resultset['rows'].append(row_dic)
                    resultset["action"] = "SELECT"
                    resultset["rowcount"] = cursor.rowcount
                    resultset["start_time"] = f"{timer_start.strftime('%Y-%m-%d %H:%M:%S')}"
                    resultset["exec_time"] = f"{timer_elapsed.total_seconds()}"
                elif command.upper().find("SHOW") == 0:
                    rows = cursor.fetchall()
                    logger.debug(f'Fetched {cursor.rowcount} rows')
                    columns = cursor.column_names
                    for row in rows:
                        row_dic = {}
                        for column in columns:
                            row_dic[column] = row[column]
                        resultset['rows'].append(row_dic)
                    resultset["action"] = "SELECT"
                    resultset["rowcount"] = cursor.rowcount
                    resultset["start_time"] = f"{timer_start.strftime('%Y-%m-%d %H:%M:%S')}"
                    resultset["exec_time"] = f"{timer_elapsed.total_seconds()}"
                else:
                    logger.debug(f"RESULTSET:\n{cursor}")
                    resultset["action"] = command.upper().split(" ")[0]
                    resultset["rowcount"] = cursor.rowcount
                    resultset["start_time"] = f"{timer_end.strftime('%Y-%m-%d %H:%M:%S')}"
                    resultset["exec_time"] = f"{timer_elapsed.total_seconds()}"
                logger.debug(f"Command executed successfully in {resultset['exec_time']} s")
            except mysql.connector.Error as err:
                logger.log(WARNING, f'Catched mysql.connector.Error while executing {command}')
                logger.log(CRITICAL, err.errno)
                logger.log(CRITICAL, err.sqlstate)
                logger.log(CRITICAL, err.msg)
            except Exception as e:
                logger.log(WARNING, f'Catched exception while executing: {command}')
                logger.log(CRITICAL, e)
            finally:
                return resultset
        else:
            logger.log(ERROR,'Please connect first, then try again')

    ## Run Script
    def run(self, script):
        sql = ""
        results = {}
        counter = 1
        if type(script) is str:
            script = script.split(sep="\n")
        for line in script:
            logger.debug(f"Current line: {line}")
            if line[:2] != "--":
                sql += line.replace("\n"," ")
                logger.debug(f"Current sql string: {sql}")
                if line.find(";") == len(line)-1:
                    logger.debug(f"SQL to execute:\n{sql}")
                    response = self.execute(command=sql)
                    results[f"command_{counter}"] = {
                        "command": sql,
                        "response": response
                    }
                    sql = ""
        return results

    ## Schema methods
    def load_schemas(self):
        self.schemas = self.execute('SELECT schema_name, default_character_set_name AS charset, default_collation_name as collation FROM information_schema.schemata')['rows']

    def get_schemas(self):
        response = self.execute("SELECT schema_name, default_character_set_name AS charset, default_collation_name as collation FROM information_schema.schemata")
        results = {}
        for row in response["rows"]:
            results[row["schema_name"]]= {
                "name": row["schema_name"],
                "charset": row["charset"],
                "collation": row["collation"],
                "tables": []
            }
        return results

    def get_schema(self, schema_name):
        result = self.execute(f"SELECT schema_name as 'name', default_character_set_name AS charset, default_collation_name as collation FROM information_schema.schemata WHERE schema_name = '{schema_name}'")['rows']
        if len(result) > 0:
            logger.debug(f'Schema {schema_name} found. Returning {result[0]}')
            return result[0]
        else:
            logger.debug(f'Schema {schema_name} not found. Returning None')
            return None

    ## User Methods
    def get_users(self):
        from pyoomysql.User import User

        response = []
        result = self.execute(f"SELECT user, host FROM mysql.user")
        if len(result["rows"]) > 0:
            for row in result["rows"]:
                if type(row["user"]) is bytearray:
                    response.append(User(database=self, user=row["user"].decode(), host=row["host"].decode()))
                else:
                    response.append(User(database=self, user=row["user"], host=row["host"]))
        return response

    def get_user_by_name(self, user):
        from pyoomysql.User import User
        response = []
        result = self.execute(f"SELECT user, host FROM mysql.user WHERE user = '{user}'")
        if len(result["rows"]) > 0:
            for row in result["rows"]:
                if type(row["user"]) is bytearray:
                    response.append(User(database=self, user=row["user"].decode(), host=row["host"].decode()))
                else:
                    response.append(User(database=self, user=row["user"], host=row["host"]))
        return response

    def get_user_by_name_host(self, user, host):
        from pyoomysql.User import User
        result = self.execute(f"SELECT user, host FROM mysql.user WHERE user = '{user}' AND host = '{host}'")
        if result["rowcount"] == 1:
            if type(result["rows"][0]["user"]) is bytearray:
                return User(database=self, user=result["rows"][0]["user"].decode(), host=result["rows"][0]["host"].decode())
            else:
                return User(database=self, user=result["rows"][0]["user"], host=result["rows"][0]["host"])

    # Flush Privileges
    def flush_privileges(self):
        return self.execute(command = "FLUSH PRIVILEGES;")

    # Replication
    def is_replica(self):
        result = self.get_replica_status()
        if result is not None:
            return True
        else:
            return False

    def get_replica_status(self):
        result = self.execute(command="SHOW SLAVE STATUS")
        if len(result["rows"]) == 1:
            return result["rows"][0]
        else:
            return None
    
    def configure_replica(self, primary_host, primary_port, replica_user, replica_pswd, binlog_file=None, binlog_pos=None):
        sql = f"CHANGE MASTER TO MASTER_HOST='{primary_host}', MASTER_PORT={primary_port}, MASTER_USER='{replica_user}', MASTER_PASSWORD='{replica_pswd}' "
        if binlog_file is not None:
            sql+=f"MASTER_LOG_FILE='{binlog_file}' "
        if binlog_pos is not None:
            sql+=f"MASTER_LOG_POS={binlog_pos}"
        return self.execute(sql)

    def start_replica(self):
        return self.execute(command="START SLAVE")

    def stop_replica(self):
        return self.execute(command="STOP SLAVE")
    
    def skip_replica_error(self, amount=1):
        logger.info("Stopping replication process...")
        self.stop_replica()
        logger.info(f"Setting SQL_SLAVE_SKIP_COUNTER to {amount}")
        self.execute(command=f"SET GLOBAL SQL_SLAVE_SKIP_COUNTER = {amount}")
        logger.info("Starting replication process again...")
        self.start_replica()
        logger.info("All done.")

    # Check if there's an active connection to the database
    def is_connected(self):
        if self.connection.is_connected():
            return True
        else:
            return False

    # Global Variables
    def get_global_variable(self, variable_name):
        return self.execute(command=f"SELECT variable_name, variable_value FROM information_schema.global_variables WHERE variable_name = '{variable_name}'")["rows"][0]

    def get_global_variables_like(self, variable_name):
        return self.execute(command=f"SELECT variable_name, variable_value FROM information_schema.global_variables WHERE variable_name LIKE '%{variable_name}%'")["rows"]
    
    def set_global_variable(self, variable_name, variable_value):
        return self.execute(command=f"SET GLOBAL {variable_name} = {variable_value}")

    def set_global_variable_from_dict(self, variable_dict: dict):
        return self.execute(command=f"SET GLOBAL {variable_dict['variable_name']} = {variable_dict['variable_value']}")
