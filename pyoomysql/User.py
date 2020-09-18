# Intra-package dependencies
from . import Database

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

# Class User
class User:
    def __init__(self, database: Database, user, host = '%', password = None):
        result = database.execute(command=f"SELECT * FROM mysql.user WHERE user = '{user}' and host = '{host}';")
        self.database = database
        self.roles = []
        self.grants = []
        if len(result["rows"]) == 0:
            self.user = user
            self.host = host
            self.password = password
            self.exists = False
        elif len(result["rows"]) == 1:
            if type(result["rows"][0]["User"]) is bytearray:
                self.user = result["rows"][0]["User"].decode()
            else:
                self.user = result["rows"][0]["User"]
            if type(result["rows"][0]["Host"]) is bytearray:
                self.host = result["rows"][0]["Host"].decode()
            else:
                self.host = result["rows"][0]["Host"]
            if password is None and "Password" in result["rows"][0].keys():
                if type(result["rows"][0]["Password"]) is bytearray:
                    self.password = result["rows"][0]["Password"].decode()
                else:
                    self.password = result["rows"][0]["Password"]
            else:
                self.password = password
            if password is None and "authentication_string" in result["rows"][0].keys():
                if type(result["rows"][0]["authentication_string"]) is bytearray:
                    self.auth_string = result["rows"][0]["authentication_string"].decode()
                else:
                    self.auth_string = result["rows"][0]["authentication_string"]
            else:
                self.password = password
            self.ssl_type = result["rows"][0]["ssl_type"]
            if type(result["rows"][0]["ssl_cipher"]) is bytearray:
                self.ssl_cipher = result["rows"][0]["ssl_cipher"].decode()
            else:
                self.ssl_cipher = result["rows"][0]["ssl_cipher"]
            if type(result["rows"][0]["x509_issuer"]) is bytearray:
                self.x509_issuer = result["rows"][0]["x509_issuer"].decode()
            else:
                self.x509_issuer = result["rows"][0]["x509_issuer"]
            if type(result["rows"][0]["x509_subject"]) is bytearray:
                self.x509_subject = result["rows"][0]["x509_subject"].decode()
            else:
                self.x509_subject = result["rows"][0]["x509_subject"]
            if type(result["rows"][0]["plugin"]) is bytearray:
                self.plugin = result["rows"][0]["plugin"].decode()
            else:
                self.plugin = result["rows"][0]["plugin"]
            self.select_priv = result["rows"][0]["Select_priv"]
            self.insert_priv = result["rows"][0]["Insert_priv"]
            self.update_priv = result["rows"][0]["Update_priv"]
            self.delete_priv = result["rows"][0]["Delete_priv"]
            self.create_priv = result["rows"][0]["Create_priv"]
            self.drop_priv = result["rows"][0]["Drop_priv"]
            self.reload_priv = result["rows"][0]["Reload_priv"]
            self.shutdown_priv = result["rows"][0]["Shutdown_priv"]
            self.process_priv = result["rows"][0]["Process_priv"]
            self.file_priv = result["rows"][0]["File_priv"]
            self.grant_priv = result["rows"][0]["Grant_priv"]
            self.references_priv = result["rows"][0]["References_priv"]
            self.index_priv = result["rows"][0]["Index_priv"]
            self.alter_priv = result["rows"][0]["Alter_priv"]
            self.show_db_priv = result["rows"][0]["Show_db_priv"]
            self.super_priv = result["rows"][0]["Super_priv"]
            self.create_tmp_table_priv = result["rows"][0]["Create_tmp_table_priv"]
            self.lock_tables_priv = result["rows"][0]["Lock_tables_priv"]
            self.execute_priv = result["rows"][0]["Execute_priv"]
            self.repl_slave_priv = result["rows"][0]["Repl_slave_priv"]
            self.repl_client_priv = result["rows"][0]["Repl_client_priv"]
            self.create_view_priv = result["rows"][0]["Create_view_priv"]
            self.show_view_priv = result["rows"][0]["Show_view_priv"]
            self.create_routine_priv = result["rows"][0]["Create_routine_priv"]
            self.alter_routine_priv = result["rows"][0]["Alter_routine_priv"]
            self.create_user_priv = result["rows"][0]["Create_user_priv"]
            self.event_priv = result["rows"][0]["Event_priv"]
            self.trigger_priv = result["rows"][0]["Trigger_priv"]
            self.create_tablespace_priv = result["rows"][0]["Create_tablespace_priv"]
            self.max_questions = result["rows"][0]["max_questions"]
            self.max_updates = result["rows"][0]["max_updates"]
            self.max_connections = result["rows"][0]["max_connections"]
            self.max_user_connections = result["rows"][0]["max_user_connections"]
            # Get roles
            self.roles = []
            # Get grants
            self.grants = []
            self.get_grants()
            self.exists = True
        else:
            logger.warning(f'{len(result["rows"])} results found. Please modify your search to get only 1 user.')

    def __str__(self):
        return json.dumps({
            "user": self.user,
            "host": self.host,
            "roles": self.roles,
            "grants": self.grants
        },indent=2)

    # Attributes and methods getters
    def get_attributes(self):
        return ['database', 'roles', 'grants', 'user', 'host', 'password', 'ssl_type', 'ssl_cipher', 'x509_issuer', 'x509_subject', 'plugin', 'select_priv', 'insert_priv', 'update_priv', 'delete_priv', 'create_priv', 'drop_priv', 'reload_priv', 'shutdown_priv', 'process_priv', 'file_priv', 'grant_priv', 'references_priv', 'index_priv', 'alter_priv', 'show_db_priv', 'super_priv', 'create_tmp_table_priv', 'lock_tables_priv', 'execute_priv', 'repl_slave_priv', 'repl_client_priv', 'create_view_priv', 'show_view_priv', 'create_routine_priv', 'alter_routine_priv', 'create_user_priv', 'event_priv', 'trigger_priv', 'create_tablespace_priv', 'max_questions', 'max_updates', 'max_connections', 'max_user_connections', 'exists']

    def get_methods(self):
        return ['get_attributes', 'get_methods', 'reload', 'get_grants', 'create', 'drop', 'update']

    def reload(self):
        loaded = User(database=self.database, user=self.user, host=self.host)
        self = loaded

    def get_grants(self):
        result = self.database.execute(f"SHOW GRANTS FOR '{self.user}'@'{self.host}'")
        if len(result["rows"]) > 0:
            for row in result["rows"]:
                self.grants.append(row[f"Grants for {self.user}@{self.host}"])
        else:
            logger.warning("No grants found!")

    def create(self):
        response = {
            "rows": []
        }
        if self.exists:
            logger.info("User already exists, UPDATING instead")
            self.update()
        else:
            # Create user
            sql = f"CREATE USER '{self.user}'@'{self.host}' IDENTIFIED BY '{self.password}'"
            logger.debug(f"SQL is: {sql}")
            response["rows"].append(self.database.execute(sql))
            self.reload()
            # Roles
            # for role in self.roles:
            #     sql = f"GRANT {role} TO {self.user}@'{self.host}'"
            #     response["rows"].append(self.database.execute(sql))
            # Grants
            # for grant in self.grants:
            #     response["rows"].append(self.database.execute(grant))
            # Flush Privileges
            self.database.flush_privileges()


    def drop(self):
        if self.exists:
            # Drop user
            sql = f"DROP USER {self.user}@'{self.host}'"
            result = self.database.execute(sql)
            self.exists = False
            return result

    def change_attr(self, attribute: str, new_value):
        try:
            logger.debug("Saving old value.")
            old_value = getattr(self, attribute)
            logger.debug(f"Old value: {old_value} saved. Setting new value.")
            setattr(self, attribute, new_value)
            logger.debug(f"New value set on {attribute}")
            logger.debug("Building update statement.")
            sql = f"UPDATE mysql.user SET {attribute} = "
            if type(new_value) is str:
                sql += f"'{new_value}' WHERE user = '{self.user}' AND host = '{old_value}'; COMMIT;"
            else:
                sql += f"{new_value} WHERE user = '{self.user}' AND host = '{old_value}'; COMMIT;"
            response = self.database.execute(command=sql)
            return response
        except Exception as err:
            logger.error(f"Catched exception:\n{err}")
            logger.warning(f"Attribute {attribute} not found!")

    def update(self):
        response = {
            "rows": []
        }
        if self.exists:
            # Check if the host is udpated and exists in the database
            user_list = self.database.get_user_by_name(user=self.user)
            if len(user_list) == 1:
                loaded_user = user_list[0]
            elif len(user_list) > 1:
                loaded_user = None
                for loaded_user in user_list:
                    if loaded_user.host == self.host:
                        break
            else:
                loaded_user = None
            # Update host if it's different
            if loaded_user is not None and self.host != loaded_user.host:
                loaded_user.change_attr("host",self.host)
            # Update password
            if self.password[0] == '*' and len(self.password) == 41:
                sql = f"SET PASSWORD FOR '{self.user}'@'{self.host}' = '{self.password}'"
            else:
                sql = f"SET PASSWORD FOR '{self.user}'@'{self.host}' = PASSWORD('{self.password}')"
            logger.debug(f"SQL is: {sql}")
            response["rows"].append(self.database.execute(sql))
            db_user = self.database.get_user_by_name_host(user=self.user, host = self.host)
            # Update attributes
            for attr in self.get_attributes():
                if attr not in ['password', 'auth_string']:
                    if getattr(self, attr) != getattr(db_user, attr):
                        self.change_attr(attribute=attr, new_value=getattr(self, attr))
            self.reload()
            # Roles
            # for role in self.roles:
            #     sql = f"GRANT {role} TO {self.user}@'{self.host}'"
            #     response["rows"].append(self.database.execute(sql))
            # Grants
            # for grant in self.grants:
            #     response["rows"].append(self.database.execute(grant))
            # Flush Privileges
            self.database.flush_privileges()
        else:
            logger.info("User doesn't exists, CREATING instead")
            self.create()
