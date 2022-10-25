# Intra-package dependencies
from . import Database
from .util.sql import grant_to_dict

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
            "exists": self.exists,
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
        for attr in self.get_attributes():
            self.__setattr__(attr, loaded.__getattribute__(attr))

    # Grants management
    def get_grants(self):
        logger = logging.getLogger(name="User.get_grants")
        result = self.database.execute(f"SHOW GRANTS FOR '{self.user}'@'{self.host}'")
        if len(result["rows"]) > 0:
            for row in result["rows"]:
                logger.debug(f"Processing row: {row} with keys {row.keys()}")
                sql = row[list(row.keys())[0]]
                self.grants.append(grant_to_dict(sql))
        else:
            logger.warning("No grants found!")

    def set_grants(self, sql_list):
        self.grants = []
        for sql in sql_list:
            self.grants.append(grant_to_dict(sql))

    def update_grants(self, sql):
        added = grant_to_dict(sql.replace("`",""))
        index = 0
        found = False
        for granted in self.grants:
            if added['object'].lower() == granted['object'].lower():
                if added['privs'].lower() != granted['privs'].lower():
                    new_privs = set(added['privs'].lower().split(","))
                    old_privs = set(granted['privs'].lower().split(","))
                    privs = ",".join(new_privs.intersection(old_privs).union(new_privs))
                    self.grants[index]['privs'] = privs
                    logger.debug(f"Updating grant: {added}")
                found = True
                break
            index += 1
        if not found:
            logger.debug(f"Adding new grant: {added}")
            self.grants.append(added)

    def create(self):
        logger = logging.getLogger(name="User.create")
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
            self.exists = True
            # Roles
            for role in self.roles:
                sql = f"GRANT {role} TO {self.user}@'{self.host}'"
                response["rows"].append(self.database.execute(sql))
            # Privileges
            logger.info(f"Found {len(self.grants)} privileges to apply")
            for self_grant in self.grants:
                # logger.debug(f"Grant type is {type(self_grant)}")
                if type(self_grant) is str:
                    # logger.debug(f"Current User: {self.user} Current grant: {self.grants}")
                    sql = self_grant
                else:
                    sql = f"GRANT {self_grant['privs']} "
                    if self_grant["object"] != "":
                        sql+= f"ON {self_grant['object']} "
                    sql += f"TO {self.user}@'{self.host}'"
                logger.debug(f"Current SQL: {sql}")
                response["rows"].append(self.database.execute(sql))
            # Flush Privileges
            self.database.flush_privileges()
            self.reload()

    def drop(self):
        logger = logging.getLogger(name="User.drop")
        if self.exists:
            # Drop user
            sql = f"DROP USER {self.user}@'{self.host}'"
            logger.debug(f"SQL: {sql}")
            result = self.database.execute(sql)
            self.exists = False
            return result

    def change_attr(self, attribute: str, new_value):
        logger = logging.getLogger(name="User.change_attr")
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
        logger = logging.getLogger(name="User.update")
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
            if self.password is None:
                logger.info("No password change detected. Skipping this step)")
            else:
                if self.password[0] == '*' and len(self.password) == 41:
                    sql = f"SET PASSWORD FOR '{self.user}'@'{self.host}' = '{self.password}'"
                else:
                    sql = f"SET PASSWORD FOR '{self.user}'@'{self.host}' = PASSWORD('{self.password}')"
                logger.debug(f"SQL is: {sql}")
                response["rows"].append(self.database.execute(sql))
            db_user = self.database.get_user_by_name_host(user=self.user, host = self.host)
            # Update attributes
            for attr in self.get_attributes():
                if attr not in ['password', 'auth_string', 'grants']:
                    if getattr(self, attr) != getattr(db_user, attr):
                        self.change_attr(attribute=attr, new_value=getattr(self, attr))
            # Privileges
            # Newly revoked
            for remote in db_user.grants:
                if type(remote) is str:
                    remote = grant_to_dict(remote)
                    logger.debug(f"remote grant dictionary {remote}")
                logger.info(f"Looking for local grants on {remote['object']}")
                found = False
                for local in self.grants:
                    if type(local) is str:
                        local = grant_to_dict(local)
                    if local['object'] == remote['object']:
                        logger.info(f"Found, will take care of it later")
                        found = True
                        break
                if not found:
                    logger.info(f"Not found, revoking privileges on {remote['object']}")
                    sql = f"REVOKE {remote['privs']} ON {remote['object']} FROM {self.user}@'{self.host}'"
                    response["rows"].append(self.database.execute(sql))
            db_user.reload() # Reload remote user grants before comparing again
            # Newly granted
            for local in self.grants:
                if type(local) is str:
                    local = grant_to_dict(local)
                logger.info(f"Looking for remote grants on {local['object']}")
                found = False
                for remote in db_user.grants:
                    if type(remote) is str:
                        remote = grant_to_dict(remote)
                    if local['object'] == remote['object']:
                        logger.info(f"Found, will take care of it later")
                        found = True
                        break
                if not found:
                    logger.info(f"Not found, adding new privilege")
                    sql = f"GRANT {local['privs']} ON {local['object']} TO {self.user}@'{self.host}'"
                    self.database.execute(sql)
            db_user.reload() # Reload remote user grants before comparing again
            # Objects with changed privileges
            for loaded_grant in db_user.grants:
                sql = ""
                # logger.debug(f"Grant type is {type(loaded_grant)}")
                if type(loaded_grant) is str:
                    logger.debug(f"Transforming GRANT string to dictionary:\n'{loaded_grant}'")
                    loaded_grant = grant_to_dict(loaded_grant)
                    logger.debug(f"{loaded_grant}")
                # logger.debug(f"Loaded User: {db_user.user} Current grant: {loaded_grant}")
                for self_grant in self.grants:
                    # logger.debug(f"Grant type is {type(self_grant)}")
                    if type(self_grant) is str:
                        logger.debug(f"Transforming GRANT string to dictionary:\n'{self_grant}'")
                        self_grant = grant_to_dict(self_grant)
                        logger.debug(f"{self_grant}")
                    # logger.debug(f"Current User: {self.user} Current grant: {self.grants}")
                    if self_grant['object'] == loaded_grant['object']:
                        # Revokes
                        new_privs = set(self_grant["privs"].strip().lower().split(","))
                        old_privs = set(loaded_grant["privs"].strip().lower().split(","))
                        revoked_list = list(old_privs.difference(new_privs))
                        if len(revoked_list) > 0:
                            revoked = ",".join(revoked_list)
                            sql = f"REVOKE {revoked} "
                            if self_grant["object"] != "":
                                sql+= f"ON {self_grant['object']} "
                            sql += f"FROM {self.user}@'{self.host}'"
                            logger.debug(f"Current SQL: {sql}")
                            response["rows"].append(self.database.execute(sql))
                        # Grants
                        new_privs = set(self_grant["privs"].lower().split(","))
                        old_privs = set(loaded_grant["privs"].lower().split(","))
                        granted_list = list(new_privs.difference(old_privs))
                        if len(granted_list) > 0:
                            granted = ",".join(granted_list)
                            sql = f"GRANT {granted} "
                            if self_grant["object"] != "":
                                sql+= f"ON {self_grant['object']} "
                            sql += f"TO {self.user}@'{self.host}'"
                            logger.debug(f"Current SQL: {sql}")
                            response["rows"].append(self.database.execute(sql))
            self.database.flush_privileges()
            self.reload()
        else:
            logger.info("User doesn't exists, CREATING instead")
            self.create()
