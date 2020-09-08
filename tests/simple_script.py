import json
import logging
import pyoomysql
mydb = pyoomysql.Database(hostname="hekkamiahmsv1c.mylabserver.com", port=3306)
mydb.connect(username="mysqldba", password="mysqlpass", log_level=logging.DEBUG)

user_list = mydb.get_users()

mysql_schema = pyoomysql.Schema(database=mydb, name="mysql")
users_table = pyoomysql.Table(schema=mysql_schema, name="users")
