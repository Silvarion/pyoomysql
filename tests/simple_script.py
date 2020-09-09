import json
import logging
import pyoomysql

mydb = pyoomysql.Database(hostname="hekkamiahmsv1c.mylabserver.com", port=3306)
mydb.connect(user="mysqldba", password="mysqlpass", log_level=logging.INFO)
user_list = mydb.get_users()


mytestschema = pyoomysql.Schema(database=mydb, name="my_test_schema")
mytestschema.create()
mytestschema.exists
mytesttable = pyoomysql.Table(schema=mytestschema, name="my_test_table")
# script = """-- Set schema
# use my_test_schema;
# -- 
# show tables;"""
# mydb.run(script=script)
mytestschema.drop()
mytestschema.exists
mysql_schema = pyoomysql.Schema(database=mydb, name="mysql")
users_table = pyoomysql.Table(schema=mysql_schema, name="users")

my_test_user = pyoomysql.User(database=mydb, user="my_test_user", password="my_test_password", host="anywhere_in_the_world")
my_test_user.create()
my_test_user.change_attr("host","my_new_host")
#my_test_user.update()
