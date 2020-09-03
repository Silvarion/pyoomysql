import pyoomysql

mydb = pyoomysql.Database(hostname="hekkamiahmsv1c.mylabserver.com", port=3306)
mydb.connect(username="mysqldba", password="mysqlpass")

mytestschema = pyoomysql.Schema(database=mydb, name="my_test_schema")
mytestschema.create()
mytestschema.exists
mytestschema.drop()
mytestschema.exists
mysql_schema = pyoomysql.Schema(database=mydb, name="mysql")
users_table = pyoomysql.Table(schema=mysql_schema, name="users")
