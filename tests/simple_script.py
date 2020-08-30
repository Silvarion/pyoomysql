import pyoomysql

mydb = pyoomysql.Database(hostname="hekkamiahmsv1c.mylabserver.com", port=3306)
mydb.connect(username="mysqldba", password="mysqlpass")

mysql_schema = pyoomysql.Schema(database=mydb, name="mysql")
users_table = pyoomysql.Table(schema=mysql_schema, name="users")
