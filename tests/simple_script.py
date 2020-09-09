#!/usr/bin/env python3

import json
import logging
import pyoomysql
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-h","--host", dest="db_host", required=True)
parser.add_argument("-P","--port", dest="db_port", required=True)
parser.add_argument("-u","--user", dest="db_user", required=True)
parser.add_argument("-p","--password", dest="db_pswd", required=True)

args = parser.parse_args()

mydb = pyoomysql.Database(hostname=args.db_host, port=args.db_port)
mydb.connect(user=args.db_user, password=args.db_pswd, log_level=logging.INFO)
for user in mydb.get_users():
    print(user)

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
