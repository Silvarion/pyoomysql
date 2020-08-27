# mysqllib
A simple object-oriented library to interact with MySQL databases.

## Table of Contents

1. Requirements
2. Classes / Objects
3. Samples

## 1. Requirements
The MySQLLib package is developed fully in Python3 and is not compatible with Python 2.X at all. Please use Python 3.6 or later to ensure the best compatibility.
This package uses mysql-connector-python as backend. Make sure you have it installed by issuing the following command:

```pip install mysql-connector-python```

## 2. Classes
### Database
The database class represents a simple pair ```hostname:port``` and holds connection status and other useful information about the Database itself and the connection, when the last is established.
#### Attributes
* ```auth_plugin```
* ```connection```
* ```hostname```
* ```password```
* ```port```
* ```schema```
* ```username```
#### Methods
* ```__init__(self, hostname, port=3306, database='information_schema', log_level=logging.INFO)```
* ```connect(username: string, password: string, [schema]: string, [auth_plugin]: string)```
* ```execute(command: string)```
* ```is_connected()```
### Schema
#### Attributes
#### Methods
### Table
#### Attributes
#### Methods
### User
#### Attributes
#### Methods

## 3. Samples