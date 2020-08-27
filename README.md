# Py-OO-MySQL
Python Object-Orinted MySQL interface

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

    Authentication plugin support.
* ```connection```
    
    Existing connection object (from mysql.connector.connection) or None if there is no connection.
* ```hostname```

    Hostname/IP address used to connect to.
* ```password```

    Password used for connecting to the database.
* ```port```

    Port used to connect to the database. Defaults to 3306
* ```schema```

    As in the default schema used for connecting to the database
* ```username```

    Username set for the connection.

#### Methods
* ```__init__(hostname, port=3306, schema, log_level)```

    Constructor that will set hsotname, port and default schema along with the standard log_level INFO (as in the standard library logging.INFO)
* ```connect(username: string, password: string, [schema]: string, [auth_plugin]: string)```
    
    Connects to the host:port using the provided username and password. The schema defaults to "information_schema"
* ```execute(command: string)```

    Executes a command in the database provided that there's a connection established.
* ```is_connected()```
    Returns True if there's a connection established or False otherwise.
### Schema
The schema class represents a schema within a database, thus it requires a database object to be passed to it at creation time.
#### Attributes
* ```character_set```
* ```collation```
* ```database```
* ```name```
* ```tables```
#### Methods
### Table
#### Attributes
#### Methods
### User
#### Attributes
#### Methods

## 3. Samples
