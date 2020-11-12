# Py-OO-MySQL
Python Object-Orinted MySQL interface.

This allows to interact ith MySQL/MariaDB databases easily by constructing objects.

## Table of Contents

1. Requirements
2. Classes / Objects
3. Functions
4. Samples

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
* ```disconnect```

    Closes the current connection if any
* ```execute(command: string)```

    Executes a command in the database provided that there's a connection established.
* ```flush_privileges```

    Flushes privileges after  granting/revoking
* ```get_schema```

    Returns a dictionary with information about particular schema
* ```get_schemas```

    Returns a dictionary with information about all the schemas found in the database
* ```get_user_by_name```

    Returns user/host information for a given username, it might be more than one
* ```get_user_by_name_host```

    Returns exactly one user/host information for a given username
* ```get_version```

    Returns the MySQL version
* ```is_connected()```

    Returns True if there's a connection established or False otherwise.
* ```load_schemas```

    Loads schemas to the databas
* ```reconnect```

    Reconnects to the database in case of lost connection with same username and password

### Schema
The schema class represents a schema within a database, thus it requires a database object to be passed to it at creation time.

#### Attributes
* ```charset```

    Returns the character set for the schema
* ```collation```

    Returns collation for the schema
* ```database```

    Returns hostname:port information about the database where the schema resides
* ```name```

    Returns the name of the schema
* ```tables```

    Returns a list with the names of the tables in the schema

#### Methods
* ```compare```

    Allows the comparison of 2 schema objects
* ```get_table```

    Returns information about a particular table
* ```get_tables```

    Returns a list of the tables found in the schema
* ```load_tables```

    Loads the tables to the schema attribute

### Table
The Table class represents a particular table within a schema within a database.

#### Attributes
* ```database```

    Database whrre the table resides
* ```fqn```

    Fully qualified name of the table in the form {schema_name}.{table_name}
* ```name```

    Name of the table
* ```schema```

    Schema where the table resides

#### Methods
* ```compare_data```

    Allows to compare row-per-row the contents of the table with another one
* ```delete```

    Deletes rows from a table based on filters
* ```get_columns```

    Returns a list with the names of the columns in the  table
* ```get_insert_statement```

    Returns the normal SQL insert statement for the table
* ```get_rowcount```

    Returns the number of rows in the table
* ```insert```

    Inserts data in the table
* ```truncate```

    Truncates the table, removing all data
* ```update```

    Update rows in the Table based on some filters

### User

The User class represents a User within the database
#### Attributes
* ```exists```

    Boolean showing if the user exists or not
* ```grants```

    List of privileges that the user has
* ```host```

    Host authorized to connect from
* ```roles```

    List of roles granted to the user
* ```username```

    This is self explanatory

#### Methods
* ```create```

    Create the user if it doesn't exist in the database
* ```drop```

    Drops the user if it exists in the database
* ```get_grants```

    Under development
* ```update```

    Updates the user if it exists in the database

## 3. Functions

### Util: SQL

## 4. Samples

    # Import the package
    import pyoomysql

    # Create Database object
    mydb = pyoomysql.Datbase(
        hostname = "example.com",
        port = 3306
    )

    # Connect
    mydb.connect(
        username="someuser",
        password="super_secret_password"
    )

    # Create a schema object
    myschema = pyoomysql.Schema(
        database = mydb,
        name = "some_schema"
    )

    # Create Table object
    mytable = pyoomysql.Table(
        schema = myschema,
        name = "some_table"
    )

    # Create User object
    myuser = pyoomysql.User(
        name = "some_nice_username",
        host = "% or host to connect from",
        password = "very_long_and_strong_password"
    )

    # Create the user in the database
    myuser.create()
