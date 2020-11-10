from .Database import Database
from _io import TextIOWrapper

class Model:
    # Constructor
    def __init__(self):
        objects=[]
        pass

    # String
    def __str__(self):
        pass

    # Load from JSON file
    def load(self, source, path):
        pass

    # Dump to file
    def dump(self, path):
        pass

    # Deploy to Database
    def deploy(self, target: Database):
        pass

    # Parse SQL to Model
    def parse_sql(self, sql):
        if type(sql) is TextIOWrapper:
            all_lines = sql.readlines()
        elif type(sql) is str:
            all_lines = sql.split("\n")
        script=[]
        sql = ""
        for line in all_lines:
            sql += line
            if line.strip(" ")[(len(line)-1):] == ';':
                script.append(sql)
                sql = ""
        for sql in script:
            pass